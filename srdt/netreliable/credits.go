package netreliable

import (
	"time"

	"example.com/srdt/reliable"
	"example.com/srdt/secureudp"
)

type openRequest struct {
	id    uint64
	total uint32
}

func (s *Server) initDelivery(p *peerSession) {
	if p.receiver != nil {
		return
	}
	p.receiver, _ = reliable.NewReceiverWithLimits(reliable.ReceiverLimits{
		MaxActiveMessages: s.config.MaxMessages, MaxActiveBytes: s.config.SessionBufferBytes, MaxCompletedMessages: 128})
	_ = p.receiver.SetBudget(s.budget)
	batchAcks := !s.config.Options.LegacyPacketAcks
	if p.crypto.ProtocolVersion() == secureudp.ProtocolV1 {
		batchAcks = p.crypto.HasCapability(secureudp.CapabilityPacketAckBatch)
	}
	if batchAcks {
		_ = p.receiver.EnablePacketAckBatching()
	}
	windowBytes := s.config.WindowBytes
	if p.bridge != nil {
		// A bridge already has a strict combined SessionBufferBytes cap. Reusing
		// that existing budget as the maximum flight window avoids limiting a
		// long TCP stream to roughly one chunk/RTT without reserving extra memory.
		windowBytes = s.config.SessionBufferBytes
	}
	p.queue, _ = reliable.NewQueue(windowBytes, s.config.SessionBufferBytes)
	_ = p.queue.SetBudget(s.budget)
	s.config.Options.configureQueue(p.queue)
	if p.flowMode == 3 {
		_ = p.receiver.RequireCredits()
		_ = p.queue.UseWindowCredits(2, 2, int(p.windowCapacity))
	} else if p.flowMode == 1 {
		_ = p.receiver.RequireCredits()
		_ = p.queue.RequireCredits()
	}
}

func (s *Server) rejectOpen(p *peerSession, f reliable.Frame, reason uint32, send sendDatagram) {
	s.stats.CreditRejected++
	s.sendFrame(p, reliable.Frame{Type: reliable.TypeReject, ID: f.ID, Total: f.Total, Offset: reason}, send)
}

func (s *Server) blocked(p *peerSession, f reliable.Frame, now time.Time, send sendDatagram) {
	// Bound responses to repeated authenticated probes per peer, not per message.
	if !p.lastBlocked.IsZero() && now.Sub(p.lastBlocked) < 100*time.Millisecond {
		return
	}
	p.lastBlocked = now
	s.stats.CreditBlocked++
	s.sendFrame(p, reliable.Frame{Type: reliable.TypeBlocked, ID: f.ID, Total: f.Total, Offset: 250}, send)
}

func (s *Server) handleOpen(p *peerSession, f reliable.Frame, now time.Time, send sendDatagram) {
	s.stats.CreditRequests++
	if (p.flowMode != 0 && p.flowMode != 1) || f.ID%2 == 0 || f.ID == ^uint64(0) || int(f.Total) > s.config.MaxMessageBytes {
		s.rejectOpen(p, f, 1, send)
		return
	}
	p.flowMode = 1
	s.initDelivery(p)
	if total, known := p.receiver.Total(f.ID); known {
		if total != f.Total {
			s.rejectOpen(p, f, 3, send)
			return
		}
		// Lost GRANT: same reservation, fresh encryption counter. Completed IDs can
		// also safely reauthorize duplicates; Receiver will never deliver them twice.
		p.confirmed, p.lastValid = true, now
		s.sendFrame(p, reliable.Frame{Type: reliable.TypeGrant, ID: f.ID, Total: total}, send)
		if s.sessions[p.id] != nil {
			s.schedule(p, now, true)
		}
		return
	}
	if f.ID < p.nextRequest || (f.ID-p.nextRequest)/2 >= 128 {
		s.rejectOpen(p, f, 2, send)
		return
	}
	for _, request := range p.pendingOpens {
		if request.id == f.ID {
			if request.total != f.Total {
				s.rejectOpen(p, f, 3, send)
				return
			}
			// A duplicate probe doesn't extend the no-progress deadline or rescan all peers.
			s.blocked(p, f, now, send)
			return
		}
	}
	if s.config.CompletionMessages > 0 && s.stats.DeliveredMessages+uint64(p.receiver.ActiveMessages()+len(p.pendingOpens)) >= s.config.CompletionMessages {
		s.rejectOpen(p, f, 1, send)
		return
	}
	if len(p.pendingOpens) >= s.config.MaxMessages {
		s.blocked(p, f, now, send)
		return
	}
	wasIdle := len(p.pendingOpens) == 0 && p.receiver.ActiveMessages() == 0 && p.queue.Pending() == 0
	p.confirmed, p.lastValid = true, now
	if wasIdle {
		p.lastProgress = now
	}
	// Preserve fairness: a newly arriving request must not jump ahead of waiters.
	if s.waitHead == nil && s.grant(p, openRequest{f.ID, f.Total}, now, send) {
		if s.sessions[p.id] != nil {
			s.schedule(p, now, true)
		}
		return
	}
	p.pendingOpens = append(p.pendingOpens, openRequest{f.ID, f.Total})
	s.linkWaiter(p)
	s.creditScan = s.waitCount
	s.blocked(p, f, now, send)
	if s.sessions[p.id] != nil {
		s.schedule(p, now, true)
	}
}

// grant reserves all receive storage before emitting permission. Transferring
// the completed payload to an echo response retains this SAME budget charge.
func (s *Server) grant(p *peerSession, request openRequest, now time.Time, send sendDatagram) bool {
	heldMessages, heldBytes := 0, 0
	if p.bridge != nil {
		heldMessages = len(p.bridge.received) + boolInt(p.bridge.writerBusy)
		heldBytes = p.bridge.heldBytes
	}
	if p.receiver.ActiveMessages()+p.queue.Pending()+heldMessages >= s.config.MaxMessages ||
		int(request.total) > s.config.SessionBufferBytes-p.receiver.ActiveBytes()-p.queue.QueuedBytes()-heldBytes {
		return false
	}
	if err := p.receiver.Reserve(request.id, request.total); err != nil {
		return false
	}
	p.lastProgress = now
	s.stats.CreditsGranted++
	if used := s.budget.Used(); used > s.stats.PeakBufferBytes {
		s.stats.PeakBufferBytes = used
	}
	s.sendFrame(p, reliable.Frame{Type: reliable.TypeGrant, ID: request.id, Total: request.total}, send)
	return true
}

func (s *Server) handleCreditReply(p *peerSession, f reliable.Frame, now time.Time) {
	if p.flowMode == 3 && p.queue != nil && f.Type == reliable.TypeReject && p.queue.MatchesWindowRequest(f) {
		s.remove(p, false)
		return
	}
	if p.flowMode != 1 || p.queue == nil {
		s.stats.InvalidPackets++
		return
	}
	switch f.Type {
	case reliable.TypeGrant:
		changed, err := p.queue.Grant(f)
		if err != nil {
			s.stats.InvalidPackets++
			return
		}
		if changed {
			p.lastProgress = now
		}
	case reliable.TypeBlocked:
		if err := p.queue.Blocked(f, now); err != nil {
			s.stats.InvalidPackets++
			return
		}
	case reliable.TypeReject:
		if !p.queue.MatchesOpen(f) {
			s.stats.InvalidPackets++
			return
		}
		// Current application is an echo harness: rejected response means the
		// transaction cannot finish, not a reason to silently discard accepted bytes.
		s.remove(p, false)
		return
	}
	p.confirmed, p.lastValid = true, now
	s.schedule(p, now, true)
}

func (s *Server) linkWaiter(p *peerSession) {
	if p.waiting {
		return
	}
	p.waiting = true
	p.waitPrev, p.waitNext = s.waitTail, nil
	if s.waitTail == nil {
		s.waitHead = p
	} else {
		s.waitTail.waitNext = p
	}
	s.waitTail = p
	s.waitCount++
	if s.waitCount > s.stats.PeakWaitingSessions {
		s.stats.PeakWaitingSessions = s.waitCount
	}
}

func (s *Server) unlinkWaiter(p *peerSession) {
	if !p.waiting {
		return
	}
	if p.waitPrev == nil {
		s.waitHead = p.waitNext
	} else {
		p.waitPrev.waitNext = p.waitNext
	}
	if p.waitNext == nil {
		s.waitTail = p.waitPrev
	} else {
		p.waitNext.waitPrev = p.waitPrev
	}
	p.waiting, p.waitPrev, p.waitNext = false, nil, nil
	s.waitCount--
	if s.waitCount == 0 {
		s.creditScan = 0
	}
}

// At most eight peers per event-loop turn, one FIFO request per peer. A full
// unsuccessful pass sleeps until storage is freed or new demand arrives; it
// does NOT poll every blocked session every Tick. One linked node per session.
func (s *Server) serviceCredits(now time.Time, send sendDatagram, start time.Time) int {
	frames := 0
	for work := 0; work < 8 && s.creditScan > 0 && s.waitHead != nil && time.Since(start) < time.Millisecond; work++ {
		p := s.waitHead
		s.creditScan--
		remaining := s.creditScan
		s.unlinkWaiter(p)
		s.creditScan = remaining
		if !now.Before(s.expires(p)) {
			s.remove(p, true)
			continue
		}

		progress := false
		// Bridge output is already present in the kernel/upstream side and can
		// otherwise deadlock behind receive reservations owned by other sessions.
		// Give one pending read a fair opportunity whenever shared storage frees.
		if p.bridge != nil && p.bridge.pendingRead != nil && p.bridge.readBudgetBlocked {
			if s.tryBridgeRead(p, now, send) {
				progress = true
			}
		}

		creditProgress := false
		if p.flowMode == 3 {
			if p.windowGranted < p.windowWanted {
				creditProgress = s.grantWindow(p, now, send)
			}
		} else if len(p.pendingOpens) > 0 {
			request := p.pendingOpens[0]
			creditProgress = s.grant(p, request, now, send)
			if creditProgress {
				copy(p.pendingOpens, p.pendingOpens[1:])
				p.pendingOpens[len(p.pendingOpens)-1] = openRequest{}
				p.pendingOpens = p.pendingOpens[:len(p.pendingOpens)-1]
			}
		}
		if s.sessions[p.id] == nil {
			continue
		}
		if creditProgress {
			frames++
		}
		progress = progress || creditProgress
		if s.hasWaitDemand(p) {
			s.linkWaiter(p)
		}
		if progress {
			s.creditScan = max(s.creditScan, s.waitCount)
			s.schedule(p, now, true)
		}
	}
	if s.waitHead == nil {
		s.creditScan = 0
	}
	return frames
}
