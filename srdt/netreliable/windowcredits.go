package netreliable

import (
	"example.com/srdt/reliable"
	"time"
)

func (s *Server) hasCreditDemand(p *peerSession) bool {
	return len(p.pendingOpens) > 0 || (p.flowMode == 3 && p.windowWanted > p.windowGranted)
}

func (s *Server) hasWaitDemand(p *peerSession) bool {
	return s.hasCreditDemand(p) || (p.bridge != nil && p.bridge.pendingRead != nil && p.bridge.readBudgetBlocked)
}

// The authenticated application requests an ID prefix, not permission to allocate
// on DATA. Every newly granted ID is reserved in Receiver BEFORE publication.
func (s *Server) handleWindowOpen(p *peerSession, f reliable.Frame, now time.Time, send sendDatagram) {
	s.stats.WindowRequests++
	if (p.flowMode != 0 && p.flowMode != 3) || f.ID%2 == 0 || f.ID == ^uint64(0) ||
		int(f.Total) > s.config.MaxMessageBytes ||
		(p.windowCapacity != 0 && p.windowCapacity != f.Total) ||
		(f.ID >= p.nextRequest && (f.ID-p.nextRequest)/2 >= 128) ||
		(s.config.CompletionMessages > 0 && (f.ID/2+1) > s.config.CompletionMessages) {
		s.rejectOpen(p, f, 1, send)
		return
	}
	idle := !s.hasCreditDemand(p) && (p.receiver == nil || (p.receiver.ActiveMessages() == 0 && p.queue.Pending() == 0))
	p.flowMode, p.windowCapacity = 3, f.Total
	s.initDelivery(p)
	p.confirmed, p.lastValid = true, now
	if f.ID > p.windowWanted {
		p.windowWanted = f.ID
		if idle {
			p.lastProgress = now
		}
		if p.bridge == nil {
			// Echo application knows its future reply IDs and maximum lengths.
			_ = p.queue.RequestWindow(f.ID + 1)
		}
		if s.hasCreditDemand(p) {
			s.linkWaiter(p)
			s.creditScan = s.waitCount
		}
	}
	// Recover a lost cumulative GRANT without reserving again or refreshing the
	// no-progress deadline. A duplicate cannot trigger a scan of all waiters.
	if p.windowGranted != 0 && (p.lastWindowReply.IsZero() || now.Sub(p.lastWindowReply) >= 50*time.Millisecond) {
		s.sendWindowGrant(p, now, send)
	}
	if s.sessions[p.id] != nil {
		s.schedule(p, now, true)
	}
}

func (s *Server) sendWindowGrant(p *peerSession, now time.Time, send sendDatagram) {
	p.lastWindowReply = now
	s.stats.WindowGrantUpdates++
	s.sendFrame(p, reliable.Frame{Type: reliable.TypeWindowGrant, ID: p.windowGranted, Total: p.windowCapacity}, send)
}

func (s *Server) grantWindow(p *peerSession, now time.Time, send sendDatagram) bool {
	if p.windowGranted >= p.windowWanted {
		return false
	}
	heldMessages, heldBytes := 0, 0
	if p.bridge != nil {
		heldMessages = len(p.bridge.received) + boolInt(p.bridge.writerBusy)
		heldBytes = p.bridge.heldBytes
	}
	granted := 0
	// Publish a small cumulative batch per fair scheduler visit. Reserving one
	// ID per visit made long streams artificially depend on the 5 ms scheduler
	// cadence (thousands of chunks => tens of seconds) even on lossless loopback.
	// Four matches the bounded client-side reply window and remains small enough
	// that one busy peer cannot consume an unbounded share of the global budget.
	for granted < 4 && p.windowGranted < p.windowWanted {
		if p.bridge != nil {
			directionMessages := s.bridgeDirectionMessages(p.bridge)
			directionBytes := s.bridgeDirectionBytes(p.bridge)
			if p.receiver.ActiveMessages()+len(p.bridge.received)+boolInt(p.bridge.writerBusy) >= directionMessages ||
				p.receiver.ActiveBytes()+heldBytes+int(p.windowCapacity) > directionBytes {
				break
			}
		}
		if p.receiver.ActiveMessages()+p.queue.Pending()+heldMessages >= s.config.MaxMessages ||
			int(p.windowCapacity) > s.config.SessionBufferBytes-p.receiver.ActiveBytes()-p.queue.QueuedBytes()-heldBytes {
			break
		}
		id := uint64(1)
		if p.windowGranted != 0 {
			id = p.windowGranted + 2
		}
		if err := p.receiver.ReserveCapacity(id, p.windowCapacity); err != nil {
			break
		}
		p.windowGranted = id
		granted++
	}
	if granted == 0 {
		return false
	}
	p.lastProgress = now
	s.stats.CreditsGranted += uint64(granted)
	if used := s.budget.Used(); used > s.stats.PeakBufferBytes {
		s.stats.PeakBufferBytes = used
	}
	s.sendWindowGrant(p, now, send)
	return true
}

// The client also reserves every reply slot before advertising it. This is
// bounded by Receiver limits, with no payload allocation until DATA arrives.
type clientReplyWindow struct {
	capacity        uint32
	wanted, granted uint64
	lastReply       time.Time
}

func (w *clientReplyWindow) service(r *reliable.Receiver, now time.Time, probe bool) (reliable.Frame, bool) {
	changed := false
	for work := 0; work < 4 && w.granted < w.wanted; work++ {
		id := uint64(2)
		if w.granted != 0 {
			id = w.granted + 2
		}
		if err := r.ReserveCapacity(id, w.capacity); err != nil {
			break
		}
		w.granted, changed = id, true
	}
	if w.granted == 0 || (!changed && (!probe || (!w.lastReply.IsZero() && now.Sub(w.lastReply) < 50*time.Millisecond))) {
		return reliable.Frame{}, false
	}
	w.lastReply = now
	return reliable.Frame{Type: reliable.TypeWindowGrant, ID: w.granted, Total: w.capacity}, true
}
