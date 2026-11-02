package netreliable

import (
	"context"
	"errors"
	"io"
	"net"
	"time"

	"example.com/srdt/reliable"
	"example.com/srdt/secureudp"
)

type bridgeEventKind byte

const (
	bridgeDialDone bridgeEventKind = iota + 1
	bridgeReadDone
	bridgeWriteDone
)

type bridgeEvent struct {
	kind      bridgeEventKind
	id        [16]byte
	conn      *net.TCPConn
	data      []byte // borrowed until consumed for bridgeReadDone
	err       error
	messageID uint64
	bytes     int
}

type bridgeWriteJob struct {
	message *reliable.Received
	id      uint64
}

func (s *Server) bridgeDirectionBytes(b *bridgePeer) int {
	limit := s.config.SessionBufferBytes / 2
	if limit < b.chunk {
		limit = b.chunk
	}
	return min(limit, s.config.SessionBufferBytes)
}

func (s *Server) bridgeDirectionMessages(b *bridgePeer) int {
	limit := max(1, s.config.MaxMessages/2)
	return min(limit, b.maxActive)
}

type bridgePeer struct {
	chunk        int
	maxActive    int
	opening      bool
	open         bool
	failed       bool
	closed       bool
	draining     bool
	closedAt     time.Time
	conn         *net.TCPConn
	cancel       context.CancelFunc
	readConsumed chan struct{}
	writeJobs    chan bridgeWriteJob

	pendingRead        *bridgeEvent
	writerBusy         bool
	writerBusyID       uint64
	received           map[uint64]*reliable.Received
	heldBytes          int
	nextInbound        uint64
	highestInboundSeen uint64
	nextOutbound       uint64
	lastOutbound       uint64

	remoteFinSeen     bool
	remoteFinID       uint64
	remoteFinApplied  bool
	localEOF          bool
	localFin          controlRetry
	readBudgetBlocked bool
}

func (s *Server) bridgeEnabled() bool { return s.config.Upstream != "" }

func (s *Server) emitBridge(ctx context.Context, event bridgeEvent) {
	if s.wake != nil {
		s.wake()
	}
	select {
	case s.bridgeEvents <- event:
	case <-ctx.Done():
		if event.conn != nil {
			_ = event.conn.Close()
		}
	}
	if s.wake != nil {
		s.wake()
	}
}

func (s *Server) startBridgeDial(p *peerSession) {
	if !s.bridgeEnabled() || p.bridge == nil || p.bridge.opening || p.bridge.open || p.bridge.failed || p.bridge.closed {
		return
	}
	select {
	case s.bridgeDialSem <- struct{}{}:
	default:
		return // Client retransmits STREAM_OPEN; do not create waiting dial goroutines.
	}
	p.bridge.opening = true
	id := p.id
	parent := s.runCtx
	upstream := s.config.Upstream
	timeout := s.config.UpstreamDialTimeout
	go func() {
		defer func() { <-s.bridgeDialSem }()
		ctx, cancel := context.WithTimeout(parent, timeout)
		defer cancel()
		conn, err := (&net.Dialer{}).DialContext(ctx, "tcp", upstream)
		if err != nil {
			s.emitBridge(parent, bridgeEvent{kind: bridgeDialDone, id: id, err: err})
			return
		}
		tcp, ok := conn.(*net.TCPConn)
		if !ok {
			_ = conn.Close()
			s.emitBridge(parent, bridgeEvent{kind: bridgeDialDone, id: id, err: errors.New("upstream is not TCP")})
			return
		}
		s.emitBridge(parent, bridgeEvent{kind: bridgeDialDone, id: id, conn: tcp})
	}()
}

func (s *Server) startBridgePumps(p *peerSession) {
	b := p.bridge
	ctx, cancel := context.WithCancel(s.runCtx)
	b.cancel = cancel
	b.readConsumed = make(chan struct{}, 1)
	b.writeJobs = make(chan bridgeWriteJob, 1)
	id := p.id
	conn := b.conn
	chunk := b.chunk

	go func() {
		buf := make([]byte, chunk)
		emptyReads := 0
		for {
			n, err := conn.Read(buf)
			if n > 0 {
				emptyReads = 0
				event := bridgeEvent{kind: bridgeReadDone, id: id, data: buf[:n], err: err}
				s.emitBridge(ctx, event)
				select {
				case <-b.readConsumed:
				case <-ctx.Done():
					return
				}
				if err != nil {
					return
				}
				continue
			}
			if err != nil {
				s.emitBridge(ctx, bridgeEvent{kind: bridgeReadDone, id: id, err: err})
				return
			}
			emptyReads++
			if emptyReads >= 100 {
				s.emitBridge(ctx, bridgeEvent{kind: bridgeReadDone, id: id, err: io.ErrNoProgress})
				return
			}
			select {
			case <-ctx.Done():
				return
			case <-time.After(time.Millisecond):
			}
		}
	}()

	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case job, ok := <-b.writeJobs:
				if !ok {
					return
				}
				result := bridgeEvent{kind: bridgeWriteDone, id: id, messageID: job.id}
				if job.message != nil {
					result.bytes = len(job.message.Data)
					result.err = writeFull(conn, job.message.Data)
					job.message.Release()
				}
				s.emitBridge(ctx, result)
				if result.err != nil {
					return
				}
			}
		}
	}()
}

func (s *Server) handleStreamOpen(p *peerSession, f reliable.Frame, now time.Time, send sendDatagram) {
	if !s.bridgeEnabled() || p.crypto.ProtocolVersion() != secureudp.ProtocolV1 ||
		!p.crypto.HasCapability(secureudp.CapabilityIndependentStreams) ||
		!p.crypto.HasCapability(secureudp.CapabilityStreaming) ||
		!p.crypto.HasCapability(secureudp.CapabilityWindowCredits) ||
		f.Total == 0 || int(f.Total) > s.config.MaxMessageBytes || f.Offset == 0 || int(f.Offset) > s.config.MaxMessages {
		s.stats.InvalidPackets++
		return
	}
	if p.bridge == nil {
		p.bridge = &bridgePeer{chunk: int(f.Total), maxActive: int(f.Offset), nextInbound: 1, nextOutbound: 2,
			received: make(map[uint64]*reliable.Received, int(f.Offset))}
	} else if p.bridge.chunk != int(f.Total) || p.bridge.maxActive != int(f.Offset) {
		s.sendFrame(p, reliable.Frame{Type: reliable.TypeStreamReset, Offset: 1}, send)
		return
	}
	p.confirmed, p.lastValid = true, now
	if p.bridge.failed {
		s.sendFrame(p, reliable.Frame{Type: reliable.TypeStreamReset, Offset: 1}, send)
		return
	}
	if p.bridge.open {
		s.sendFrame(p, reliable.Frame{Type: reliable.TypeStreamOpenAck, Total: f.Total, Offset: f.Offset}, send)
		s.schedule(p, now, true)
		return
	}
	s.startBridgeDial(p)
	s.schedule(p, now, true)
}

func (s *Server) handleStreamFin(p *peerSession, f reliable.Frame, now time.Time, send sendDatagram) {
	if p.bridge == nil || !p.bridge.open || f.Offset != streamDirectionClientToServer || (f.ID != 0 && f.ID%2 == 0) {
		s.stats.InvalidPackets++
		return
	}
	b := p.bridge
	if !streamFinConsistent(b.highestInboundSeen, f.ID) {
		s.sendFrame(p, reliable.Frame{Type: reliable.TypeStreamReset, Offset: 2}, send)
		return
	}
	if b.remoteFinSeen && b.remoteFinID != f.ID {
		s.sendFrame(p, reliable.Frame{Type: reliable.TypeStreamReset, Offset: 2}, send)
		return
	}
	b.remoteFinSeen, b.remoteFinID = true, f.ID
	p.confirmed, p.lastValid = true, now
	s.sendFrame(p, reliable.Frame{Type: reliable.TypeStreamFinAck, ID: f.ID, Offset: f.Offset}, send)
	s.tryBridgeWrite(p, now, send)
	s.tryBridgeHalfClose(p)
	s.schedule(p, now, true)
}

func (s *Server) handleStreamFinAck(p *peerSession, f reliable.Frame, now time.Time) {
	if p.bridge == nil || f.Offset != streamDirectionServerToClient || p.bridge.localFin.frame.Type == 0 || f.ID != p.bridge.localFin.frame.ID {
		s.stats.InvalidPackets++
		return
	}
	p.bridge.localFin.acked = true
	p.confirmed, p.lastValid, p.lastProgress = true, now, now
	s.maybeCloseBridgeIO(p)
	s.schedule(p, now, true)
}

func (s *Server) serviceBridgeEvents(now time.Time, send sendDatagram, start time.Time) int {
	if s.bridgeEvents == nil {
		return 0
	}
	frames := 0
	for work := 0; work < 64 && time.Since(start) < time.Millisecond; work++ {
		select {
		case event := <-s.bridgeEvents:
			p := s.sessions[event.id]
			if p == nil || p.bridge == nil {
				if event.conn != nil {
					_ = event.conn.Close()
				}
				continue
			}
			b := p.bridge
			switch event.kind {
			case bridgeDialDone:
				b.opening = false
				if event.err != nil {
					s.stats.BridgeDialFailures++
					b.failed = true
					s.sendFrame(p, reliable.Frame{Type: reliable.TypeStreamReset, Offset: 1}, send)
					frames++
					continue
				}
				if b.closed || b.open {
					_ = event.conn.Close()
					continue
				}
				if err := event.conn.SetReadBuffer(s.config.UpstreamSocketBuffer); err != nil {
					b.failed = true
					_ = event.conn.Close()
					s.sendFrame(p, reliable.Frame{Type: reliable.TypeStreamReset, Offset: 3}, send)
					frames++
					continue
				}
				if err := event.conn.SetWriteBuffer(s.config.UpstreamSocketBuffer); err != nil {
					b.failed = true
					_ = event.conn.Close()
					s.sendFrame(p, reliable.Frame{Type: reliable.TypeStreamReset, Offset: 3}, send)
					frames++
					continue
				}
				_ = event.conn.SetNoDelay(true)
				b.conn, b.open = event.conn, true
				s.stats.BridgeSessions++
				p.flowMode, p.windowCapacity = 3, uint32(b.chunk)
				s.initDelivery(p)
				if p.crypto.HasCapability(secureudp.CapabilityPacketTracking) && p.packetMode == 0 {
					if err := p.queue.EnablePacketTracking(s.packetBudget, s.config.Options.fragment(), packetWireOverhead); err != nil {
						b.failed = true
						_ = event.conn.Close()
						s.sendFrame(p, reliable.Frame{Type: reliable.TypeStreamReset, Offset: 3}, send)
						frames++
						continue
					}
					p.packetMode = 2
				}
				s.startBridgePumps(p)
				s.sendFrame(p, reliable.Frame{Type: reliable.TypeStreamOpenAck, Total: uint32(b.chunk), Offset: uint32(b.maxActive)}, send)
				frames++
				p.lastProgress = now
			case bridgeReadDone:
				if !b.open || b.closed || b.pendingRead != nil {
					continue
				}
				copyEvent := event
				b.pendingRead = &copyEvent
				s.tryBridgeRead(p, now, send)
			case bridgeWriteDone:
				if !b.writerBusy || b.writerBusyID != event.messageID {
					continue
				}
				b.writerBusy, b.writerBusyID = false, 0
				b.heldBytes -= event.bytes
				if b.heldBytes < 0 {
					b.heldBytes = 0
				}
				if event.err != nil {
					b.failed = true
					s.sendFrame(p, reliable.Frame{Type: reliable.TypeStreamReset, Offset: 2}, send)
					frames++
					s.closeBridgeIO(p)
					continue
				}
				if event.messageID != b.nextInbound {
					b.failed = true
					s.sendFrame(p, reliable.Frame{Type: reliable.TypeStreamReset, Offset: 2}, send)
					frames++
					s.closeBridgeIO(p)
					continue
				}
				b.nextInbound += 2
				s.stats.BridgeUpstreamWrittenBytes += uint64(event.bytes)
				s.creditScan = s.waitCount
				p.lastProgress = now
				// A pending upstream read may have been blocked only because the
				// session resource limit included this in-progress write. Revisit it
				// immediately when the write releases ownership instead of waiting
				// for an unrelated ACK or the session timer.
				s.tryBridgeRead(p, now, send)
				s.tryBridgeWrite(p, now, send)
				s.tryBridgeHalfClose(p)
			}
			if s.sessions[p.id] != nil {
				s.schedule(p, now, true)
			}
		default:
			return frames
		}
	}
	return frames
}

func (s *Server) tryBridgeRead(p *peerSession, now time.Time, send sendDatagram) bool {
	b := p.bridge
	if b == nil || b.pendingRead == nil || !b.open || b.closed || p.queue == nil {
		return false
	}
	event := b.pendingRead
	if len(event.data) == 0 {
		b.pendingRead = nil
		b.readBudgetBlocked = false
		if errors.Is(event.err, io.EOF) {
			s.bridgeLocalEOF(p, now)
		} else if event.err != nil {
			b.failed = true
			s.sendFrame(p, reliable.Frame{Type: reliable.TypeStreamReset, Offset: 2}, send)
			s.closeBridgeIO(p)
		}
		if p.waiting && !s.hasWaitDemand(p) {
			s.unlinkWaiter(p)
		}
		return true
	}
	directionMessages := s.bridgeDirectionMessages(b)
	directionBytes := s.bridgeDirectionBytes(b)
	if p.queue.Pending() >= directionMessages ||
		p.receiver.ActiveMessages()+p.queue.Pending()+len(b.received)+boolInt(b.writerBusy) >= s.config.MaxMessages ||
		p.queue.QueuedBytes()+len(event.data) > directionBytes ||
		len(event.data) > s.config.SessionBufferBytes-p.receiver.ActiveBytes()-p.queue.QueuedBytes()-b.heldBytes {
		b.readBudgetBlocked = false
		return false
	}
	id := b.nextOutbound
	if id == 0 || id == ^uint64(0) {
		b.failed = true
		s.sendFrame(p, reliable.Frame{Type: reliable.TypeStreamReset, Offset: 3}, send)
		s.closeBridgeIO(p)
		return false
	}
	if err := p.queue.Enqueue(id, event.data, 80*time.Millisecond); err != nil {
		if errors.Is(err, reliable.ErrBudgetExceeded) {
			b.readBudgetBlocked = true
			s.linkWaiter(p)
			s.creditScan = max(s.creditScan, s.waitCount)
			return false
		}
		if errors.Is(err, reliable.ErrQueueFull) {
			b.readBudgetBlocked = false
			return false
		}
		// The preflight checks above make other enqueue failures an internal
		// invariant violation. Fail this bridge rather than leaving a read pump
		// permanently blocked with no wake source.
		b.failed = true
		s.sendFrame(p, reliable.Frame{Type: reliable.TypeStreamReset, Offset: 3}, send)
		s.closeBridgeIO(p)
		return false
	}
	b.readBudgetBlocked = false
	_ = p.queue.RequestWindow(id)
	b.lastOutbound, b.nextOutbound = id, id+2
	s.stats.BridgeUpstreamReadBytes += uint64(len(event.data))
	b.pendingRead = nil
	p.lastProgress = now
	select {
	case b.readConsumed <- struct{}{}:
	default:
	}
	if p.waiting && !s.hasWaitDemand(p) {
		s.unlinkWaiter(p)
	}
	if errors.Is(event.err, io.EOF) {
		s.bridgeLocalEOF(p, now)
	} else if event.err != nil {
		b.failed = true
		s.sendFrame(p, reliable.Frame{Type: reliable.TypeStreamReset, Offset: 2}, send)
		s.closeBridgeIO(p)
	}
	return true
}

func (s *Server) bridgeLocalEOF(p *peerSession, now time.Time) {
	b := p.bridge
	if b == nil || b.localEOF {
		return
	}
	b.localEOF = true
	b.localFin.frame = reliable.Frame{Type: reliable.TypeStreamFin, ID: b.lastOutbound, Offset: streamDirectionServerToClient}
	b.localFin.next = time.Time{}
	p.lastProgress = now
}

func (s *Server) tryBridgeWrite(p *peerSession, now time.Time, send sendDatagram) {
	b := p.bridge
	if b == nil || !b.open || b.closed || b.writerBusy {
		return
	}
	message := b.received[b.nextInbound]
	if message == nil {
		return
	}
	select {
	case b.writeJobs <- bridgeWriteJob{message: message, id: b.nextInbound}:
		delete(b.received, b.nextInbound)
		b.writerBusy, b.writerBusyID = true, b.nextInbound
	default:
	}
}

func (s *Server) tryBridgeHalfClose(p *peerSession) {
	b := p.bridge
	if b == nil || !b.open || b.closed || !b.remoteFinSeen || b.remoteFinApplied || b.writerBusy || b.nextInbound <= b.remoteFinID {
		return
	}
	if b.conn != nil {
		_ = b.conn.CloseWrite()
	}
	b.remoteFinApplied = true
	s.stats.BridgeHalfCloses++
	s.maybeCloseBridgeIO(p)
}

func (s *Server) maybeCloseBridgeIO(p *peerSession) {
	b := p.bridge
	if b == nil || b.closed || !b.localEOF || !b.localFin.acked || !b.remoteFinSeen || !b.remoteFinApplied || b.writerBusy || b.pendingRead != nil || len(b.received) != 0 || (p.queue != nil && p.queue.Pending() != 0) {
		return
	}
	b.draining = true
	s.closeBridgeIO(p)
}

func (s *Server) closeBridgeIO(p *peerSession) {
	b := p.bridge
	if b == nil || b.closed {
		return
	}
	b.closed = true
	if b.closedAt.IsZero() {
		b.closedAt = time.Now()
	}
	if b.cancel != nil {
		b.cancel()
	}
	if b.conn != nil {
		_ = b.conn.Close()
		b.conn = nil
	}
	for id, message := range b.received {
		message.Release()
		delete(b.received, id)
	}
	b.heldBytes = 0
}

func (s *Server) closeBridge(p *peerSession) {
	if p.bridge == nil {
		return
	}
	s.closeBridgeIO(p)
}

func (s *Server) serviceBridgeControl(p *peerSession, now time.Time, send sendDatagram) int {
	b := p.bridge
	if b == nil || !b.open || b.closed || b.localFin.frame.Type == 0 || !b.localFin.due(now) {
		return 0
	}
	s.sendFrame(p, b.localFin.frame, send)
	b.localFin.sent(now)
	return 1
}

func (s *Server) bridgeWake(p *peerSession) (time.Time, bool) {
	if p.bridge == nil || p.bridge.localFin.frame.Type == 0 {
		return time.Time{}, false
	}
	return p.bridge.localFin.wake()
}

func boolInt(v bool) int {
	if v {
		return 1
	}
	return 0
}
