package netreliable

import (
	"context"
	"crypto/ecdh"
	"errors"
	"fmt"
	"io"
	"net"
	"time"

	"example.com/srdt/reliable"
	"example.com/srdt/secureudp"
)

const (
	streamDirectionClientToServer uint32 = 1
	streamDirectionServerToClient uint32 = 2
)

func streamFinConsistent(highestSeen, finID uint64) bool {
	if highestSeen == 0 {
		return true
	}
	return finID != 0 && finID >= highestSeen
}

// HalfCloseDuplex is the bridge-facing endpoint contract. CloseWrite must
// half-close the application-facing write direction without closing reads.
type HalfCloseDuplex interface {
	io.ReadWriteCloser
	CloseWrite() error
}

type controlRetry struct {
	frame   reliable.Frame
	next    time.Time
	backoff time.Duration
	acked   bool
}

func (c *controlRetry) due(now time.Time) bool {
	return !c.acked && (c.next.IsZero() || !now.Before(c.next))
}

func (c *controlRetry) sent(now time.Time) {
	if c.backoff == 0 {
		c.backoff = 150 * time.Millisecond
	} else {
		c.backoff = min(1200*time.Millisecond, c.backoff*2)
	}
	c.next = now.Add(c.backoff)
}

func (c *controlRetry) wake() (time.Time, bool) {
	if c.acked {
		return time.Time{}, false
	}
	if c.next.IsZero() {
		return time.Now(), true
	}
	return c.next, true
}

func independentOpen(ctx context.Context, conn net.PacketConn, addr net.Addr, session *secureudp.Session, config StreamConfig, o Options, deadline time.Time) error {
	open := reliable.Frame{Type: reliable.TypeStreamOpen, Total: uint32(config.ChunkSize), Offset: uint32(config.MaxActive)}
	var next time.Time
	buf := make([]byte, secureudp.MaxDatagramSize+1)
	for {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		now := time.Now()
		if !now.Before(deadline) {
			return errors.New("independent stream open timeout")
		}
		if next.IsZero() || !now.Before(next) {
			if err := o.send(conn, addr, session, open); err != nil {
				return err
			}
			next = now.Add(150 * time.Millisecond)
		}
		readUntil := minTime(deadline, minTime(next, now.Add(100*time.Millisecond)))
		if err := conn.SetReadDeadline(readUntil); err != nil {
			return err
		}
		n, from, err := conn.ReadFrom(buf)
		if err != nil {
			if secureudp.IsDatagramReadError(err) {
				continue
			}
			if ne, ok := err.(net.Error); ok && ne.Timeout() {
				continue
			}
			return err
		}
		if from.String() != addr.String() {
			continue
		}
		plain, err := session.Open(buf[:n])
		if err != nil {
			continue
		}
		f, err := reliable.Decode(plain)
		if err != nil {
			continue
		}
		switch f.Type {
		case reliable.TypeStreamOpenAck:
			if f.Total == uint32(config.ChunkSize) && f.Offset == uint32(config.MaxActive) {
				return nil
			}
		case reliable.TypeStreamReset:
			return fmt.Errorf("independent stream reset during open (reason %d)", f.Offset)
		}
	}
}

// ExchangeIndependentDuplex carries two independent ordered byte streams over
// one SRDT session. Client->server uses odd logical message IDs; server->client
// uses even IDs. It requires V1 independent-stream capability and explicit
// half-close support from the owned local endpoint.
func ExchangeIndependentDuplex(ctx context.Context, server string, public *ecdh.PublicKey, psk []byte, endpoint HalfCloseDuplex, config StreamConfig, o Options) (StreamStats, error) {
	var stats StreamStats
	if endpoint == nil {
		return stats, errors.New("nil duplex endpoint")
	}
	defer endpoint.Close()
	config, err := config.normalized()
	if err != nil {
		return stats, err
	}
	if o.LegacyHandshake || o.LegacyCredits {
		return stats, secureudp.ErrIncompatible
	}
	addr, err := net.ResolveUDPAddr("udp", server)
	if err != nil {
		return stats, err
	}
	conn, err := net.ListenPacket("udp", ":0")
	if err != nil {
		return stats, err
	}
	defer conn.Close()
	handshakeDeadline := time.Now().Add(o.timeout())
	required := secureudp.CapabilityStreaming | secureudp.CapabilityIndependentStreams
	if !o.LegacyLiveness {
		required |= secureudp.CapabilityPathLiveness
	}
	session, err := clientHandshake(ctx, conn, addr, public, psk, handshakeDeadline, o, required)
	if err != nil {
		return stats, err
	}
	if !session.HasCapability(secureudp.CapabilityStreaming) || !session.HasCapability(secureudp.CapabilityIndependentStreams) || !session.HasCapability(secureudp.CapabilityWindowCredits) {
		return stats, secureudp.ErrIncompatible
	}
	pathLiveness := session.HasCapability(secureudp.CapabilityPathLiveness)
	keepaliveInterval := o.keepaliveInterval()
	keepaliveTimeout := o.keepaliveTimeout()
	if pathLiveness && (keepaliveInterval < 10*time.Millisecond || keepaliveTimeout <= keepaliveInterval) {
		return stats, errors.New("invalid path-liveness intervals")
	}
	stats.ProtocolVersion = session.ProtocolVersion()
	stats.Capabilities = session.Capabilities()
	if err := independentOpen(ctx, conn, addr, session, config, o, handshakeDeadline); err != nil {
		return stats, err
	}
	lastRx := time.Now()
	lastTx := lastRx
	lastPingID := uint64(0)
	// De-synchronize large client populations without a timer/goroutine per
	// connection. The session already owns secret-derived pseudo-random padding
	// state; consume one sample to choose a stable 90..110% keepalive period.
	keepalivePeriod := keepaliveInterval
	if pathLiveness {
		jitterPercent := 90 + session.NextControlPadding()%21
		keepalivePeriod = keepaliveInterval * time.Duration(jitterPercent) / 100
	}
	sendFrame := func(f reliable.Frame) error {
		if err := o.send(conn, addr, session, f); err != nil {
			return err
		}
		lastTx = time.Now()
		return nil
	}

	pumpCtx, pumpCancel := context.WithCancel(ctx)
	defer pumpCancel()
	wake := func() { _ = conn.SetReadDeadline(time.Now()) }
	reader := startStreamReader(pumpCtx, endpoint, config.ChunkSize, wake)
	writer := startStreamWriter(pumpCtx, endpoint, wake)
	var pendingRead *streamReadEvent
	writerBusy := false

	queue, err := reliable.NewQueue(min(64<<10, config.MaxBuffered), config.MaxBuffered)
	if err != nil {
		return stats, err
	}
	defer queue.Close()
	o.configureQueue(queue)
	packetMode := session.HasCapability(secureudp.CapabilityPacketTracking)
	if packetMode {
		if err := queue.EnablePacketTracking(nil, o.fragment(), packetWireOverhead); err != nil {
			return stats, err
		}
	}
	if err := queue.UseWindowCredits(1, 2, config.ChunkSize); err != nil {
		return stats, err
	}

	limits := reliable.LowMemoryReceiverLimits()
	limits.MaxActiveMessages = max(limits.MaxActiveMessages, config.MaxActive)
	limits.MaxActiveBytes = max(limits.MaxActiveBytes, config.MaxBuffered)
	receiver, err := reliable.NewReceiverWithLimits(limits)
	if err != nil {
		return stats, err
	}
	defer receiver.Close()
	receiveBudget, err := reliable.NewBudget(int64(config.MaxBuffered))
	if err != nil {
		return stats, err
	}
	if err := receiver.SetBudget(receiveBudget); err != nil {
		return stats, err
	}
	if err := receiver.RequireCredits(); err != nil {
		return stats, err
	}
	if packetMode && o.BatchPacketAcks && !o.LegacyPacketAcks && session.HasCapability(secureudp.CapabilityPacketAckBatch) {
		_ = receiver.EnablePacketAckBatching()
	}

	inboundWindow := clientReplyWindow{capacity: uint32(config.ChunkSize)}
	received := make(map[uint64]*reliable.Received, config.MaxActive)
	defer func() {
		for _, message := range received {
			message.Release()
		}
	}()

	nextOutID := uint64(1)
	highestOutID := uint64(0)
	nextInID := uint64(2)
	highestInSeen := uint64(0)
	localEOF := false
	localFin := controlRetry{}
	remoteFinSeen := false
	remoteFinID := uint64(0)
	remoteWriteClosed := false
	var completedAt time.Time
	buf := make([]byte, secureudp.MaxDatagramSize+1)

	for {
		now := time.Now()
		if !pathLiveness && !now.Before(handshakeDeadline) {
			return stats, fmt.Errorf("independent stream timeout: pending=%d rx=%d", queue.Pending(), len(received))
		}
		if ctx.Err() != nil {
			return stats, ctx.Err()
		}
		if pathLiveness {
			if now.Sub(lastRx) >= keepaliveTimeout {
				// Best effort: the reverse path may be dead while client->server is
				// still usable. An authenticated RESET lets the server release its
				// upstream TCP socket immediately instead of retaining it until its
				// idle timeout. Failure is expected when the whole path is gone.
				_ = sendFrame(reliable.Frame{Type: reliable.TypeStreamReset, Offset: 4})
				return stats, errors.New("independent stream path-liveness timeout")
			}
			if now.Sub(lastTx) >= keepalivePeriod {
				if lastPingID == ^uint64(0) {
					return stats, errors.New("path-liveness token exhausted")
				}
				lastPingID++
				if err := sendFrame(reliable.Frame{Type: reliable.TypePathPing, ID: lastPingID}); err != nil {
					return stats, err
				}
				stats.PathPingsSent++
			}
		}

		for _, ack := range receiver.FlushPacketAcks(now) {
			if err := sendFrame(ack); err != nil {
				return stats, err
			}
		}

		if writerBusy {
			select {
			case result, ok := <-writer.done:
				if !ok {
					return stats, io.ErrClosedPipe
				}
				writerBusy = false
				if result.err != nil {
					if ctx.Err() != nil {
						return stats, ctx.Err()
					}
					return stats, result.err
				}
				if uint64(result.requestID) != nextInID {
					return stats, errors.New("independent stream writer order violation")
				}
				stats.ReceivedChunks++
				stats.ReceivedBytes += uint64(result.bytes)
				nextInID += 2
			default:
			}
		}

		if !localEOF && pendingRead == nil {
			select {
			case event, ok := <-reader.events:
				if !ok {
					localEOF = true
				} else {
					pendingRead = &event
				}
			default:
			}
		}
		if pendingRead != nil && len(pendingRead.data) == 0 {
			terminal := pendingRead.err
			pendingRead = nil
			if errors.Is(terminal, io.EOF) {
				localEOF = true
			} else if terminal != nil {
				return stats, terminal
			}
		}
		if pendingRead != nil && queue.Pending() < config.MaxActive && queue.QueuedBytes()+len(pendingRead.data) <= config.MaxBuffered {
			if nextOutID == 0 || nextOutID == ^uint64(0) {
				return stats, errors.New("independent stream message ID exhausted")
			}
			n := len(pendingRead.data)
			if err := queue.Enqueue(nextOutID, pendingRead.data, 80*time.Millisecond); err != nil {
				return stats, err
			}
			highestOutID = nextOutID
			nextOutID += 2
			stats.SentChunks++
			stats.SentBytes += uint64(n)
			stats.PeakTransactions = max(stats.PeakTransactions, queue.Pending())
			stats.PeakQueueBytes = max(stats.PeakQueueBytes, queue.QueuedBytes())
			terminal := pendingRead.err
			pendingRead = nil
			select {
			case reader.consumed <- struct{}{}:
			default:
			}
			if errors.Is(terminal, io.EOF) {
				localEOF = true
			} else if terminal != nil {
				return stats, terminal
			}
		}

		if highestOutID != 0 {
			_ = queue.RequestWindow(highestOutID)
		}
		if localEOF && localFin.frame.Type == 0 {
			localFin.frame = reliable.Frame{Type: reliable.TypeStreamFin, ID: highestOutID, Offset: streamDirectionClientToServer}
		}
		if localFin.frame.Type != 0 && localFin.due(now) {
			if err := sendFrame(localFin.frame); err != nil {
				return stats, err
			}
			localFin.sent(now)
		}

		if grant, ok := inboundWindow.service(receiver, now, false); ok {
			if err := sendFrame(grant); err != nil {
				return stats, err
			}
		}

		for i := 0; i < 16; i++ {
			f, ok := queue.NextSized(now, o.fragment(), o.retryFragment())
			if !ok {
				break
			}
			if err := sendFrame(f); err != nil {
				return stats, err
			}
		}

		if !writerBusy {
			if message := received[nextInID]; message != nil {
				select {
				case writer.jobs <- streamWriteJob{response: message, requestID: nextInID}:
					delete(received, nextInID)
					writerBusy = true
				default:
				}
			}
		}
		if remoteFinSeen && !remoteWriteClosed && !writerBusy && nextInID > remoteFinID {
			if err := endpoint.CloseWrite(); err != nil && !errors.Is(err, net.ErrClosed) {
				return stats, err
			}
			remoteWriteClosed = true
		}

		done := localEOF && localFin.acked && remoteFinSeen && remoteWriteClosed && !writerBusy && queue.Pending() == 0 && len(received) == 0
		if done {
			if completedAt.IsZero() {
				completedAt = now
			}
			if now.Sub(completedAt) >= config.DrainTimeout {
				return stats, nil
			}
		} else {
			completedAt = time.Time{}
		}

		// Application pumps explicitly wake the UDP owner with SetReadDeadline(now).
		// Keep the old short reaction time while protocol work is outstanding, but
		// avoid a 20 ms polling loop for a genuinely quiet bridge: at 1000 idle
		// sessions that alone would create ~50k useless wakeups/s.
		poll := 500 * time.Millisecond
		if pendingRead != nil || writerBusy || queue.Pending() > 0 || len(received) > 0 || localFin.frame.Type != 0 || remoteFinSeen {
			poll = 20 * time.Millisecond
		}
		readAt := time.Now().Add(poll)
		if at, ok := queue.Wake(time.Now()); ok && at.Before(readAt) {
			readAt = maxTime(at, time.Now().Add(eventLoopMinimumSleep))
		}
		if at, ok := receiver.PacketAckWake(); ok && at.Before(readAt) {
			readAt = maxTime(at, time.Now().Add(eventLoopMinimumSleep))
		}
		if at, ok := localFin.wake(); ok && at.Before(readAt) {
			readAt = maxTime(at, time.Now().Add(eventLoopMinimumSleep))
		}
		if pathLiveness {
			if at := lastTx.Add(keepalivePeriod); at.Before(readAt) {
				readAt = maxTime(at, time.Now().Add(eventLoopMinimumSleep))
			}
			if at := lastRx.Add(keepaliveTimeout); at.Before(readAt) {
				readAt = maxTime(at, time.Now().Add(eventLoopMinimumSleep))
			}
		}
		_ = conn.SetReadDeadline(readAt)
		n, from, readErr := conn.ReadFrom(buf)
		if readErr != nil {
			if secureudp.IsDatagramReadError(readErr) {
				continue
			}
			if ne, ok := readErr.(net.Error); ok && ne.Timeout() {
				continue
			}
			return stats, readErr
		}
		if from.String() != addr.String() {
			continue
		}
		plain, err := session.Open(buf[:n])
		if err != nil {
			continue
		}
		f, err := reliable.Decode(plain)
		if err != nil {
			continue
		}
		now = time.Now()
		lastRx = now
		switch f.Type {
		case reliable.TypePathPong:
			if pathLiveness && f.ID <= lastPingID {
				stats.PathPongsReceived++
			}
			continue
		case reliable.TypePathPing:
			// Stage 24 uses client-originated probes. Ignore an otherwise valid
			// unexpected server probe rather than creating a ping-pong loop.
			continue
		case reliable.TypeWindowOpen:
			if f.ID < nextInID || f.ID%2 != 0 || f.Total != uint32(config.ChunkSize) {
				continue
			}
			if (f.ID-nextInID)/2 >= uint64(config.MaxActive) {
				continue
			}
			inboundWindow.wanted = max(inboundWindow.wanted, f.ID)
			if grant, ok := inboundWindow.service(receiver, now, true); ok {
				if err := sendFrame(grant); err != nil {
					return stats, err
				}
			}
			continue
		case reliable.TypeWindowGrant:
			_, _ = queue.GrantWindow(f)
			continue
		case reliable.TypeAck, reliable.TypePacketAck, reliable.TypePacketAckBatch:
			_ = queue.AckAt(f, now)
			continue
		case reliable.TypeStreamFin:
			if f.Offset != streamDirectionServerToClient || (f.ID != 0 && f.ID%2 != 0) {
				continue
			}
			if !streamFinConsistent(highestInSeen, f.ID) {
				return stats, errors.New("remote stream FIN precedes observed DATA")
			}
			if remoteFinSeen && remoteFinID != f.ID {
				return stats, errors.New("conflicting remote stream FIN")
			}
			remoteFinSeen, remoteFinID = true, f.ID
			if err := sendFrame(reliable.Frame{Type: reliable.TypeStreamFinAck, ID: f.ID, Offset: f.Offset}); err != nil {
				return stats, err
			}
			continue
		case reliable.TypeStreamFinAck:
			if f.Offset == streamDirectionClientToServer && localFin.frame.Type != 0 && f.ID == localFin.frame.ID {
				localFin.acked = true
			}
			continue
		case reliable.TypeStreamReset:
			return stats, fmt.Errorf("independent stream reset (reason %d)", f.Offset)
		case reliable.TypeStreamOpenAck:
			// Duplicate open acknowledgement after a lost packet is harmless.
			continue
		}

		if f.Type != reliable.TypeData && f.Type != reliable.TypePacketData {
			continue
		}
		if (packetMode && f.Type != reliable.TypePacketData) || (!packetMode && f.Type != reliable.TypeData) || f.ID < 2 || f.ID%2 != 0 {
			continue
		}
		if remoteFinSeen && f.ID > remoteFinID {
			continue
		}
		if f.ID > highestInSeen {
			highestInSeen = f.ID
		}
		if !receiver.Known(f.ID) {
			continue
		}
		acks, message, err := receiver.AcceptRetainedAt(f, now)
		if err != nil {
			continue
		}
		for _, ack := range acks {
			if err := sendFrame(ack); err != nil {
				if message != nil {
					message.Release()
				}
				return stats, err
			}
		}
		if message != nil {
			if _, exists := received[f.ID]; exists {
				message.Release()
			} else {
				received[f.ID] = message
			}
		}
	}
}
