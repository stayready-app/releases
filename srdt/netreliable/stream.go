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
	DefaultStreamChunk    = 16 << 10
	DefaultStreamBuffered = 64 << 10
	DefaultStreamActive   = 4
)

type StreamConfig struct {
	ChunkSize    int
	MaxBuffered  int
	MaxActive    int
	DrainTimeout time.Duration
}

type StreamStats struct {
	SentChunks        uint64
	SentBytes         uint64
	ReceivedChunks    uint64
	ReceivedBytes     uint64
	PathPingsSent     uint64
	PathPongsReceived uint64
	PeakTransactions  int
	PeakQueueBytes    int
	ProtocolVersion   uint16
	Capabilities      secureudp.Capabilities
}

func (c StreamConfig) normalized() (StreamConfig, error) {
	if c.ChunkSize == 0 {
		c.ChunkSize = DefaultStreamChunk
	}
	if c.MaxBuffered == 0 {
		c.MaxBuffered = DefaultStreamBuffered
	}
	if c.MaxActive == 0 {
		c.MaxActive = DefaultStreamActive
	}
	if c.DrainTimeout == 0 {
		c.DrainTimeout = 300 * time.Millisecond
	}
	if c.ChunkSize < 1 || c.ChunkSize > 32<<10 || c.MaxBuffered < c.ChunkSize || c.MaxBuffered > reliable.MaxActiveBytes || c.MaxActive < 1 || c.MaxActive > 16 || c.DrainTimeout < 0 || c.DrainTimeout > 5*time.Second {
		return StreamConfig{}, errors.New("invalid stream configuration")
	}
	return c, nil
}

type streamTransaction struct {
	size              int
	responseDelivered bool
}

// ExchangeStream is a bounded asynchronous byte-stream adapter over the SRDT
// message substrate. Application reads and writes are isolated from the network
// owner: a blocked Reader/Writer cannot stop ACK, retransmission, or receive-window
// progress. It currently targets the echo application used by Server; it is not
// yet a generic arbitrary-response/Xray bridge. The finite Reader/Writer form does
// not take ownership of endpoints; callers that need cancellation of a blocking
// endpoint should use ExchangeDuplex.

// ExchangeDuplex adapts one owned full-duplex application endpoint to the SRDT
// stream exchange. The endpoint is closed on return, which makes context
// cancellation able to interrupt a blocked application Read or Write when the
// concrete endpoint implements normal Close semantics (for example net.Conn).
// Application I/O is bounded and asynchronous relative to protocol progress.
func ExchangeDuplex(ctx context.Context, server string, public *ecdh.PublicKey, psk []byte, endpoint io.ReadWriteCloser, config StreamConfig, o Options) (StreamStats, error) {
	if endpoint == nil {
		return StreamStats{}, errors.New("nil duplex endpoint")
	}
	defer endpoint.Close()
	return ExchangeStream(ctx, server, public, psk, endpoint, endpoint, config, o)
}

func ExchangeStream(ctx context.Context, server string, public *ecdh.PublicKey, psk []byte, src io.Reader, dst io.Writer, config StreamConfig, o Options) (StreamStats, error) {
	var stats StreamStats
	if src == nil || dst == nil {
		return stats, errors.New("nil stream endpoint")
	}
	config, err := config.normalized()
	if err != nil {
		return stats, err
	}
	if o.LegacyHandshake {
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
	pumpCtx, pumpCancel := context.WithCancel(ctx)
	defer pumpCancel()
	wake := func() { _ = conn.SetReadDeadline(time.Now()) }
	reader := startStreamReader(pumpCtx, src, config.ChunkSize, wake)
	writer := startStreamWriter(pumpCtx, dst, wake)
	var pendingRead *streamReadEvent
	writerBusy := false
	deadline := time.Now().Add(o.timeout())
	session, err := clientHandshake(ctx, conn, addr, public, psk, deadline, o, secureudp.CapabilityStreaming)
	if err != nil {
		return stats, err
	}
	if !session.HasCapability(secureudp.CapabilityStreaming) {
		return stats, secureudp.ErrIncompatible
	}
	stats.ProtocolVersion = session.ProtocolVersion()
	stats.Capabilities = session.Capabilities()

	queue, err := reliable.NewQueue(min(64<<10, config.MaxBuffered), config.MaxBuffered)
	if err != nil {
		return stats, err
	}
	defer queue.Close()
	o.configureQueue(queue)
	packetMode := session.HasCapability(secureudp.CapabilityPacketTracking)
	windowMode := session.HasCapability(secureudp.CapabilityWindowCredits)
	if packetMode {
		if err := queue.EnablePacketTracking(nil, o.fragment(), packetWireOverhead); err != nil {
			return stats, err
		}
	}
	if windowMode {
		if err := queue.UseWindowCredits(1, 2, config.ChunkSize); err != nil {
			return stats, err
		}
	} else {
		if err := queue.RequireCredits(); err != nil {
			return stats, err
		}
	}

	limits := reliable.LowMemoryReceiverLimits()
	limits.MaxActiveMessages = max(limits.MaxActiveMessages, config.MaxActive*2)
	limits.MaxActiveBytes = max(limits.MaxActiveBytes, config.MaxBuffered)
	receiver, err := reliable.NewReceiverWithLimits(limits)
	if err != nil {
		return stats, err
	}
	defer receiver.Close()
	if err := receiver.RequireCredits(); err != nil {
		return stats, err
	}
	if packetMode && o.BatchPacketAcks && !o.LegacyPacketAcks && session.HasCapability(secureudp.CapabilityPacketAckBatch) {
		_ = receiver.EnablePacketAckBatching()
	}

	replyWindow := clientReplyWindow{capacity: uint32(config.ChunkSize)}
	transactions := make(map[uint64]*streamTransaction, config.MaxActive)
	responses := make(map[uint64]*reliable.Received, config.MaxActive)
	defer func() {
		for _, response := range responses {
			response.Release()
		}
	}()

	buf := make([]byte, secureudp.MaxDatagramSize+1)
	nextRequestID := uint64(1)
	nextResponseID := uint64(2)
	highestQueued := uint64(0)
	eof := false
	var completedAt time.Time

	for {
		now := time.Now()
		if now.After(deadline) {
			return stats, fmt.Errorf("stream timeout: transactions=%d pending=%d", len(transactions), queue.Pending())
		}

		for _, ack := range receiver.FlushPacketAcks(now) {
			if err := o.send(conn, addr, session, ack); err != nil {
				return stats, err
			}
		}

		// Retire local transaction metadata only after the request itself is fully
		// acknowledged and the ordered echo has been delivered to the writer.
		for id, transaction := range transactions {
			if transaction.responseDelivered && !queue.PendingID(id) {
				delete(transactions, id)
			}
		}

		// Retire a completed application write without blocking the network owner.
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
				stats.ReceivedChunks++
				stats.ReceivedBytes += uint64(result.bytes)
				if transaction := transactions[result.requestID]; transaction != nil {
					transaction.responseDelivered = true
				}
			default:
			}
		}

		if ctx.Err() != nil {
			return stats, ctx.Err()
		}

		// Pull at most one borrowed application chunk at a time. The reader pump
		// reuses a single buffer only after this loop signals consumed. Queue.Enqueue
		// performs the sole retained copy into bounded protocol storage.
		if !eof && pendingRead == nil {
			select {
			case event, ok := <-reader.events:
				if !ok {
					eof = true
				} else {
					pendingRead = &event
				}
			default:
			}
		}
		if pendingRead != nil && len(pendingRead.data) == 0 {
			err := pendingRead.err
			pendingRead = nil
			if errors.Is(err, io.EOF) {
				eof = true
			} else if err != nil {
				return stats, err
			}
		}
		if pendingRead != nil && len(transactions) < config.MaxActive && queue.QueuedBytes()+len(pendingRead.data) <= config.MaxBuffered {
			if nextRequestID == 0 || nextRequestID == ^uint64(0) {
				return stats, errors.New("stream message ID exhausted")
			}
			n := len(pendingRead.data)
			if err := queue.Enqueue(nextRequestID, pendingRead.data, 80*time.Millisecond); err != nil {
				return stats, err
			}
			transactions[nextRequestID] = &streamTransaction{size: n}
			highestQueued = nextRequestID
			nextRequestID += 2
			stats.SentChunks++
			stats.SentBytes += uint64(n)
			stats.PeakTransactions = max(stats.PeakTransactions, len(transactions))
			stats.PeakQueueBytes = max(stats.PeakQueueBytes, queue.QueuedBytes())
			terminal := pendingRead.err
			pendingRead = nil
			select {
			case reader.consumed <- struct{}{}:
			default:
			}
			if errors.Is(terminal, io.EOF) {
				eof = true
			} else if terminal != nil {
				return stats, terminal
			}
		}

		if windowMode && highestQueued != 0 {
			_ = queue.RequestWindow(highestQueued)
		}
		if grant, ok := replyWindow.service(receiver, now, false); ok {
			if err := o.send(conn, addr, session, grant); err != nil {
				return stats, err
			}
		}

		for i := 0; i < 16; i++ {
			f, ok := queue.NextSized(now, o.fragment(), o.retryFragment())
			if !ok {
				break
			}
			if err := o.send(conn, addr, session, f); err != nil {
				return stats, err
			}
		}

		// Hand the next ordered response to the writer without blocking protocol
		// progress. Ownership of Received transfers to the writer pump on success.
		if !writerBusy {
			if response := responses[nextResponseID]; response != nil {
				requestID := nextResponseID - 1
				select {
				case writer.jobs <- streamWriteJob{response: response, requestID: requestID}:
					delete(responses, nextResponseID)
					nextResponseID += 2
					writerBusy = true
				default:
				}
			}
		}

		if eof && !writerBusy && len(transactions) == 0 && queue.Pending() == 0 && len(responses) == 0 {
			if completedAt.IsZero() {
				completedAt = time.Now()
			}
			if time.Since(completedAt) >= config.DrainTimeout {
				return stats, nil
			}
		} else {
			completedAt = time.Time{}
		}

		readAt := time.Now().Add(20 * time.Millisecond)
		if at, ok := queue.Wake(time.Now()); ok && at.Before(readAt) {
			readAt = maxTime(at, time.Now().Add(eventLoopMinimumSleep))
		}
		if at, ok := receiver.PacketAckWake(); ok && at.Before(readAt) {
			readAt = maxTime(at, time.Now().Add(eventLoopMinimumSleep))
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
		switch f.Type {
		case reliable.TypeWindowOpen:
			if !windowMode || f.ID < 2 || f.ID%2 != 0 || f.Total != uint32(config.ChunkSize) {
				continue
			}
			requestID := f.ID - 1
			if requestID > highestQueued {
				continue
			}
			replyWindow.wanted = max(replyWindow.wanted, f.ID)
			if grant, ok := replyWindow.service(receiver, time.Now(), true); ok {
				if err := o.send(conn, addr, session, grant); err != nil {
					return stats, err
				}
			}
			continue
		case reliable.TypeWindowGrant:
			if windowMode {
				_, _ = queue.GrantWindow(f)
			}
			continue
		case reliable.TypeAck, reliable.TypePacketAck, reliable.TypePacketAckBatch:
			_ = queue.AckAt(f, time.Now())
			continue
		case reliable.TypeGrant:
			if !windowMode {
				_, _ = queue.Grant(f)
			}
			continue
		case reliable.TypeBlocked:
			if !windowMode {
				_ = queue.Blocked(f, time.Now())
			}
			continue
		case reliable.TypeReject:
			if queue.MatchesOpen(f) || queue.MatchesWindowRequest(f) {
				return stats, fmt.Errorf("stream receiver rejected message %d (reason %d)", f.ID, f.Offset)
			}
			continue
		}

		if f.Type == reliable.TypeOpen {
			if windowMode || f.ID < 2 || f.ID%2 != 0 {
				continue
			}
			requestID := f.ID - 1
			transaction := transactions[requestID]
			if transaction == nil || f.Total != uint32(transaction.size) {
				continue
			}
			response := reliable.Frame{Type: reliable.TypeGrant, ID: f.ID, Total: f.Total}
			if err := receiver.Reserve(f.ID, f.Total); err != nil {
				response.Type, response.Offset = reliable.TypeBlocked, 250
				if errors.Is(err, reliable.ErrFrame) || errors.Is(err, reliable.ErrStale) {
					response.Type, response.Offset = reliable.TypeReject, 2
				}
			}
			if err := o.send(conn, addr, session, response); err != nil {
				return stats, err
			}
			continue
		}
		if f.Type != reliable.TypeData && f.Type != reliable.TypePacketData {
			continue
		}
		if (packetMode && f.Type != reliable.TypePacketData) || (!packetMode && f.Type != reliable.TypeData) || f.ID < 2 || f.ID%2 != 0 {
			continue
		}
		requestID := f.ID - 1
		transaction := transactions[requestID]
		if transaction == nil {
			// Recently completed duplicate responses are still handled if Receiver
			// retains their bounded replay metadata.
			if total, known := receiver.Total(f.ID); !known || total != f.Total {
				continue
			}
		} else if f.Total != uint32(transaction.size) {
			continue
		}
		if !receiver.Known(f.ID) {
			continue
		}
		acks, received, err := receiver.AcceptRetainedAt(f, time.Now())
		if err != nil {
			continue
		}
		for _, ack := range acks {
			if err := o.send(conn, addr, session, ack); err != nil {
				if received != nil {
					received.Release()
				}
				return stats, err
			}
		}
		if received != nil {
			_ = queue.ConfirmDelivered(f.ID-1, time.Now())
			if _, exists := responses[f.ID]; exists {
				received.Release()
			} else {
				responses[f.ID] = received
			}
		}
	}
}

func writeFull(w io.Writer, data []byte) error {
	for len(data) > 0 {
		n, err := w.Write(data)
		if n > 0 {
			data = data[n:]
		}
		if err != nil {
			return err
		}
		if n == 0 {
			return io.ErrShortWrite
		}
	}
	return nil
}
