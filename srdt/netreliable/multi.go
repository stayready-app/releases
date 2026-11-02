package netreliable

import (
	"bytes"
	"context"
	"crypto/ecdh"
	"errors"
	"fmt"
	"net"
	"time"

	"example.com/srdt/reliable"
	"example.com/srdt/secureudp"
)

// ServeBatch receives a fixed number of logical messages over one authenticated
// UDP session and echoes each with ID+1. Experimental: single peer only.
func ServeBatch(ctx context.Context, listen string, identity *secureudp.Identity, count int, o Options) error {
	return serveFinite(ctx, listen, identity, count, 256<<10, o)
}

// ExchangeBatch sends multiple independent messages concurrently over one UDP
// session. Request IDs are odd; matching echo response IDs are even.
func ExchangeBatch(ctx context.Context, server string, public *ecdh.PublicKey, psk []byte, messages [][]byte, o Options) error {
	for _, m := range messages {
		if len(m) == 0 || len(m) > reliable.MaxMessageSize {
			return reliable.ErrFrame
		}
	}
	if len(messages) == 0 {
		return errors.New("no messages")
	}
	addr, err := net.ResolveUDPAddr("udp", server)
	if err != nil {
		return err
	}
	conn, err := net.ListenPacket("udp", ":0")
	if err != nil {
		return err
	}
	defer conn.Close()
	deadline := time.Now().Add(o.timeout())
	session, err := clientHandshake(ctx, conn, addr, public, psk, deadline, o, 0)
	if err != nil {
		return err
	}
	buf := make([]byte, secureudp.MaxDatagramSize+1)

	// Bound active transactions independently of batch length. Admission now
	// prevents rejected DATA from occupying the flight window. Refill completed
	// slots individually: a slow response must not stall an entire four-item group.
	queueLimit := 64 << 10
	for _, message := range messages {
		if len(message) == 0 || len(message) > reliable.MaxMessageSize {
			return reliable.ErrFrame
		}
		if len(message) > queueLimit {
			queueLimit = len(message)
		}
	}
	queue, err := reliable.NewQueue(64<<10, queueLimit)
	if err != nil {
		return err
	}
	defer queue.Close()
	o.configureQueue(queue)
	packetMode := !o.LegacyPackets && !o.LegacyCredits
	windowMode := !o.LegacyCredits
	if session.ProtocolVersion() == secureudp.ProtocolV1 {
		packetMode = session.HasCapability(secureudp.CapabilityPacketTracking)
		windowMode = session.HasCapability(secureudp.CapabilityWindowCredits)
	}
	if packetMode {
		if err := queue.EnablePacketTracking(nil, o.fragment(), packetWireOverhead); err != nil {
			return err
		}
	}
	capacity := 0
	for _, message := range messages {
		capacity = max(capacity, len(message))
	}
	if !windowMode {
		_ = queue.RequireCredits()
	} else {
		_ = queue.UseWindowCredits(1, 2, capacity)
	}
	nextQueued := 0
	active := make([]int, 0, 4)
	activeBytes := 0
	limits := reliable.LowMemoryReceiverLimits()
	limits.MaxActiveBytes = max(limits.MaxActiveBytes, queueLimit)
	receiver, err := reliable.NewReceiverWithLimits(limits)
	if err != nil {
		return err
	}
	defer receiver.Close()
	_ = receiver.RequireCredits()
	// Keep client receipts immediate by default. The shared server scheduler
	// benefits from those ACK arrivals as natural wakeups; batching them can
	// otherwise turn sub-millisecond pacing into timer-driven stalls. Server-side
	// receive ACKs remain batched by default.
	if packetMode && o.BatchPacketAcks && !o.LegacyPacketAcks && (session.ProtocolVersion() != secureudp.ProtocolV1 || session.HasCapability(secureudp.CapabilityPacketAckBatch)) {
		_ = receiver.EnablePacketAckBatching()
	}
	replyWindow := clientReplyWindow{capacity: uint32(capacity)}
	wanted := 0
	got := make(map[uint64]bool)
	var completedAt time.Time
	for {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("batch timeout: received %d/%d, pending %d", len(got), len(messages), queue.Pending())
		}
		for _, ack := range receiver.FlushPacketAcks(time.Now()) {
			if err := o.send(conn, addr, session, ack); err != nil {
				return err
			}
		}
		if len(got) == len(messages) && queue.Pending() == 0 {
			if completedAt.IsZero() {
				completedAt = time.Now()
			}
			if time.Since(completedAt) >= 300*time.Millisecond {
				return nil
			}
		}
		kept := active[:0]
		for _, index := range active {
			if got[uint64(index*2+2)] && !queue.PendingID(uint64(index*2+1)) {
				activeBytes -= len(messages[index])
			} else {
				kept = append(kept, index)
			}
		}
		active = kept
		for len(active) < 4 && nextQueued < len(messages) {
			if len(active) > 0 && nextQueued >= active[0]+128 {
				break
			}
			message := messages[nextQueued]
			if len(active) > 0 && activeBytes+len(message) > 64<<10 {
				break
			}
			if err := queue.Enqueue(uint64(nextQueued*2+1), message, 80*time.Millisecond); err != nil {
				return err
			}
			activeBytes += len(message)
			active = append(active, nextQueued)
			nextQueued++
		}
		if windowMode {
			// Keep receiver admission ahead of the four payload transactions.
			// Only the upper ID is stored: there is no per-future-message payload.
			wanted = min(len(messages), nextQueued+4)
			if len(active) > 0 {
				wanted = min(wanted, active[0]+128)
			}
			_ = queue.RequestWindow(uint64(wanted*2 - 1))
			if grant, ok := replyWindow.service(receiver, time.Now(), false); ok {
				if err := o.send(conn, addr, session, grant); err != nil {
					return err
				}
			}
		}
		for i := 0; i < 16; i++ {
			f, ok := queue.NextSized(time.Now(), o.fragment(), o.retryFragment())
			if !ok {
				break
			}
			if err := o.send(conn, addr, session, f); err != nil {
				return err
			}
		}
		readAt := time.Now().Add(20 * time.Millisecond)
		if at, ok := queue.Wake(time.Now()); ok && at.Before(readAt) {
			readAt = maxTime(at, time.Now().Add(eventLoopMinimumSleep))
		}
		if at, ok := receiver.PacketAckWake(); ok && at.Before(readAt) {
			readAt = maxTime(at, time.Now().Add(eventLoopMinimumSleep))
		}
		_ = conn.SetReadDeadline(readAt)
		n, from, e := conn.ReadFrom(buf)
		if e != nil {
			if secureudp.IsDatagramReadError(e) {
				continue
			}
			if ne, ok := e.(net.Error); ok && ne.Timeout() {
				continue
			}
			return e
		}
		if from.String() != addr.String() {
			continue
		}
		plain, e := session.Open(buf[:n])
		if e != nil {
			continue
		}
		f, e := reliable.Decode(plain)
		if e != nil {
			continue
		}
		switch f.Type {
		case reliable.TypeWindowOpen:
			if !windowMode || f.ID%2 != 0 || f.ID < 2 || f.ID/2 > uint64(wanted) || f.Total != uint32(capacity) {
				continue
			}
			replyWindow.wanted = max(replyWindow.wanted, f.ID)
			if grant, ok := replyWindow.service(receiver, time.Now(), true); ok {
				if err := o.send(conn, addr, session, grant); err != nil {
					return err
				}
			}
			continue
		case reliable.TypeWindowGrant:
			_, _ = queue.GrantWindow(f)
			continue
		case reliable.TypeAck, reliable.TypePacketAck, reliable.TypePacketAckBatch:
			_ = queue.AckAt(f, time.Now())
			continue
		case reliable.TypeGrant:
			_, _ = queue.Grant(f)
			continue
		case reliable.TypeBlocked:
			_ = queue.Blocked(f, time.Now())
			continue
		case reliable.TypeReject:
			if queue.MatchesOpen(f) || queue.MatchesWindowRequest(f) {
				return fmt.Errorf("receiver rejected message %d (reason %d)", f.ID, f.Offset)
			}
			continue
		}
		if (f.Type != reliable.TypeData && f.Type != reliable.TypePacketData && f.Type != reliable.TypeOpen) || f.ID < 2 || f.ID%2 != 0 {
			continue
		}
		index64 := f.ID/2 - 1
		if index64 >= uint64(nextQueued) || f.Total != uint32(len(messages[index64])) {
			continue
		}
		if f.Type == reliable.TypeOpen {
			if windowMode {
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
				return err
			}
			continue
		}
		if (packetMode && f.Type != reliable.TypePacketData) || (!packetMode && f.Type != reliable.TypeData) {
			continue
		}
		if !receiver.Known(f.ID) {
			continue
		} // DATA cannot implicitly grant credit.
		acks, data, e := receiver.AcceptAt(f, time.Now())
		if e != nil {
			continue
		}
		for _, ack := range acks {
			if err := o.send(conn, addr, session, ack); err != nil {
				return err
			}
		}
		if data != nil {
			index := int(f.ID/2 - 1)
			if index >= len(messages) || !bytes.Equal(data, messages[index]) {
				return fmt.Errorf("echo mismatch: id %d", f.ID)
			}
			_ = queue.ConfirmDelivered(f.ID-1, time.Now())
			got[f.ID] = true
		}

	}
}

func maxTime(a, b time.Time) time.Time {
	if a.After(b) {
		return a
	}
	return b
}
