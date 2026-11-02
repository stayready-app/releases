// Package netreliable provides bounded experimental reliable UDP echo services.
// It includes multi-client and finite single-client harnesses, NOT a public VPN service.
package netreliable

import (
	"context"
	"crypto/ecdh"
	"errors"
	"net"
	"time"

	"example.com/srdt/reliable"
	"example.com/srdt/secureudp"
)

const MaxFramePayload = 900
const eventLoopMinimumSleep = 100 * time.Microsecond

type Options struct {
	LegacyHandshake   bool // Explicit Stage 20 fixed-size HELLO compatibility; no automatic downgrade.
	LegacyPackets     bool // Explicit Stage 18 range-only ACKs; no automatic fallback.
	LegacyPacketAcks  bool // Explicit Stage 19 one-packet receipt compatibility.
	LegacyLiveness    bool // Explicit Stage 23 bridge compatibility without authenticated keepalive.
	BatchPacketAcks   bool // Client-side ACK batching opt-in; server batching is configured separately.
	DisableCongestion bool // Laboratory A/B only. Normal reliable UDP enables DATA control.
	LegacyCredits     bool // Explicit Stage 16 exact-message admission for compatibility tests.
	DropMin           int  // Drop outgoing encrypted UDP datagrams in [DropMin, DropMax].
	DropMax           int
	Fragment          int // Maximum reliable payload per frame; default 900.
	RetryFragment     int // Payload size after first timeout; default 350.
	KeepaliveInterval time.Duration
	KeepaliveTimeout  time.Duration
	Timeout           time.Duration
}

func (o Options) fragment() int {
	if o.Fragment <= 0 || o.Fragment > MaxFramePayload {
		return MaxFramePayload
	}
	return o.Fragment
}
func (o Options) retryFragment() int {
	if o.RetryFragment <= 0 || o.RetryFragment > MaxFramePayload {
		return 350
	}
	return o.RetryFragment
}
func (o Options) timeout() time.Duration {
	if o.Timeout <= 0 {
		return 15 * time.Second
	}
	return o.Timeout
}

func (o Options) keepaliveInterval() time.Duration {
	if o.KeepaliveInterval <= 0 {
		return 15 * time.Second
	}
	return o.KeepaliveInterval
}

func (o Options) keepaliveTimeout() time.Duration {
	if o.KeepaliveTimeout <= 0 {
		return 45 * time.Second
	}
	return o.KeepaliveTimeout
}
func (o Options) send(conn net.PacketConn, to net.Addr, session *secureudp.Session, f reliable.Frame) error {
	padding := 0
	if (session.HasCapability(secureudp.CapabilityControlPadding) && controlFrame(f.Type)) || f.Type == reliable.TypeWindowOpen || f.Type == reliable.TypeWindowGrant {
		padding = session.NextControlPadding()
	}
	encoded, err := encodeFrame(nil, f, session.HasCapability(secureudp.CapabilityControlPadding), padding)
	if err != nil {
		return err
	}
	packet, err := session.Seal(encoded)
	if err != nil {
		return err
	}
	if o.DropMin > 0 && len(packet) >= o.DropMin && len(packet) <= o.DropMax {
		return nil
	}
	if err = conn.SetWriteDeadline(time.Now().Add(100 * time.Millisecond)); err != nil {
		return err
	}
	_, err = conn.WriteTo(packet, to)
	if transientSendError(err) {
		return nil
	} // Not an ACK; queue retains the data.
	return err
}

// Serve accepts one authenticated client and echoes one complete logical message.
// It uses the same bounded server/handshake implementation as ServeMulti.
func Serve(ctx context.Context, listen string, identity *secureudp.Identity, o Options) error {
	return serveFinite(ctx, listen, identity, 1, reliable.MaxMessageSize, o)
}

func serveFinite(ctx context.Context, listen string, identity *secureudp.Identity, count int, maxMessage int, o Options) error {
	if count <= 0 {
		return errors.New("invalid count")
	}
	c := DefaultServerConfig()
	c.Options = o
	c.MaxSessions = 1
	c.MaxSessionsPerIP = 1
	c.CompletionMessages = uint64(count)
	c.MaxMessageBytes = maxMessage
	c.SessionBufferBytes = min(reliable.MaxActiveBytes, max(64<<10, maxMessage*2))
	c.WindowBytes = 64 << 10
	c.ProgressTimeout = o.timeout()
	c.IdleTimeout = o.timeout()
	runCtx, cancel := context.WithTimeout(ctx, o.timeout())
	defer cancel()
	stats, err := ServeMulti(runCtx, listen, identity, c)
	if err != nil {
		return err
	}
	if ctx.Err() != nil {
		return ctx.Err()
	}
	if stats.DeliveredMessages != uint64(count) || runCtx.Err() != nil {
		return errors.New("finite echo session timeout")
	}
	return nil
}

// Exchange sends data reliably and verifies the echoed logical message byte-for-byte.
func Exchange(ctx context.Context, server string, public *ecdh.PublicKey, psk, data []byte, o Options) error {
	return ExchangeBatch(ctx, server, public, psk, [][]byte{data}, o)
}

// UDP write completion is never proof of delivery. A transient failure leaves
// reliable state unacknowledged, allowing a repacked retry under its own timer.
func transientSendError(err error) bool {
	if err == nil {
		return false
	}
	if ne, ok := err.(net.Error); ok && (ne.Timeout() || ne.Temporary()) {
		return true
	}
	return secureudp.IsDatagramReadError(err)
}

// DATA: 25 session header + 16 AEAD tag + 17 reliable header + 8 UDP + 40 IPv6.
// Conservative for IPv4 without options. Does not include L2 or outer tunnels.
const dataWireOverhead = 106

func (o Options) configureQueue(q *reliable.Queue) {
	_ = q.EnableAdaptiveRTO()
	if !o.DisableCongestion {
		_ = q.EnableCongestionControl(MaxFramePayload, dataWireOverhead)
	}
}

// Stage 19 adds an encrypted 8-byte DATA transmission number.
const packetWireOverhead = dataWireOverhead + 8
