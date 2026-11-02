package netreliable

import (
	"context"
	"crypto/ecdh"
	"errors"
	"example.com/srdt/secureudp"
	"net"
	"time"
)

// Invalid incoming traffic must not trigger extra HELLO transmissions. The
// retransmission clock is independent of ReadFrom returning early on injection.
func clientHandshake(ctx context.Context, conn net.PacketConn, addr net.Addr, public *ecdh.PublicKey, psk []byte, deadline time.Time, o Options, extraRequired secureudp.Capabilities) (*secureudp.Session, error) {
	var hello []byte
	var session *secureudp.Session
	var err error
	if o.LegacyHandshake {
		hello, session, err = secureudp.ClientHello(public, psk)
	} else {
		hello, session, err = secureudp.ClientHelloV1(public, psk, offeredCapabilities(o), requiredCapabilities(o, extraRequired))
	}
	if err != nil {
		return nil, err
	}
	if d, ok := ctx.Deadline(); ok && d.Before(deadline) {
		deadline = d
	}
	var nextSend time.Time
	buf := make([]byte, secureudp.MaxDatagramSize+1)
	for {
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		now := time.Now()
		if !now.Before(deadline) {
			return nil, errors.New("handshake timeout")
		}
		if !now.Before(nextSend) {
			if !o.LegacyHandshake && !nextSend.IsZero() {
				if repacked, e := secureudp.RepadHelloV1(hello); e == nil {
					hello = repacked
				}
			}
			if err = conn.SetWriteDeadline(minTime(deadline, now.Add(100*time.Millisecond))); err != nil {
				return nil, err
			}
			if _, err = conn.WriteTo(hello, addr); err != nil && !transientSendError(err) {
				return nil, err
			}
			nextSend = now.Add(150 * time.Millisecond)
		}
		readUntil := minTime(deadline, minTime(nextSend, now.Add(100*time.Millisecond)))
		if err = conn.SetReadDeadline(readUntil); err != nil {
			return nil, err
		}
		n, from, e := conn.ReadFrom(buf)
		if e == nil {
			if from.String() == addr.String() && secureudp.VerifyWelcome(buf[:n], session) == nil {
				return session, nil
			}
			continue
		}
		if secureudp.IsDatagramReadError(e) {
			continue
		}
		if ne, ok := e.(net.Error); !ok || !ne.Timeout() {
			return nil, e
		}
	}
}

func minTime(a, b time.Time) time.Time {
	if a.Before(b) {
		return a
	}
	return b
}
