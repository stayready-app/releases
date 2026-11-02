package secureudp

import (
	"bytes"
	"context"
	"crypto/ecdh"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"net"
	"time"
)

func ParsePSK(value string) ([]byte, error) {
	key, err := hex.DecodeString(value)
	if err != nil || len(key) != 32 {
		return nil, errors.New("PSK must be 64 hex characters")
	}
	return key, nil
}

func ParsePublic(value string) (*ecdh.PublicKey, error) {
	raw, err := hex.DecodeString(value)
	if err != nil {
		return nil, err
	}
	return ecdh.X25519().NewPublicKey(raw)
}

// ServeEcho is a single-client integration harness, not a production server.
func ServeEcho(ctx context.Context, addr string, identity *Identity) error {
	conn, err := net.ListenPacket("udp", addr)
	if err != nil {
		return err
	}
	defer conn.Close()
	var session *Session
	var remote net.Addr
	var cachedHello [32]byte
	var welcome []byte
	var lastWelcome time.Time
	buf := make([]byte, MaxDatagramSize+1)
	for {
		if ctx.Err() != nil {
			return nil
		}
		if err := conn.SetReadDeadline(time.Now().Add(100 * time.Millisecond)); err != nil {
			return err
		}
		n, from, err := conn.ReadFrom(buf)
		if ne, ok := err.(net.Error); ok && ne.Timeout() {
			if ctx.Err() != nil {
				return nil
			}
			continue
		}
		if IsDatagramReadError(err) {
			continue
		}
		if err != nil {
			return err
		}
		if n == 81 && buf[0] == helloType {
			if session != nil {
				if from.String() == remote.String() && sha256.Sum256(buf[:n]) == cachedHello && time.Since(lastWelcome) >= 100*time.Millisecond {
					_ = conn.SetWriteDeadline(time.Now().Add(100 * time.Millisecond))
					_, _ = conn.WriteTo(welcome, from)
					lastWelcome = time.Now()
				}
				continue
			}
			w, s, err := AcceptHello(identity, buf[:n])
			if err != nil {
				continue
			}
			session, remote = s, from
			cachedHello = sha256.Sum256(buf[:n])
			welcome = w
			lastWelcome = time.Now()
			_ = conn.SetWriteDeadline(time.Now().Add(100 * time.Millisecond))
			_, _ = conn.WriteTo(welcome, from)
			continue
		}
		if session == nil || from.String() != remote.String() {
			continue
		}
		plain, err := session.Open(buf[:n])
		if err != nil {
			continue
		}
		packet, err := session.Seal(plain)
		if err != nil {
			continue
		}
		_ = conn.SetWriteDeadline(time.Now().Add(100 * time.Millisecond))
		if _, err := conn.WriteTo(packet, from); err != nil {
			return err
		}
	}
}

func RunEcho(addr string, public *ecdh.PublicKey, psk []byte, payload []byte) error {
	return RunEchoContext(context.Background(), addr, public, psk, payload, 5*time.Second)
}

func RunEchoContext(ctx context.Context, addr string, public *ecdh.PublicKey, psk []byte, payload []byte, timeout time.Duration) error {
	if timeout <= 0 {
		return errors.New("invalid echo timeout")
	}
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	conn, err := (&net.Dialer{}).DialContext(ctx, "udp", addr)
	if err != nil {
		return err
	}
	defer conn.Close()
	deadline, _ := ctx.Deadline()
	if err = conn.SetDeadline(deadline); err != nil {
		return err
	}
	stop := context.AfterFunc(ctx, func() { _ = conn.SetDeadline(time.Now()) })
	defer stop()
	hello, session, err := ClientHello(public, psk)
	if err != nil {
		return err
	}
	if _, err := conn.Write(hello); err != nil {
		return err
	}
	buf := make([]byte, MaxDatagramSize+1)
	for {
		n, e := conn.Read(buf)
		if IsDatagramReadError(e) {
			continue
		}
		if e != nil {
			return e
		}
		if VerifyWelcome(buf[:n], session) == nil {
			break
		}
	}

	packet, err := session.Seal(payload)
	if err != nil {
		return err
	}
	if _, err := conn.Write(packet); err != nil {
		return err
	}
	for {
		n, e := conn.Read(buf)
		if IsDatagramReadError(e) {
			continue
		}
		if e != nil {
			return e
		}
		received, e := session.Open(buf[:n])
		if e != nil {
			continue
		}
		if !bytes.Equal(received, payload) {
			return fmt.Errorf("echo data mismatch")
		}
		break
	}

	return nil
}
