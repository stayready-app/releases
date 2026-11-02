package secureudp

import (
	"context"
	"net"
	"testing"
	"time"
)

func TestAuditRepeatedAcceptCannotRecreateKeys(t *testing.T) {
	i, _ := NewIdentity(make([]byte, 32))
	h, _, _ := ClientHello(i.Public, i.PSK)
	_, _, e := AcceptHello(i, h)
	if e != nil {
		t.Fatal(e)
	}
	if _, _, e = AcceptHello(i, h); e == nil {
		t.Fatal("same transcript creates a new session with reset GCM nonce")
	}
}
func TestAuditClientInputsValidated(t *testing.T) {
	if _, _, err := ClientHello(nil, make([]byte, 32)); err == nil {
		t.Fatal("nil pinned key accepted")
	}
	i, _ := NewIdentity(make([]byte, 32))
	if _, _, err := ClientHello(i.Public, []byte{1}); err == nil {
		t.Fatal("invalid PSK accepted")
	}
	if _, _, err := AcceptHello(nil, make([]byte, 81)); err == nil {
		t.Fatal("nil identity accepted")
	}
}
func TestAuditOpenRejectsOversize(t *testing.T) {
	i, _ := NewIdentity(make([]byte, 32))
	h, c, _ := ClientHello(i.Public, i.PSK)
	_, s, _ := AcceptHello(i, h)
	// Build authenticated oversized input bypassing the public Seal limit.
	p, _ := c.Seal([]byte{1})
	p = c.tx.Seal(p[:headerSize], nonce(c.txCounter), make([]byte, MaxDatagramSize), p[:headerSize])
	if _, err := s.Open(p); err == nil {
		t.Fatal("oversized authenticated packet accepted")
	}
}

func FuzzAuditOpen(f *testing.F) {
	key := make([]byte, 32)
	var id [16]byte
	sender, _ := newSession(id, key, key)
	p, _ := sender.Seal([]byte("authenticated seed"))
	f.Add(p)
	f.Add([]byte{3})
	f.Add(make([]byte, 1300))
	f.Fuzz(func(t *testing.T, p []byte) {
		session, _ := newSession(id, key, key)
		_, err := session.Open(p)
		if err != nil && (session.rxHighest != 0 || session.rxBitmap != 0) {
			t.Fatal("unauthenticated input changed replay state")
		}
	})
}

func TestAuditRawEchoIgnoresUnauthenticatedNoise(t *testing.T) {
	identity, _ := NewIdentity(make([]byte, 32))
	conn, err := net.ListenPacket("udp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	done := make(chan error, 1)
	go func() {
		_ = conn.SetDeadline(time.Now().Add(2 * time.Second))
		buf := make([]byte, 1201)
		n, peer, err := conn.ReadFrom(buf)
		if err != nil {
			done <- err
			return
		}
		welcome, s, err := AcceptHello(identity, buf[:n])
		if err != nil {
			done <- err
			return
		}
		_, _ = conn.WriteTo([]byte{3, 1, 1}, peer)
		_, _ = conn.WriteTo(welcome, peer)
		n, _, err = conn.ReadFrom(buf)
		if err != nil {
			done <- err
			return
		}
		plain, err := s.Open(buf[:n])
		if err != nil {
			done <- err
			return
		}
		packet, err := s.Seal(plain)
		if err != nil {
			done <- err
			return
		}
		fake := append([]byte(nil), packet...)
		fake[len(fake)-1] ^= 1
		_, _ = conn.WriteTo(fake, peer)
		_, err = conn.WriteTo(packet, peer)
		done <- err
	}()
	if err := RunEchoContext(context.Background(), conn.LocalAddr().String(), identity.Public, identity.PSK, []byte("echo"), 2*time.Second); err != nil {
		t.Fatal(err)
	}
	if err := <-done; err != nil {
		t.Fatal(err)
	}
}
