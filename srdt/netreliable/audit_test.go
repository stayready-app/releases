package netreliable

import (
	"context"
	"example.com/srdt/reliable"
	"example.com/srdt/secureudp"
	"net"
	"net/netip"
	"testing"
	"time"
)

func TestAuditInvalidACKCannotConfirmOrRefresh(t *testing.T) {
	s, i, _ := testMulti(t)
	now := time.Now()
	_, c, p := admit(t, s, i, 1234, now)
	for n := 0; n < 10; n++ {
		sendClient(t, s, c, p, reliable.Frame{Type: reliable.TypeAck, ID: 2, Total: 100, Payload: []byte{0, 0, 0, 1}}, now.Add(4*time.Second))
	}
	if p.confirmed || !p.lastValid.Equal(now) {
		t.Fatal("unknown ACK confirmed or extended session")
	}
	s.service(now.Add(6*time.Second), discardPacket)
	if len(s.sessions) != 0 {
		t.Fatal("invalid ACK pins session")
	}
}
func TestAuditExpiredTimersStillPermitInput(t *testing.T) {
	s, i, _ := testMulti(t)
	now := time.Now()
	_, _, _ = admit(t, s, i, 1234, now)
	d := s.readDeadline(now.Add(time.Hour))
	if !d.After(now.Add(time.Hour)) {
		t.Fatal("overdue work starves socket reads")
	}
}
func TestAuditServiceHasGlobalWorkBound(t *testing.T) {
	s, i, _ := testMulti(t)
	now := time.Now()
	for n := 0; n < 50; n++ {
		_, c, p := admit(t, s, i, uint16(n+1000), now)
		sendClient(t, s, c, p, reliable.Frame{Type: reliable.TypeData, ID: 1, Total: 900, Payload: make([]byte, 900)}, now)
	}
	sent := 0
	s.service(now, func([]byte, netip.AddrPort) error { sent++; return nil })
	if sent > 32 {
		t.Fatalf("unbounded send burst %d", sent)
	}
	for _, p := range s.sessions {
		s.remove(p, false)
	}
	if s.budget.Used() != 0 {
		t.Fatal("leaked")
	}
}
func TestAuditIdentityReuseCannotReopenTranscript(t *testing.T) {
	s, i, c := testMulti(t)
	now := time.Now()
	h, _, p := admit(t, s, i, 1234, now)
	s.remove(p, false)
	other, _ := NewServer(i, c)
	other.handle(h, netip.MustParseAddrPort("127.0.0.1:1234"), now, discardPacket)
	if len(other.sessions) != 0 {
		t.Fatal("new server reset nonces under same identity")
	}
}
func TestAuditClientHandshakeInjectionDoesNotAmplify(t *testing.T) {
	c, e := net.ListenPacket("udp", "127.0.0.1:0")
	if e != nil {
		t.Fatal(e)
	}
	defer c.Close()
	peer, e := net.ListenPacket("udp", "127.0.0.1:0")
	if e != nil {
		t.Fatal(e)
	}
	defer peer.Close()
	identity, _ := secureudp.NewIdentity(make([]byte, 32))
	ctx, cancel := context.WithTimeout(context.Background(), 400*time.Millisecond)
	defer cancel()
	done := make(chan int, 1)
	go func() {
		var target net.Addr
		count := 0
		buf := make([]byte, 1201)
		for ctx.Err() == nil {
			_ = peer.SetReadDeadline(time.Now().Add(time.Millisecond))
			n, from, err := peer.ReadFrom(buf)
			if err == nil && secureudp.IsHello(buf[:n]) {
				count++
				target = from
			}
			if target != nil {
				_, _ = peer.WriteTo([]byte{1, 2, 3}, target)
			}
		}
		done <- count
	}()
	_, err := clientHandshake(ctx, c, peer.LocalAddr(), identity.Public, identity.PSK, time.Now().Add(time.Second), Options{}, 0)
	if err == nil {
		t.Fatal("fake welcome accepted")
	}
	count := <-done
	if count < 1 || count > 4 {
		t.Fatalf("injected packets triggered %d HELLO transmissions", count)
	}
}
func TestAuditOversizePacketDoesNotStopServer(t *testing.T) {
	_, i, c := testMulti(t)
	s, addr, stop := startMulti(t, c, i)
	defer stop()
	conn, err := net.Dial("udp", addr)
	if err != nil {
		t.Fatal(err)
	}
	_, _ = conn.Write(make([]byte, 60000))
	_ = conn.Close()
	if err := ExchangeBatch(context.Background(), addr, i.Public, i.PSK, [][]byte{[]byte("after oversize")}, Options{Timeout: 3 * time.Second}); err != nil {
		t.Fatal(err)
	}
	if s.Stats().InvalidPackets == 0 {
		t.Fatal("oversize never exercised")
	}
}

func TestAuditLegacyLargeMessagePreserved(t *testing.T) {
	_, identity, c := testMulti(t)
	c.MaxMessageBytes = 512 << 10
	c.SessionBufferBytes = 1 << 20
	_, addr, stop := startMulti(t, c, identity)
	defer stop()
	// Exchange historically accepted more than LowMemoryReceiverLimits (256KiB).
	// Unifying its implementation must not silently lower that public API limit.
	data := make([]byte, 300<<10)
	for i := range data {
		data[i] = byte(i)
	}
	if err := Exchange(context.Background(), addr, identity.Public, identity.PSK, data, Options{Timeout: 10 * time.Second}); err != nil {
		t.Fatal(err)
	}
}

func TestAuditGlobalBudgetUnderIncompleteFlood(t *testing.T) {
	_, identity, c := testMulti(t)
	c.MaxSessionsPerIP = 1024
	c.BufferBudget = 4 * reliable.BufferCharge(c.MaxMessageBytes)
	s, err := NewServer(identity, c)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now()
	for n := 0; n < 1024; n++ {
		_, client, p := admit(t, s, identity, uint16(n+1000), now)
		sendClient(t, s, client, p, reliable.Frame{Type: reliable.TypeData, ID: 1, Total: uint32(c.MaxMessageBytes), Payload: []byte{7}}, now)
		if s.budget.Used() > c.BufferBudget {
			t.Fatal("global cap exceeded")
		}
	}
	if s.stats.ResourceDrops != 1020 {
		t.Fatalf("admission accounting: %d", s.stats.ResourceDrops)
	}
	for n := 0; n < 1024 && len(s.sessions) > 0; n++ {
		s.service(now.Add(time.Minute), discardPacket)
	}
	if len(s.sessions) != 0 || len(s.events) != 0 || s.budget.Used() != 0 {
		t.Fatal("incomplete-message flood leaked resources")
	}
}
