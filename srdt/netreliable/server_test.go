package netreliable

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/json"
	"net"
	"net/netip"
	"runtime"
	"sync"
	"testing"
	"time"

	"example.com/srdt/reliable"
	"example.com/srdt/secureudp"
)

func testMulti(t *testing.T) (*Server, *secureudp.Identity, ServerConfig) {
	t.Helper()
	psk := make([]byte, 32)
	_, _ = rand.Read(psk)
	identity, err := secureudp.NewIdentity(psk)
	if err != nil {
		t.Fatal(err)
	}
	c := DefaultServerConfig()
	// Existing tests deliberately exercise DATA-first legacy admission. Modern
	// strict-credit behavior has separate tests and is the production default.
	c.AllowLegacyData = true
	c.AllowLegacyHandshake = true
	c.HelloBurst = 10000
	c.HelloRate = 10000
	c.MaxSessionsPerIP = c.MaxSessions
	s, err := NewServer(identity, c)
	if err != nil {
		t.Fatal(err)
	}
	return s, identity, c
}

func discardPacket([]byte, netip.AddrPort) error { return nil }

func admit(t *testing.T, s *Server, i *secureudp.Identity, port uint16, now time.Time) ([]byte, *secureudp.Session, *peerSession) {
	t.Helper()
	h, c, e := secureudp.ClientHello(i.Public, i.PSK)
	if e != nil {
		t.Fatal(e)
	}
	a := netip.AddrPortFrom(netip.MustParseAddr("127.0.0.1"), port)
	s.handle(h, a, now, func(b []byte, _ netip.AddrPort) error { return secureudp.VerifyWelcome(b, c) })
	id, _ := secureudp.SessionID(h)
	p := s.sessions[id]
	if p == nil {
		t.Fatal("session not admitted")
	}
	return h, c, p
}

func sendClient(t *testing.T, s *Server, c *secureudp.Session, p *peerSession, f reliable.Frame, now time.Time) {
	t.Helper()
	encoded, e := reliable.Encode(f)
	if e != nil {
		t.Fatal(e)
	}
	packet, e := c.Seal(encoded)
	if e != nil {
		t.Fatal(e)
	}
	s.handle(packet, p.peer, now, discardPacket)
}

func TestMultiHelloIdempotentAndRetirement(t *testing.T) {
	s, i, _ := testMulti(t)
	now := time.Now()
	h, c, p := admit(t, s, i, 1234, now)
	original := p.crypto
	before := len(s.events)
	s.handle(h, p.peer, now.Add(100*time.Millisecond), func(b []byte, _ netip.AddrPort) error {
		if !bytes.Equal(b, p.welcome) {
			t.Error("welcome encrypted again")
		}
		return nil
	})
	if p.crypto != original || len(s.events) != before || !p.lastValid.Equal(now) {
		t.Fatal("replayed hello replaced or extended session")
	}
	encoded, _ := reliable.Encode(reliable.Frame{Type: reliable.TypeData, ID: 1, Total: 1, Payload: []byte{3}})
	packet, _ := c.Seal(encoded)
	s.handle(packet, p.peer, now, discardPacket)
	s.handle(packet, p.peer, now.Add(time.Second), discardPacket)
	if s.stats.DeliveredMessages != 1 {
		t.Fatal("replay delivered")
	}
	s.remove(p, false)
	if s.budget.Used() != 0 {
		t.Fatal("cleanup leaked queue credit")
	}
	s.handle(h, p.peer, now.Add(time.Second), discardPacket)
	if len(s.sessions) != 0 || s.stats.AdmissionRejects != 1 {
		t.Fatal("retired handshake replayed")
	}
}

func TestMultiLimitsAndExpiredCredit(t *testing.T) {
	s, i, c := testMulti(t)
	c.MaxSessions = 2
	c.MaxSessionsPerIP = 1
	c.MaxAdmissions = 2
	s, _ = NewServer(i, c)
	now := time.Now()
	_, client, p := admit(t, s, i, 1234, now)
	second, _, _ := secureudp.ClientHello(i.Public, i.PSK)
	s.handle(second, netip.MustParseAddrPort("127.0.0.1:1235"), now, discardPacket)
	if len(s.sessions) != 1 {
		t.Fatal("per-IP cap ignored")
	}
	s.handle(second, netip.MustParseAddrPort("127.0.0.2:1235"), now, discardPacket)
	if len(s.sessions) != 2 {
		t.Fatal("separate IP rejected")
	}
	third, _, _ := secureudp.ClientHello(i.Public, i.PSK)
	s.handle(third, netip.MustParseAddrPort("127.0.0.3:1235"), now, discardPacket)
	if len(s.sessions) != 2 {
		t.Fatal("session cap ignored")
	}
	sendClient(t, s, client, p, reliable.Frame{Type: reliable.TypeData, ID: 1, Total: 1000, Payload: []byte{7}}, now)
	if s.budget.Used() != reliable.BufferCharge(1000) {
		t.Fatal("receive budget missing")
	}
	s.service(now.Add(time.Minute), discardPacket)
	if len(s.sessions) != 0 || s.budget.Used() != 0 || len(s.events) != 0 || len(s.perIP) != 0 {
		t.Fatal("expiration leak")
	}
	s.handle(third, netip.MustParseAddrPort("127.0.0.3:1235"), now.Add(time.Minute), discardPacket)
	if len(s.sessions) != 0 {
		t.Fatal("full hello registry evicted tombstones")
	}
}

func TestMultiGlobalBudgetAndSlowPeer(t *testing.T) {
	_, i, c := testMulti(t)
	c.BufferBudget = reliable.BufferCharge(1000)
	c.MaxMessageBytes = 1000
	s, _ := NewServer(i, c)
	now := time.Now()
	_, a, pa := admit(t, s, i, 1001, now)
	_, b, pb := admit(t, s, i, 1002, now)
	f := reliable.Frame{Type: reliable.TypeData, ID: 1, Total: 1000, Payload: []byte{3}}
	sendClient(t, s, a, pa, f, now)
	sendClient(t, s, b, pb, f, now)
	if s.stats.ResourceDrops != 1 || pb.receiver.ActiveBytes() != 0 {
		t.Fatal("global budget not shared")
	}
	// Completing A retains the same credit in its outgoing response queue.
	f.Offset = 1
	f.Payload = bytes.Repeat([]byte{3}, 999)
	sendClient(t, s, a, pa, f, now)
	if pa.queue.Pending() != 1 || s.budget.Used() != c.BufferBudget {
		t.Fatal("unaccounted completed response")
	}
	// Spoofing a larger packet number from another source cannot mutate crypto.
	x, _ := a.Seal([]byte("invalid"))
	s.handle(x, pb.peer, now.Add(time.Second), discardPacket)
	if !pa.lastValid.Equal(now) {
		t.Fatal("wrong peer extended session")
	}
	s.remove(pa, false)
	f.Offset = 0
	f.Payload = []byte{3}
	sendClient(t, s, b, pb, f, now.Add(time.Second))
	if pb.receiver.ActiveBytes() != 1000 {
		t.Fatal("credit not reusable")
	}
	s.remove(pb, false)
	if s.budget.Used() != 0 {
		t.Fatal("leaked")
	}
}

func TestMultiStaleIDsCannotRedeliverAfterCacheEviction(t *testing.T) {
	s, i, _ := testMulti(t)
	now := time.Now()
	_, client, p := admit(t, s, i, 1000, now)
	for id := uint64(1); id <= 401; id += 2 {
		now = now.Add(2 * time.Millisecond) // Virtual time must permit paced tiny echoes.
		sendClient(t, s, client, p, reliable.Frame{Type: reliable.TypeData, ID: id, Total: 1, Payload: []byte{3}}, now)
		frame, ok := p.queue.Next(now, 1)
		if !ok {
			t.Fatal("missing echo")
		}
		ack := reliable.Frame{Type: reliable.TypeAck, ID: frame.ID, Total: 1, Payload: []byte{0, 0, 0, 1}}
		sendClient(t, s, client, p, ack, now)
	}
	before := s.stats.DeliveredMessages
	sendClient(t, s, client, p, reliable.Frame{Type: reliable.TypeData, ID: 1, Total: 1, Payload: []byte{3}}, now)
	if s.stats.DeliveredMessages != before || s.stats.StaleMessages != 1 || s.budget.Used() != 0 {
		t.Fatal("retired ID delivered again")
	}
}

func TestMultiInjectionDoesNotKeepSessionAlive(t *testing.T) {
	s, i, _ := testMulti(t)
	now := time.Now()
	_, client, p := admit(t, s, i, 1111, now)
	packet, _ := client.Seal([]byte("test"))
	packet[len(packet)-1] ^= 1
	for n := 0; n < 10; n++ {
		s.handle(packet, p.peer, now.Add(4*time.Second), discardPacket)
	}
	s.service(now.Add(6*time.Second), discardPacket)
	if len(s.sessions) != 0 {
		t.Fatal("injection kept pending handshake alive")
	}
}

func TestMultiHelloRateBeforeStateAllocation(t *testing.T) {
	_, i, c := testMulti(t)
	c.HelloRate = 1
	c.HelloBurst = 1
	s, _ := NewServer(i, c)
	now := time.Now()
	bad := make([]byte, 81)
	bad[0] = 1
	for n := 0; n < 100; n++ {
		s.handle(bad, netip.MustParseAddrPort("127.0.0.1:1"), now, discardPacket)
	}
	if len(s.sessions) != 0 || len(s.used) != 0 || s.stats.RateLimited != 99 {
		t.Fatal("hello admission not rate bounded")
	}
}

func TestMultiThousandIdleState(t *testing.T) {
	s, i, c := testMulti(t)
	c.MaxSessions = 1000
	c.MaxSessionsPerIP = 1000
	s, _ = NewServer(i, c)
	now := time.Now()
	runtime.GC()
	var before, after runtime.MemStats
	runtime.ReadMemStats(&before)
	goroutines := runtime.NumGoroutine()
	for n := 0; n < 1000; n++ {
		admit(t, s, i, uint16(1000+n), now)
	}
	runtime.GC()
	runtime.ReadMemStats(&after)
	if len(s.sessions) != 1000 || len(s.events) != 1000 || s.budget.Used() != 0 {
		t.Fatal("idle state not lazy")
	}
	if runtime.NumGoroutine() != goroutines {
		t.Fatal("goroutine per session")
	}
	delta := int64(after.HeapAlloc) - int64(before.HeapAlloc)
	t.Logf("1000 idle manager sessions: retained heap delta=%d, bytes/session=%.1f, goroutine delta=0, payload credit=0", delta, float64(delta)/1000)
	// Generous regression threshold, NOT a production RAM guarantee.
	if delta > 16<<20 {
		t.Fatalf("excessive idle state: %d", delta)
	}
	for _, p := range s.sessions {
		s.remove(p, false)
	}
	if len(s.events) != 0 || s.budget.Used() != 0 {
		t.Fatal("mass cleanup leak")
	}
	runtime.KeepAlive(s)
}

func startMulti(t *testing.T, c ServerConfig, i *secureudp.Identity) (*Server, string, func()) {
	t.Helper()
	s, e := NewServer(i, c)
	if e != nil {
		t.Fatal(e)
	}
	conn, e := net.ListenUDP("udp", &net.UDPAddr{IP: net.ParseIP("127.0.0.1")})
	if e != nil {
		t.Fatal(e)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- s.Run(ctx, conn) }()
	stop := func() {
		cancel()
		select {
		case e := <-done:
			if e != nil {
				t.Error(e)
			}
		case <-time.After(2 * time.Second):
			t.Error("server did not stop")
		}
	}
	return s, conn.LocalAddr().String(), stop
}

func TestMultiRealUDPConcurrentClients(t *testing.T) {
	for _, filtered := range []bool{false, true} {
		t.Run(map[bool]string{false: "baseline", true: "size-filter"}[filtered], func(t *testing.T) {
			_, i, c := testMulti(t)
			c.IdleTimeout = 20 * time.Second
			c.ProgressTimeout = 20 * time.Second
			o := Options{Timeout: 20 * time.Second}
			if filtered {
				o.DropMin = 800
				o.DropMax = 1100
			}
			c.Options = o
			s, addr, stop := startMulti(t, c, i)
			defer stop()
			const clients = 32
			const count = 12
			const size = 4096
			var wg sync.WaitGroup
			errs := make(chan error, clients)
			for n := 0; n < clients; n++ {
				wg.Add(1)
				go func(n int) {
					defer wg.Done()
					messages := make([][]byte, count)
					for j := range messages {
						messages[j] = bytes.Repeat([]byte{byte(n), byte(j), 0x5a, 0xff}, size/4)
					}
					errs <- ExchangeBatch(context.Background(), addr, i.Public, i.PSK, messages, o)
				}(n)
			}
			wg.Wait()
			close(errs)
			for e := range errs {
				if e != nil {
					t.Error(e)
				}
			}
			stats := s.Stats()
			raw, _ := json.Marshal(stats)
			t.Log(string(raw))
			if stats.DeliveredMessages != clients*count || stats.PeakSessions < 2 || stats.PeakBufferBytes > c.BufferBudget {
				t.Fatal("wrong multi-session totals")
			}
			if filtered && stats.FilteredDatagrams == 0 {
				t.Fatal("filter never exercised")
			}
		})
	}
}

func TestMultiShutdownReleasesPendingBuffers(t *testing.T) {
	_, i, c := testMulti(t)
	s, addr, stop := startMulti(t, c, i)
	conn, e := net.Dial("udp", addr)
	if e != nil {
		t.Fatal(e)
	}
	defer conn.Close()
	_ = conn.SetDeadline(time.Now().Add(time.Second))
	h, client, _ := secureudp.ClientHello(i.Public, i.PSK)
	_, _ = conn.Write(h)
	buf := make([]byte, 1200)
	n, e := conn.Read(buf)
	if e != nil || secureudp.VerifyWelcome(buf[:n], client) != nil {
		t.Fatal("handshake", e)
	}
	f, _ := reliable.Encode(reliable.Frame{Type: reliable.TypeData, ID: 1, Total: 1000, Payload: []byte{1}})
	packet, _ := client.Seal(f)
	_, _ = conn.Write(packet)
	_, e = conn.Read(buf)
	if e != nil {
		t.Fatal(e)
	}
	if s.Stats().BufferBytes == 0 {
		t.Fatal("no pending buffer")
	}
	stop()
	stats := s.Stats()
	if stats.BufferBytes != 0 || stats.ActiveSessions != 0 {
		t.Fatal("shutdown leaked", stats)
	}
}

func BenchmarkMultiAdmission1024(b *testing.B) {
	psk := make([]byte, 32)
	c := DefaultServerConfig()
	c.MaxSessionsPerIP = 1024
	c.HelloBurst = 1000000
	c.HelloRate = 1000000
	b.ReportAllocs()
	for n := 0; n < b.N; n++ {
		b.StopTimer()
		// Fresh authenticated transcripts per iteration. Replaying the same 1024
		// IDs benchmarks REJECTION, not admission, once replay protection works.
		i, _ := secureudp.NewIdentity(psk)
		hellos := make([][]byte, 1024)
		for j := range hellos {
			hellos[j], _, _ = secureudp.ClientHello(i.Public, psk)
		}
		s, _ := NewServer(i, c)
		b.StartTimer()
		for j, h := range hellos {
			s.handle(h, netip.AddrPortFrom(netip.MustParseAddr("127.0.0.1"), uint16(j+1)), time.Now(), discardPacket)
		}
		if len(s.sessions) != 1024 {
			b.Fatal("did not measure successful admission")
		}
	}
}

func TestMultiBatchLargerThanReceiverWindow(t *testing.T) {
	_, i, c := testMulti(t)
	s, addr, stop := startMulti(t, c, i)
	defer stop()
	messages := make([][]byte, 32)
	for n := range messages {
		messages[n] = bytes.Repeat([]byte{byte(n)}, 16<<10)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := ExchangeBatch(ctx, addr, i.Public, i.PSK, messages, Options{Timeout: 10 * time.Second}); err != nil {
		t.Fatal(err)
	}
	if s.Stats().DeliveredMessages != 32 {
		t.Fatal("incomplete batch")
	}
}
