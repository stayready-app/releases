package netreliable

import (
	"bytes"
	"context"
	"net/netip"
	"sync"
	"testing"
	"time"

	"example.com/srdt/emulator"
	"example.com/srdt/reliable"
)

func TestCreditsDataNeedsPermissionAndNoDowngrade(t *testing.T) {
	_, i, c := testMulti(t)
	c.AllowLegacyData = false
	s, _ := NewServer(i, c)
	now := time.Now()
	_, client, p := admit(t, s, i, 1234, now)
	f := reliable.Frame{Type: reliable.TypeData, ID: 1, Total: 1, Payload: []byte{7}}
	sendClient(t, s, client, p, f, now)
	if s.stats.CreditViolations != 1 || p.confirmed || s.budget.Used() != 0 || p.receiver != nil {
		t.Fatal("unsolicited DATA allocated state")
	}
	sendClient(t, s, client, p, reliable.Frame{Type: reliable.TypeOpen, ID: 1, Total: 1}, now)
	if s.budget.Used() != reliable.BufferCharge(1) {
		t.Fatal("missing grant reservation")
	}
	sendClient(t, s, client, p, f, now)
	if s.stats.DeliveredMessages != 1 || p.queue.Pending() != 1 {
		t.Fatal("granted delivery failed")
	}
	open, ok := p.queue.Next(now, 900)
	if !ok || open.Type != reliable.TypeOpen || open.ID != 2 {
		t.Fatal("response omitted receive admission")
	}
	sendClient(t, s, client, p, reliable.Frame{Type: reliable.TypeGrant, ID: 2, Total: 1}, now)
	data, ok := p.queue.Next(now, 900)
	if !ok || data.Type != reliable.TypeData {
		t.Fatal("reply grant ignored")
	}
	sendClient(t, s, client, p, reliable.Frame{Type: reliable.TypeAck, ID: 2, Total: 1, Payload: []byte{0, 0, 0, 1}}, now)
	if s.budget.Used() != 0 {
		t.Fatal("reply ACK did not release credit")
	}
	sendClient(t, s, client, p, reliable.Frame{Type: reliable.TypeData, ID: 3, Total: 1, Payload: []byte{7}}, now)
	if s.stats.CreditViolations != 2 {
		t.Fatal("session downgraded after credit exchange")
	}
}

func TestCreditsReservationLossDuplicateAndConflictingLength(t *testing.T) {
	_, i, c := testMulti(t)
	c.AllowLegacyData = false
	s, _ := NewServer(i, c)
	now := time.Now()
	_, client, p := admit(t, s, i, 1234, now)
	open := reliable.Frame{Type: reliable.TypeOpen, ID: 1, Total: 1000}
	sendClient(t, s, client, p, open, now) // Discard the first GRANT.
	for n := 0; n < 100; n++ {
		sendClient(t, s, client, p, open, now.Add(time.Second))
	}
	if s.budget.Used() != reliable.BufferCharge(1000) || s.stats.CreditsGranted != 1 {
		t.Fatal("lost grant consumed new memory")
	}
	if !p.lastProgress.Equal(now) {
		t.Fatal("duplicate OPEN refreshed no-progress deadline")
	}
	open.Total--
	sendClient(t, s, client, p, open, now.Add(time.Second))
	if s.stats.CreditRejected != 1 {
		t.Fatal("changed length permitted")
	}
	s.service(now.Add(c.ProgressTimeout+time.Second), discardPacket)
	if s.budget.Used() != 0 || len(s.sessions) != 0 || s.waitCount != 0 {
		t.Fatal("unused reservation leak")
	}
}

func TestCreditsWaitQueueFairnessReleaseAndNoPolling(t *testing.T) {
	_, i, c := testMulti(t)
	c.AllowLegacyData = false
	c.MaxMessageBytes = 1000
	c.BufferBudget = reliable.BufferCharge(1000)
	s, _ := NewServer(i, c)
	now := time.Now()
	_, a, pa := admit(t, s, i, 1001, now)
	_, b, pb := admit(t, s, i, 1002, now)
	_, d, pd := admit(t, s, i, 1003, now)
	open := reliable.Frame{Type: reliable.TypeOpen, ID: 1, Total: 1000}
	sendClient(t, s, a, pa, open, now)
	sendClient(t, s, b, pb, open, now)
	sendClient(t, s, d, pd, open, now)
	s.service(now, discardPacket)
	if s.waitCount != 2 || s.creditScan != 0 || s.stats.ResourceDrops != 0 {
		t.Fatal("blocked clients not quiescent", s.Stats())
	}
	for n := 0; n < 50; n++ {
		s.service(now, discardPacket)
	}
	if s.stats.CreditsGranted != 1 {
		t.Fatal("overcommitted receive credit")
	}
	s.remove(pa, false) // Release a global reservation and wake the fair queue.
	s.service(now, discardPacket)
	if pb.receiver.ActiveBytes() != 1000 || pd.receiver.ActiveBytes() != 0 || s.waitCount != 1 {
		t.Fatal("FIFO fairness not preserved")
	}
	s.remove(pb, false)
	s.service(now, discardPacket)
	if pd.receiver.ActiveBytes() != 1000 || s.waitCount != 0 {
		t.Fatal("last waiter stranded")
	}
	s.remove(pd, false)
	if s.budget.Used() != 0 || s.waitHead != nil || s.waitTail != nil || s.creditScan != 0 {
		t.Fatal("wait-list cleanup leaked")
	}
}

func TestCreditsPendingRequestsBoundedAndDuplicatesDoNotPin(t *testing.T) {
	_, i, c := testMulti(t)
	c.AllowLegacyData = false
	c.MaxMessageBytes = 1000
	c.BufferBudget = reliable.BufferCharge(1000)
	s, _ := NewServer(i, c)
	now := time.Now()
	_, a, pa := admit(t, s, i, 1, now)
	_, b, pb := admit(t, s, i, 2, now)
	sendClient(t, s, a, pa, reliable.Frame{Type: reliable.TypeOpen, ID: 1, Total: 1000}, now)
	for id := uint64(1); id < 100; id += 2 {
		sendClient(t, s, b, pb, reliable.Frame{Type: reliable.TypeOpen, ID: id, Total: 1000}, now)
	}
	if len(pb.pendingOpens) != c.MaxMessages || s.waitCount != 1 {
		t.Fatal("unbounded pending metadata")
	}
	for n := 0; n < 100; n++ {
		sendClient(t, s, b, pb, reliable.Frame{Type: reliable.TypeOpen, ID: 1, Total: 1000}, now.Add(14*time.Second))
	}
	if !pb.lastProgress.Equal(now) {
		t.Fatal("probes pinned blocked peer")
	}
	for n := 0; n < 4; n++ {
		s.service(now.Add(16*time.Second), discardPacket)
	}
	if len(s.sessions) != 0 || s.waitCount != 0 || s.budget.Used() != 0 {
		t.Fatal("blocked expiry leaked")
	}
}

func TestCreditsManyWaitersFairBoundedPass(t *testing.T) {
	_, i, c := testMulti(t)
	c.AllowLegacyData = false
	c.MaxMessageBytes = 1000
	c.BufferBudget = reliable.BufferCharge(1000)
	s, _ := NewServer(i, c)
	now := time.Now()
	for n := 0; n < 64; n++ {
		_, client, p := admit(t, s, i, uint16(1000+n), now)
		sendClient(t, s, client, p, reliable.Frame{Type: reliable.TypeOpen, ID: 1, Total: 1000}, now)
	}
	for n := 0; n < 10; n++ {
		s.service(now, discardPacket)
	}
	if s.waitCount != 63 || s.creditScan != 0 {
		t.Fatal("full unsuccessful pass did not sleep")
	}
	for _, p := range s.sessions {
		s.remove(p, false)
	}
	if s.waitCount != 0 || s.budget.Used() != 0 {
		t.Fatal("mass waiter cleanup")
	}
}

func TestCreditsRealUDPMinimalGlobalBudget(t *testing.T) {
	_, i, c := testMulti(t)
	c.AllowLegacyData = false
	c.MaxMessageBytes = 4096
	c.BufferBudget = reliable.BufferCharge(4096)
	c.ProgressTimeout = 20 * time.Second
	s, addr, stop := startMulti(t, c, i)
	defer stop()
	const clients = 16
	var wg sync.WaitGroup
	errs := make(chan error, clients)
	for n := 0; n < clients; n++ {
		wg.Add(1)
		go func(n int) {
			defer wg.Done()
			m := make([][]byte, 8)
			for j := range m {
				m[j] = bytes.Repeat([]byte{byte(n), byte(j)}, 2048)
			}
			errs <- ExchangeBatch(context.Background(), addr, i.Public, i.PSK, m, Options{Timeout: 20 * time.Second})
		}(n)
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Error(err)
		}
	}
	stats := s.Stats()
	t.Logf("minimal budget: %+v", stats)
	if stats.DeliveredMessages != clients*8 || stats.ResourceDrops != 0 || stats.CreditViolations != 0 || stats.PeakBufferBytes > c.BufferBudget || stats.CreditsGranted != clients*8 {
		t.Fatal("receive-credit invariant failed", stats)
	}
}

func TestCreditsTooLargeMessageFailsExplicitly(t *testing.T) {
	_, i, c := testMulti(t)
	c.AllowLegacyData = false
	c.MaxMessageBytes = 100
	s, addr, stop := startMulti(t, c, i)
	defer stop()
	start := time.Now()
	err := Exchange(context.Background(), addr, i.Public, i.PSK, make([]byte, 101), Options{Timeout: 5 * time.Second})
	if err == nil || time.Since(start) > time.Second || s.Stats().CreditRejected != 1 || s.Stats().BufferBytes != 0 {
		t.Fatal("rejection was not explicit", err, s.Stats())
	}
}

func TestCreditsControlWorkBound(t *testing.T) {
	// Releasing one session must not synchronously sweep all blocked peers or
	// allocate a goroutine/timer per request. serviceCredits itself is bounded.
	_, i, c := testMulti(t)
	c.AllowLegacyData = false
	c.MaxMessageBytes = 1000
	c.BufferBudget = reliable.BufferCharge(1000)
	s, _ := NewServer(i, c)
	now := time.Now()
	var first *peerSession
	for n := 0; n < 32; n++ {
		_, client, p := admit(t, s, i, uint16(100+n), now)
		if n == 0 {
			first = p
		}
		sendClient(t, s, client, p, reliable.Frame{Type: reliable.TypeOpen, ID: 1, Total: 1000}, now)
	}
	s.remove(first, false)
	sent := 0
	s.service(now, func([]byte, netip.AddrPort) error { sent++; return nil })
	if sent > 8 {
		t.Fatal("unbounded grant burst", sent)
	}
	for _, p := range s.sessions {
		s.remove(p, false)
	}
}

func TestCreditsPermanentControlSizeFilter(t *testing.T) {
	_, i, c := testMulti(t)
	c.AllowLegacyData = false
	s, addr, stop := startMulti(t, c, i)
	defer stop()
	spec := faultSpec{}
	spec.up.DropSizes = []emulator.SizeRange{{Min: 58, Max: 58}}
	spec.down.DropSizes = []emulator.SizeRange{{Min: 58, Max: 58}}
	proxy, stopProxy := faultProxy(t, addr, spec)
	defer stopProxy()
	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()
	err := Exchange(ctx, proxy, i.Public, i.PSK, []byte("all control sizes blocked"), Options{Timeout: time.Second, LegacyCredits: true, LegacyHandshake: true})
	if err == nil || s.Stats().DeliveredMessages != 0 || s.Stats().BufferBytes != 0 {
		t.Fatal("permanent credit blackhole reported success", err, s.Stats())
	}
}
