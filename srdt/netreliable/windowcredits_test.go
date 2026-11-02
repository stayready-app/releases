package netreliable

import (
	"bytes"
	"context"
	"fmt"
	"testing"
	"time"

	"example.com/srdt/emulator"
	"example.com/srdt/reliable"
)

func TestWindowServerCapacityReplayModeAndExpiry(t *testing.T) {
	_, i, c := testMulti(t)
	c.AllowLegacyData = false
	c.MaxMessageBytes = 4096
	c.BufferBudget = reliable.BufferCharge(4096) * 2
	s, _ := NewServer(i, c)
	now := time.Now()
	_, client, p := admit(t, s, i, 1001, now)
	open := reliable.Frame{Type: reliable.TypeWindowOpen, ID: 7, Total: 4096}
	sendClient(t, s, client, p, open, now)
	for n := 0; n < 4; n++ {
		s.service(now, discardPacket)
	}
	if p.windowGranted != 3 || s.budget.Used() != c.BufferBudget || s.waitCount != 1 || s.creditScan != 0 {
		t.Fatal("unbounded grant", s.Stats())
	}
	for n := 0; n < 100; n++ {
		sendClient(t, s, client, p, open, now.Add(time.Second))
	}
	if s.budget.Used() != c.BufferBudget || !p.lastProgress.Equal(now) || p.windowGranted != 3 {
		t.Fatal("duplicate grew credit or pinned session")
	}
	bad := open
	bad.Total--
	sendClient(t, s, client, p, bad, now)
	if s.stats.CreditRejected != 1 || p.windowCapacity != 4096 {
		t.Fatal("capacity not immutable")
	}
	sendClient(t, s, client, p, reliable.Frame{Type: reliable.TypeOpen, ID: 9, Total: 1}, now)
	if s.stats.CreditRejected != 2 || p.flowMode != 3 {
		t.Fatal("mode switched")
	}
	sendClient(t, s, client, p, reliable.Frame{Type: reliable.TypeData, ID: 5, Total: 1, Payload: []byte{1}}, now)
	if s.stats.CreditViolations != 1 || s.stats.DeliveredMessages != 0 {
		t.Fatal("ungranted DATA")
	}
	for n := 0; n < 4; n++ {
		s.service(now.Add(c.ProgressTimeout+time.Second), discardPacket)
	}
	if len(s.sessions) != 0 || s.budget.Used() != 0 || s.waitCount != 0 {
		t.Fatal("orphan window pinned credit")
	}
}

func TestWindowFairnessWithExactAdmissionWaiters(t *testing.T) {
	_, i, c := testMulti(t)
	c.AllowLegacyData = false
	c.MaxMessageBytes = 1000
	c.BufferBudget = reliable.BufferCharge(1000)
	s, _ := NewServer(i, c)
	now := time.Now()
	_, a, pa := admit(t, s, i, 1001, now)
	_, b, pb := admit(t, s, i, 1002, now)
	_, d, pd := admit(t, s, i, 1003, now)
	sendClient(t, s, a, pa, reliable.Frame{Type: reliable.TypeWindowOpen, ID: 3, Total: 1000}, now)
	s.service(now, discardPacket)
	sendClient(t, s, b, pb, reliable.Frame{Type: reliable.TypeOpen, ID: 1, Total: 1000}, now)
	sendClient(t, s, d, pd, reliable.Frame{Type: reliable.TypeWindowOpen, ID: 1, Total: 1000}, now)
	for n := 0; n < 4; n++ {
		s.service(now, discardPacket)
	}
	s.remove(pa, false)
	s.service(now, discardPacket)
	if pb.receiver.ActiveBytes() != 1000 || pd.receiver.ActiveBytes() != 0 {
		t.Fatal("exact waiter starved by window")
	}
	s.remove(pb, false)
	s.service(now, discardPacket)
	if pd.receiver.ActiveBytes() != 1000 {
		t.Fatal("window waiter stranded")
	}
	s.remove(pd, false)
	if s.budget.Used() != 0 || s.waitCount != 0 {
		t.Fatal("mixed waiters leak")
	}
}

func TestWindowVariableSizesAndBoundedLookahead(t *testing.T) {
	_, i, c := testMulti(t)
	c.AllowLegacyData = false
	s, addr, stop := startMulti(t, c, i)
	defer stop()
	spec := faultSpec{up: emulator.Config{DropSizes: []emulator.SizeRange{{Min: 800, Max: 1100}}}}
	proxy, stopProxy := faultProxy(t, addr, spec)
	defer stopProxy()
	messages := make([][]byte, 400)
	messages[0] = bytes.Repeat([]byte{0x9a}, 32768) // First message is deliberately delayed by size filtering.
	for n := 1; n < len(messages); n++ {
		messages[n] = bytes.Repeat([]byte{byte(n)}, 1+n%63)
	}
	if err := ExchangeBatch(context.Background(), proxy, i.Public, i.PSK, messages, Options{Timeout: 20 * time.Second}); err != nil {
		t.Fatal(err)
	}
	r := s.Stats()
	if r.DeliveredMessages != 400 || r.ResourceDrops != 0 || r.CreditViolations != 0 || r.CreditRejected != 0 || r.BufferBytes != 0 || r.PeakBufferBytes > c.BufferBudget {
		t.Fatal(r)
	}
}

func TestWindowControlPaddingSizeBlackholes(t *testing.T) {
	for _, sizes := range []emulator.SizeRange{{Min: 58, Max: 58}, {Min: 200, Max: 280}} {
		t.Run(fmt.Sprintf("%d-%d", sizes.Min, sizes.Max), func(t *testing.T) {
			_, i, c := testMulti(t)
			c.AllowLegacyData = false
			s, addr, stop := startMulti(t, c, i)
			defer stop()
			cfg := emulator.Config{DropSizes: []emulator.SizeRange{sizes}, DuplicatePercent: 2, DelayMaxMs: 4}
			proxy, stopProxy := faultProxy(t, addr, faultSpec{up: cfg, down: cfg, inject: true})
			defer stopProxy()
			messages := make([][]byte, 12)
			for n := range messages {
				messages[n] = bytes.Repeat([]byte{byte(n)}, 4096)
			}
			if err := ExchangeBatch(context.Background(), proxy, i.Public, i.PSK, messages, Options{Timeout: 20 * time.Second}); err != nil {
				t.Fatal(err)
			}
			if r := s.Stats(); r.DeliveredMessages != 12 || r.ResourceDrops != 0 || r.CreditViolations != 0 {
				t.Fatal(r)
			}
		})
	}
}

func TestWindowExplicitLegacyCompatibility(t *testing.T) {
	_, i, c := testMulti(t)
	c.AllowLegacyData = false
	s, addr, stop := startMulti(t, c, i)
	defer stop()
	if err := ExchangeBatch(context.Background(), addr, i.Public, i.PSK, [][]byte{[]byte("stage16"), []byte("stage16again")}, Options{LegacyCredits: true}); err != nil {
		t.Fatal(err)
	}
	if r := s.Stats(); r.DeliveredMessages != 2 || r.CreditRequests == 0 || r.WindowRequests != 0 {
		t.Fatal(r)
	}
}

func TestWindowPerSessionBudgetNeverMultiplies(t *testing.T) {
	_, i, c := testMulti(t)
	c.AllowLegacyData = false
	c.MaxMessageBytes = 32768
	s, _ := NewServer(i, c)
	now := time.Now()
	_, client, p := admit(t, s, i, 1001, now)
	sendClient(t, s, client, p, reliable.Frame{Type: reliable.TypeWindowOpen, ID: 255, Total: 32768}, now)
	for n := 0; n < 40; n++ {
		s.service(now, discardPacket)
	}
	if p.windowGranted != 3 || p.receiver.ActiveBytes() != c.SessionBufferBytes || s.budget.Used() != 2*reliable.BufferCharge(32768) {
		t.Fatal("lookahead multiplied allocation", s.Stats())
	}
	// Invalid new requests may not expand the immutable admitted capacity.
	sendClient(t, s, client, p, reliable.Frame{Type: reliable.TypeWindowOpen, ID: 257, Total: 32768}, now)
	if s.stats.CreditRejected != 1 {
		t.Fatal("unbounded sparse demand")
	}
	s.remove(p, false)
	if s.budget.Used() != 0 {
		t.Fatal("leaked capacity")
	}
}

// Explicit Stage 20 compatibility keeps the old fixed-size HELLO and must fail
// closed when that exact size is permanently blocked.
func TestWindowLegacyFixedHelloFilterFailsClosed(t *testing.T) {
	_, i, c := testMulti(t)
	c.AllowLegacyData = false
	s, addr, stop := startMulti(t, c, i)
	defer stop()
	cfg := emulator.Config{DropSizes: []emulator.SizeRange{{Min: 81, Max: 81}}}
	proxy, stopProxy := faultProxy(t, addr, faultSpec{up: cfg, down: cfg})
	defer stopProxy()
	ctx, cancel := context.WithTimeout(context.Background(), 400*time.Millisecond)
	defer cancel()
	if err := Exchange(ctx, proxy, i.Public, i.PSK, []byte("blocked before admission"), Options{Timeout: time.Second, LegacyHandshake: true}); err == nil {
		t.Fatal("false successful connection")
	}
	if r := s.Stats(); r.ActiveSessions != 0 || r.BufferBytes != 0 || r.DeliveredMessages != 0 {
		t.Fatal("blocked handshake allocated a session", r)
	}
}

func TestWindowFutureReverseGrantsCannotPinIdleReservations(t *testing.T) {
	_, i, c := testMulti(t)
	c.AllowLegacyData = false
	c.MaxMessageBytes = 1000
	s, _ := NewServer(i, c)
	now := time.Now()
	_, client, p := admit(t, s, i, 1001, now)
	sendClient(t, s, client, p, reliable.Frame{Type: reliable.TypeWindowOpen, ID: 15, Total: 1000}, now)
	for n := 0; n < 4; n++ {
		s.service(now, discardPacket)
	}
	// The echo server announced future reverse demand before receiving DATA.
	// Granting an ever-larger unused prefix must not reset the no-progress clock.
	for id := uint64(2); id <= 16; id += 2 {
		sendClient(t, s, client, p, reliable.Frame{Type: reliable.TypeWindowGrant, ID: id, Total: 1000}, now.Add(14*time.Second))
	}
	if !p.lastProgress.Equal(now) {
		t.Fatal("unused future permissions pinned payload reservations")
	}
	for n := 0; n < 4; n++ {
		s.service(now.Add(16*time.Second), discardPacket)
	}
	if len(s.sessions) != 0 || s.budget.Used() != 0 {
		t.Fatal("reservation leak")
	}
}

func TestWindowV1HelloSurvivesLegacyExactSizeFilter(t *testing.T) {
	_, i, c := testMulti(t)
	c.AllowLegacyData = false
	s, addr, stop := startMulti(t, c, i)
	defer stop()
	cfg := emulator.Config{DropSizes: []emulator.SizeRange{{Min: 81, Max: 81}}}
	proxy, stopProxy := faultProxy(t, addr, faultSpec{up: cfg, down: cfg})
	defer stopProxy()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := Exchange(ctx, proxy, i.Public, i.PSK, []byte("v1 variable hello"), Options{Timeout: 5 * time.Second}); err != nil {
		t.Fatal(err)
	}
	if r := s.Stats(); r.DeliveredMessages != 1 || r.V1Sessions != 1 {
		t.Fatal("V1 session not delivered", r)
	}
}
