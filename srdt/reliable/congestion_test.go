package reliable

import (
	"bytes"
	"encoding/binary"
	"testing"
	"time"
	"unsafe"
)

func controlledQueue(t *testing.T) *Queue {
	t.Helper()
	q, err := NewQueue(64<<10, 128<<10)
	if err != nil {
		t.Fatal(err)
	}
	if err = q.EnableCongestionControl(900, 106); err != nil {
		t.Fatal(err)
	}
	return q
}
func TestCongestionFreshFlightAndPacing(t *testing.T) {
	q := controlledQueue(t)
	defer q.Close()
	_ = q.Enqueue(1, make([]byte, 32768), time.Second)
	now := time.Unix(100, 0)
	// Initial immediate allowance is two packets, not the whole flight window.
	for n := 0; n < 2; n++ {
		if _, ok := q.Next(now, 900); !ok {
			t.Fatal("initial DATA blocked")
		}
	}
	if _, ok := q.Next(now, 900); ok {
		t.Fatal("unpaced burst")
	}
	if at, ok := q.Wake(now); !ok || !at.After(now) {
		t.Fatal("busy-loop wake", at)
	}
	// No ACKs: reaching the congestion window must stop new bytes.
	for n := 0; n < 8; n++ {
		now = now.Add(10 * time.Millisecond)
		if _, ok := q.Next(now, 900); !ok {
			t.Fatal("credit not replenished")
		}
	}
	if q.InFlight() != 9000 {
		t.Fatal(q.InFlight())
	}
	if _, ok := q.Next(now.Add(10*time.Millisecond), 900); ok {
		t.Fatal("flight exceeds cwnd")
	}
}
func TestCongestionInvalidAndRepeatedACKDoNotGrowWindow(t *testing.T) {
	q := controlledQueue(t)
	defer q.Close()
	_ = q.Enqueue(1, make([]byte, 32768), time.Second)
	now := time.Unix(100, 0)
	var frames []Frame
	for n := 0; n < 10; n++ {
		now = now.Add(10 * time.Millisecond)
		f, ok := q.Next(now, 900)
		if !ok {
			t.Fatal(n)
		}
		frames = append(frames, f)
	}
	before := q.CongestionStats().WindowBytes
	bad := Frame{Type: TypeAck, ID: 1, Total: 32768, Offset: 30000, Payload: []byte{0, 0, 0, 1}}
	if q.AckAt(bad, now) == nil || q.CongestionStats().WindowBytes != before {
		t.Fatal("invalid ACK grew window")
	}
	ack := acknowledgments(frames[0])[0]
	if err := q.AckAt(ack, now.Add(5*time.Millisecond)); err != nil {
		t.Fatal(err)
	}
	grew := q.CongestionStats().WindowBytes
	if grew <= before {
		t.Fatal("no slow-start growth")
	}
	for n := 0; n < 100; n++ {
		if err := q.AckAt(ack, now); err != nil {
			t.Fatal(err)
		}
	}
	if q.CongestionStats().WindowBytes != grew {
		t.Fatal("duplicate ACK inflation")
	}
}
func TestCongestionRetryAtFullWindowAndLossEpoch(t *testing.T) {
	q := controlledQueue(t)
	defer q.Close()
	_ = q.Enqueue(1, make([]byte, 32768), time.Second)
	start := time.Unix(100, 0)
	for n := 0; n < 10; n++ {
		if _, ok := q.Next(start.Add(time.Duration(n)*10*time.Millisecond), 900); !ok {
			t.Fatal(n)
		}
	}
	now := start.Add(time.Second)
	f, ok := q.NextSized(now, 900, 350)
	if !ok || len(f.Payload) > 350 {
		t.Fatal("retry stopped by full flight")
	}
	st := q.CongestionStats()
	if st.LossEvents != 1 || st.WindowBytes != 1800 || st.RetryPackets != 1 {
		t.Fatal(st)
	}
	// Partial retransmits of one pre-loss flight do not repeatedly cut cwnd.
	for n := 0; n < 5; n++ {
		now = now.Add(30 * time.Millisecond)
		q.NextSized(now, 900, 350)
	}
	st = q.CongestionStats()
	if st.LossEvents != 1 || q.InFlight() != 9000 {
		t.Fatal(st, q.InFlight())
	}
	// ACK of the retransmitted range is useful but not a growth/RTT sample.
	ack := acknowledgments(f)[0]
	_ = q.AckAt(ack, now)
	if q.CongestionStats().WindowBytes != 1800 || q.rtt.Samples() != 0 {
		t.Fatal("ambiguous ACK credited")
	}
}
func TestCongestionControlsNotBlockedByPacer(t *testing.T) {
	q := controlledQueue(t)
	defer q.Close()
	_ = q.UseWindowCredits(1, 2, 4096)
	_ = q.Enqueue(1, make([]byte, 4096), time.Second)
	q.congestion.tokens = 0
	f, ok := q.Next(time.Unix(100, 0), 900)
	if !ok || f.Type != TypeWindowOpen {
		t.Fatal("credit probe blocked by data pacer")
	}
}
func TestCongestionBoundedIdleBurstAndState(t *testing.T) {
	c, _ := newCongestion(65536, 900, 106)
	r := &RTTEstimator{}
	r.Observe(time.Millisecond)
	now := time.Unix(100, 0)
	c.refill(now, r)
	c.window = 65536
	c.lastSent = now
	now = now.Add(time.Hour)
	c.readyAt(now, 900, r)
	c.onSend(now, 900, false, r, time.Second)
	if c.window != 9000 || c.tokens > 1006 {
		t.Fatal("idle restart allows old-window burst", c)
	}
	if unsafe.Sizeof(*c) > 384 {
		t.Fatal("controller too large", unsafe.Sizeof(*c))
	}
	allocations := testing.AllocsPerRun(1000, func() {
		now = now.Add(time.Millisecond)
		c.readyAt(now, 900, r)
		c.onSend(now, 900, true, r, time.Second)
	})
	if allocations != 0 {
		t.Fatal("hot-path allocation", allocations)
	}
	t.Logf("controller_bytes=%d hot_path_allocs=%g", unsafe.Sizeof(*c), allocations)
}
func TestCongestionRecoveryCompletesWithRepacking(t *testing.T) {
	q := controlledQueue(t)
	defer q.Close()
	data := bytes.Repeat([]byte{1, 7, 0, 255}, 8192)
	_ = q.Enqueue(1, data, time.Second)
	r := NewReceiver()
	defer r.Close()
	now := time.Unix(100, 0)
	done := false
	for n := 0; n < 10000 && q.Pending() > 0; n++ {
		now = now.Add(5 * time.Millisecond)
		for j := 0; j < 16; j++ {
			f, ok := q.NextSized(now, 900, 350)
			if !ok {
				break
			}
			if len(f.Payload) > 500 {
				continue
			}
			acks, got, err := r.Accept(f)
			if err != nil {
				t.Fatal(err)
			}
			for _, ack := range acks {
				if err = q.AckAt(ack, now.Add(time.Millisecond)); err != nil {
					t.Fatal(err)
				}
			}
			if got != nil {
				if !bytes.Equal(got, data) {
					t.Fatal("mismatch")
				}
				done = true
			}
		}
	}
	if !done || q.Pending() != 0 {
		t.Fatal("no recovery", q.CongestionStats(), q.InFlight())
	}
	if q.CongestionStats().LossEvents == 0 || q.CongestionStats().RetryPackets == 0 {
		t.Fatal("not exercising retries")
	}
}
func TestCongestionPartialACKCreditIsUnique(t *testing.T) {
	c, _ := newCongestion(65536, 900, 106)
	c.window = 1800
	c.threshold = 1800
	for n := 0; n < 1800; n++ {
		c.onAck(1, 1800, true)
	}
	if c.window != 2700 {
		t.Fatal("byte counting", c.window)
	}
	c.onAck(0, 10000, true)
	c.onAck(20000, 10000, false)
	if c.window != 2700 {
		t.Fatal("growth without new eligible bytes")
	}
}
func BenchmarkCongestionPacer(b *testing.B) {
	c, _ := newCongestion(65536, 900, 106)
	r := &RTTEstimator{}
	r.Observe(10 * time.Millisecond)
	now := time.Unix(100, 0)
	b.ReportAllocs()
	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		now = now.Add(time.Millisecond)
		c.readyAt(now, 900, r)
		c.onSend(now, 900, false, r, time.Second)
	}
}
func TestCongestionNoGrowthFromOldFlight(t *testing.T) {
	q := controlledQueue(t)
	defer q.Close()
	_ = q.Enqueue(1, make([]byte, 32768), time.Second)
	now := time.Unix(100, 0)
	f, _ := q.Next(now, 900)
	q.congestion.onTimeout(now, now.Add(time.Second))
	ack := Frame{Type: TypeAck, ID: 1, Total: 32768, Offset: f.Offset, Payload: make([]byte, 4)}
	binary.BigEndian.PutUint32(ack.Payload, uint32(len(f.Payload)))
	_ = q.AckAt(ack, now.Add(2*time.Second))
	if q.CongestionStats().WindowBytes != 1800 {
		t.Fatal("old flight grew window")
	}
}

// A final/small original range fits below retryPayload. Merely changing the
// global retry maximum leaves its physical size identical forever.
func TestCongestionTailSizeBlackhole(t *testing.T) {
	q := controlledQueue(t)
	defer q.Close()
	data := bytes.Repeat([]byte{0xa5}, 193)
	if err := q.Enqueue(1, data, time.Millisecond); err != nil {
		t.Fatal(err)
	}
	r := NewReceiver()
	defer r.Close()
	now := time.Unix(100, 0)
	received := false
	for n := 0; n < 1000 && q.Pending() != 0; n++ {
		now = now.Add(20 * time.Millisecond)
		f, ok := q.NextSized(now, 900, 350)
		if !ok {
			continue
		}
		size := 58 + len(f.Payload) // existing UDP/AEAD/frame overhead, excluding IP
		if size >= 200 && size <= 280 {
			continue
		}
		acks, got, err := r.Accept(f)
		if err != nil {
			t.Fatal(err)
		}
		for _, a := range acks {
			if err = q.AckAt(a, now); err != nil {
				t.Fatal(err)
			}
		}
		if got != nil {
			received = bytes.Equal(data, got)
		}
	}
	if !received || q.Pending() != 0 {
		t.Fatalf("permanent tail-size loop: pending=%d control=%+v", q.Pending(), q.CongestionStats())
	}
}

func TestCongestionNoTinyFreshFragmentFromWindowRemainder(t *testing.T) {
	q := controlledQueue(t)
	defer q.Close()
	_ = q.Enqueue(1, make([]byte, 16384), time.Second)
	q.congestion.window = 1801
	now := time.Unix(100, 0)
	first, ok := q.Next(now, 900)
	if !ok {
		t.Fatal("first")
	}
	if _, ok = q.Next(now, 900); !ok {
		t.Fatal("second")
	}
	now = now.Add(100 * time.Millisecond)
	if f, ok := q.Next(now, 900); ok {
		t.Fatalf("window sliver emitted tiny DATA: len=%d", len(f.Payload))
	}
	if at, ok := q.Wake(now); !ok || !at.After(now) {
		t.Fatal("window sliver causes busy wake", at)
	}
	if err := q.AckAt(acknowledgments(first)[0], now); err != nil {
		t.Fatal(err)
	}
	if f, ok := q.Next(now, 900); !ok || len(f.Payload) != 900 {
		t.Fatal("full fragment not resumed", ok, len(f.Payload))
	}
}

func TestCongestionPacerAmortizesFastPathTimerQuantum(t *testing.T) {
	c, _ := newCongestion(65536, 900, 106)
	r := &RTTEstimator{}
	r.Observe(100 * time.Microsecond)
	c.window = 16384
	now := time.Unix(100, 0)
	c.refill(now, r)
	c.lastSent = now
	now = now.Add(time.Millisecond)
	c.refill(now, r)
	if c.tokens <= 4*1006 || c.tokens > float64(c.window)*1006/900 {
		t.Fatal("fast-path credit not bounded to useful quantum/window", c.tokens)
	}
	packets := 0
	for !c.readyAt(now, 900, r).After(now) {
		c.onSend(now, 900, false, r, time.Second)
		packets++
		if packets > 64 {
			t.Fatal("unbounded burst")
		}
	}
	if packets < 16 {
		t.Fatal("timer tick artificially throttles fast path", packets)
	}
}

func TestCongestionPacerSlowPathAndAbsoluteBurstCap(t *testing.T) {
	c, _ := newCongestion(1<<24, 900, 106)
	r := &RTTEstimator{}
	r.Observe(80 * time.Millisecond)
	c.window = 16384
	if c.burstLimit(r) != 4*1006 {
		t.Fatal("slow path burst changed", c.burstLimit(r))
	}
	r = &RTTEstimator{}
	r.Observe(time.Microsecond)
	c.window = 1 << 24
	if c.burstLimit(r) > 64*1006 {
		t.Fatal("large configuration permits unbounded burst")
	}
	c, _ = newCongestion(1, 900, 106)
	if c.burstLimit(r) < 1006 {
		t.Fatal("tiny flight window starves pacer")
	}
}
