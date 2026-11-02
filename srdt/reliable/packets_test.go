package reliable

import (
	"bytes"
	"encoding/binary"
	"testing"
	"time"
	"unsafe"
)

func packetReceipt(f Frame) Frame {
	b := make([]byte, 4)
	binary.BigEndian.PutUint32(b, uint32(len(f.Payload)))
	return Frame{Type: TypePacketAck, ID: f.ID, Offset: f.Offset, Total: f.Total, Payload: b, PacketNumber: f.PacketNumber}
}
func packetQueue(t *testing.T, cc bool) *Queue {
	t.Helper()
	q, _ := NewQueue(64<<10, 128<<10)
	if cc {
		if e := q.EnableCongestionControl(900, 106); e != nil {
			t.Fatal(e)
		}
	}
	if e := q.EnablePacketTracking(nil, 900, 114); e != nil {
		t.Fatal(e)
	}
	t.Cleanup(q.Close)
	return q
}
func TestPacketCodec(t *testing.T) {
	for _, kind := range []byte{TypePacketData, TypePacketAck} {
		f := Frame{Type: kind, ID: 12, Total: 9, Offset: 2, PacketNumber: 71, Payload: []byte{3, 4, 5}}
		if kind == TypePacketAck {
			f.Payload = []byte{0, 0, 0, 3}
		}
		b, e := Encode(f)
		if e != nil {
			t.Fatal(e)
		}
		got, e := Decode(b)
		if e != nil || got.Type != f.Type || got.PacketNumber != 71 || !bytes.Equal(got.Payload, f.Payload) {
			t.Fatal(got, e)
		}
		if len(b) != HeaderSize+8+len(f.Payload) {
			t.Fatal("packet number overhead")
		}
		clear(b[HeaderSize : HeaderSize+8])
		if _, e = Decode(b); e == nil {
			t.Fatal("accepted zero number")
		}
	}
	f := Frame{Type: TypeData, ID: 1, Total: 1, Payload: []byte{1}, PacketNumber: 1}
	if _, e := Encode(f); e == nil {
		t.Fatal("legacy packet number smuggling")
	}
}
func TestPacketReceiptExactAndCopiesIndependent(t *testing.T) {
	q := packetQueue(t, true)
	now := time.Now()
	if e := q.Enqueue(1, make([]byte, 900), time.Second); e != nil {
		t.Fatal(e)
	}
	f, ok := q.Next(now, 900)
	if !ok || f.Type != TypePacketData {
		t.Fatal(f, ok)
	}
	// A distinct physical copy of the same logical bytes. The first receipt may
	// retire the Sender, but it must NOT remove the second copy from flight.
	copyFrame := q.packets.sent(Frame{Type: TypeData, ID: 1, Total: 900, Payload: make([]byte, 900)}, now.Add(time.Millisecond), time.Second, 114)
	before := q.CongestionStats()
	bad := packetReceipt(f)
	bad.PacketNumber = 12345
	if e := q.AckAt(bad, now.Add(5*time.Millisecond)); e == nil {
		t.Fatal("unsent packet accepted")
	}
	if q.InFlight() != 900 || q.PacketStats().WireInFlight != 2028 || q.CongestionStats() != before {
		t.Fatal("bad receipt changed state")
	}
	bad = packetReceipt(f)
	bad.Offset++
	if e := q.AckAt(bad, now.Add(5*time.Millisecond)); e == nil {
		t.Fatal("mismatched range accepted")
	}
	if e := q.AckAt(packetReceipt(f), now.Add(10*time.Millisecond)); e != nil {
		t.Fatal(e)
	}
	if q.Pending() != 0 || q.PacketStats().WireInFlight != 1014 {
		t.Fatal(q.Pending(), q.PacketStats())
	}
	if at, ok := q.Wake(now.Add(10 * time.Millisecond)); !ok || !at.After(now.Add(10*time.Millisecond)) {
		t.Fatal("lost last-copy timer", at, ok)
	}
	before = q.CongestionStats()
	if e := q.AckAt(packetReceipt(f), now.Add(11*time.Millisecond)); e != nil {
		t.Fatal(e)
	}
	if q.PacketStats().WireInFlight != 1014 || q.CongestionStats() != before {
		t.Fatal("duplicate receipt frees/grows twice")
	}
	if e := q.AckAt(packetReceipt(copyFrame), now.Add(12*time.Millisecond)); e != nil {
		t.Fatal(e)
	}
	if q.PacketStats().WireInFlight != 0 || q.PacketStats().Receipts != 2 || q.PacketStats().DuplicateReceipts != 1 {
		t.Fatal(q.PacketStats())
	}
}
func TestPacketLateReceiptAndRetransmitRTT(t *testing.T) {
	q := packetQueue(t, true)
	now := time.Now()
	_ = q.Enqueue(1, bytes.Repeat([]byte{3}, 900), time.Second)
	f, _ := q.Next(now, 900)
	q.packets.expire(now.Add(301*time.Millisecond), q.congestion)
	if q.PacketStats().Lost != 1 || q.PacketStats().WireInFlight != 0 {
		t.Fatal(q.PacketStats())
	}
	retry, ok := q.NextSized(now.Add(310*time.Millisecond), 900, 350)
	if !ok || retry.PacketNumber == f.PacketNumber || retry.PacketNumber == 0 {
		t.Fatal("missing distinct retry", retry, ok)
	}
	if e := q.AckAt(packetReceipt(retry), now.Add(330*time.Millisecond)); e != nil {
		t.Fatal(e)
	}
	if q.rtt.Samples() != 1 || q.rtt.smoothed != 20*time.Millisecond {
		t.Fatal("exact retransmission receipt did not sample RTT", q.rtt)
	}
	if e := q.AckAt(packetReceipt(f), now.Add(340*time.Millisecond)); e != nil {
		t.Fatal(e)
	}
	if q.Pending() != 0 || q.PacketStats().LateReceipts != 1 {
		t.Fatal(q.Pending(), q.PacketStats())
	}
}
func TestPacketStateBudgetAndRingBound(t *testing.T) {
	if unsafe.Sizeof(packetLedger{}) > uintptr(PacketStateCharge) {
		t.Fatal("unaccounted ledger allocation", unsafe.Sizeof(packetLedger{}))
	}
	b, _ := NewBudget(PacketStateCharge)
	q, _ := NewQueue(65536, 65536)
	if e := q.EnablePacketTracking(b, 1, 114); e != nil {
		t.Fatal(e)
	}
	other, _ := NewQueue(65536, 65536)
	if e := other.EnablePacketTracking(b, 1, 114); e != nil {
		t.Fatal(e)
	}
	if b.Used() != 0 {
		t.Fatal("idle ledger charged storage", b.Used())
	}
	now := time.Now()
	_ = q.Enqueue(1, make([]byte, 32768), time.Second)
	var first Frame
	for i := 0; i < PacketSlots; i++ {
		f, ok := q.Next(now, 1)
		if !ok {
			t.Fatal("early ring block", i)
		}
		if i == 0 {
			first = f
		}
	}
	if _, ok := q.Next(now, 1); ok {
		t.Fatal("ring overwrote live transmission")
	}
	if q.PacketStats().Outstanding != PacketSlots || b.Used() != PacketStateCharge {
		t.Fatal(q.PacketStats(), b.Used())
	}
	_ = other.Enqueue(2, []byte{1}, time.Second)
	if _, ok := other.Next(now, 1); ok || b.Used() != PacketStateCharge {
		t.Fatal("second ledger bypassed shared budget", ok, b.Used())
	}
	q.packets.expire(now.Add(time.Second), nil)
	if q.PacketStats().WireInFlight != 0 {
		t.Fatal(q.PacketStats())
	}
	if _, ok := q.Next(now.Add(time.Second), 1); !ok {
		t.Fatal("expiry did not unblock ring")
	}
	before := q.InFlight()
	if e := q.AckAt(packetReceipt(first), now.Add(time.Second)); e != ErrStale || q.InFlight() != before {
		t.Fatal("overwritten ACK had authority", e)
	}
	q.Close()
	q.Close()
	other.Close()
	if b.Used() != 0 {
		t.Fatal("leaked packet budget", b.Used())
	}
}
func TestPacketPhysicalWindowAndLegacyAckReject(t *testing.T) {
	q := packetQueue(t, true)
	now := time.Now()
	_ = q.Enqueue(1, make([]byte, 32000), time.Second)
	q.congestion.tokens = 1e9 // Isolate flight admission, not pacing, in this test.
	for i := 0; i < 100; i++ {
		if _, ok := q.Next(now, 900); !ok {
			break
		}
	}
	st := q.PacketStats()
	if st.WireInFlight > q.congestion.window || st.Outstanding != 10 {
		t.Fatal(st, q.congestion.window)
	}
	legacy := Frame{Type: TypeAck, ID: 1, Total: 32000, Payload: []byte{0, 0, 3, 132}}
	before := q.InFlight()
	if e := q.AckAt(legacy, now); e == nil || q.InFlight() != before {
		t.Fatal("implicit ACK downgrade", e)
	}
}
func TestPacketReceiverPreservesReceiptOnDuplicate(t *testing.T) {
	r := NewReceiver()
	defer r.Close()
	f := Frame{Type: TypePacketData, ID: 1, Total: 3, Payload: []byte{1, 2, 3}, PacketNumber: 7}
	ack, data, e := r.Accept(f)
	if e != nil || !bytes.Equal(data, f.Payload) || len(ack) != 1 || ack[0].PacketNumber != 7 || ack[0].Type != TypePacketAck {
		t.Fatal(ack, data, e)
	}
	f.PacketNumber = 8
	ack, data, e = r.Accept(f)
	if e != nil || data != nil || ack[0].PacketNumber != 8 {
		t.Fatal("duplicate application delivery or wrong receipt", ack, data, e)
	}
}
func TestPacketNoCounterWrap(t *testing.T) {
	p, _ := newPacketLedger(nil)
	p.next = ^uint64(0)
	if p.canSend() {
		t.Fatal("counter reuse permitted")
	}
	p.close()
}
func BenchmarkPacketLedgerReceipt(b *testing.B) {
	p, _ := newPacketLedger(nil)
	defer p.close()
	if !p.ensureStorage() {
		b.Fatal("packet ledger storage unavailable")
	}
	now := time.Now()
	f := Frame{Type: TypeData, ID: 1, Total: 900, Payload: make([]byte, 900)}
	ack := Frame{Type: TypePacketAck, ID: 1, Total: 900, Payload: []byte{0, 0, 3, 132}}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		sent := p.sent(f, now, time.Second, 114)
		ack.PacketNumber = sent.PacketNumber
		r, e := p.check(ack)
		if e != nil {
			b.Fatal(e)
		}
		p.acknowledge(r)
	}
}

func FuzzPacketReceipt(f *testing.F) {
	good, _ := Encode(Frame{Type: TypePacketAck, ID: 5, Total: 900, Payload: []byte{0, 0, 3, 132}, PacketNumber: 1})
	f.Add(good)
	f.Add([]byte{TypePacketAck})
	f.Add([]byte{})
	f.Fuzz(func(t *testing.T, raw []byte) {
		if len(raw) > 2048 {
			return
		}
		q, _ := NewQueue(2048, 2048)
		if e := q.EnablePacketTracking(nil, 900, 114); e != nil {
			t.Fatal(e)
		}
		defer q.Close()
		now := time.Unix(123, 0)
		_ = q.Enqueue(5, make([]byte, 900), time.Second)
		_, _ = q.Next(now, 900)
		receipt, e := Decode(raw)
		if e != nil {
			return
		}
		before := q.PacketStats()
		if e = q.AckAt(receipt, now.Add(time.Millisecond)); e != nil {
			if q.InFlight() != 900 || q.Pending() != 1 || q.PacketStats() != before {
				t.Fatal("invalid receipt mutated state")
			}
		} else if q.InFlight() != 0 || q.Pending() != 0 || q.PacketStats().WireInFlight != 0 || q.PacketStats().Receipts != 1 {
			t.Fatal("receipt accounting mismatch")
		}
	})
}

func TestPacketClosedQueueCannotReacquireMemory(t *testing.T) {
	b, _ := NewBudget(16384)
	q, _ := NewQueue(1024, 1024)
	_ = q.SetBudget(b)
	_ = q.EnablePacketTracking(b, 900, 114)
	q.Close()
	if e := q.Enqueue(1, []byte{1}, time.Second); e == nil || b.Used() != 0 {
		t.Fatal("closed queue acquired data", e, b.Used())
	}
	if e := q.EnableCongestionControl(900, 114); e == nil {
		t.Fatal("closed queue reconfigured")
	}
	if e := q.EnableAdaptiveRTO(); e == nil {
		t.Fatal("closed queue RTO reset")
	}
	if e := q.UseWindowCredits(1, 2, 32); e == nil {
		t.Fatal("closed queue opened credits")
	}
}

func TestPacketClosedQueueStopsControlTimers(t *testing.T) {
	q, _ := NewQueue(1024, 1024)
	_ = q.UseWindowCredits(1, 2, 32)
	_ = q.RequestWindow(3)
	q.Close()
	if _, ok := q.Next(time.Now(), 900); ok {
		t.Fatal("closed queue emitted control")
	}
	if _, ok := q.Wake(time.Now()); ok {
		t.Fatal("closed queue has live timer")
	}
	if e := q.RequestWindow(5); e == nil {
		t.Fatal("closed queue extended demand")
	}
}

func TestPacketAckBatchReceiverAndSender(t *testing.T) {
	receiver := NewReceiver()
	if err := receiver.EnablePacketAckBatching(); err != nil {
		t.Fatal(err)
	}

	// The first two packet receipts are deliberately sent immediately. This
	// seeds RTT/pacing before delayed batching is enabled.
	for i, packetNumber := range []uint64{99, 100} {
		warmup := Frame{
			Type:         TypePacketData,
			ID:           uint64(10 + i),
			Total:        1,
			Payload:      []byte{byte(i)},
			PacketNumber: packetNumber,
		}
		acks, data, err := receiver.Accept(warmup)
		if err != nil || len(data) != 1 || len(acks) != 1 || acks[0].Type != TypePacketAck ||
			acks[0].PacketNumber != packetNumber {
			t.Fatal("bad packet ACK warmup", packetNumber, acks, data, err)
		}
	}

	first := Frame{Type: TypePacketData, ID: 20, Total: 6, Offset: 0, Payload: []byte{1, 2, 3}, PacketNumber: 101}
	acks, data, err := receiver.Accept(first)
	if err != nil || data != nil || len(acks) != 0 {
		t.Fatal("first batched receipt should be deferred", acks, data, err)
	}
	second := Frame{Type: TypePacketData, ID: 20, Total: 6, Offset: 3, Payload: []byte{4, 5, 6}, PacketNumber: 102}
	acks, data, err = receiver.Accept(second)
	if err != nil || !bytes.Equal(data, []byte{1, 2, 3, 4, 5, 6}) || len(acks) != 1 {
		t.Fatal(acks, data, err)
	}
	ack := acks[0]
	if ack.Type != TypePacketAckBatch || ack.Total != 2 || len(ack.Payload) != 16 ||
		binary.BigEndian.Uint64(ack.Payload[:8]) != 101 || binary.BigEndian.Uint64(ack.Payload[8:]) != 102 {
		t.Fatal("bad batch", ack)
	}
	encoded, err := Encode(ack)
	if err != nil {
		t.Fatal(err)
	}
	decoded, err := Decode(encoded)
	if err != nil || decoded.Type != TypePacketAckBatch || decoded.Total != 2 || !bytes.Equal(decoded.Payload, ack.Payload) {
		t.Fatal(decoded, err)
	}

	q := packetQueue(t, true)
	now := time.Now()
	if err := q.Enqueue(1, []byte{1, 2, 3, 4, 5, 6}, time.Second); err != nil {
		t.Fatal(err)
	}
	f1, ok := q.Next(now, 3)
	if !ok {
		t.Fatal("missing first send")
	}
	f2, ok := q.Next(now, 3)
	if !ok {
		t.Fatal("missing second send")
	}
	var payload [16]byte
	binary.BigEndian.PutUint64(payload[:8], f1.PacketNumber)
	binary.BigEndian.PutUint64(payload[8:], f2.PacketNumber)
	if err := q.AckAt(Frame{Type: TypePacketAckBatch, Total: 2, Payload: payload[:]}, now.Add(time.Millisecond)); err != nil {
		t.Fatal(err)
	}
	if q.Pending() != 0 || q.InFlight() != 0 || q.PacketStats().Receipts != 2 || q.PacketStats().Outstanding != 0 {
		t.Fatal(q.Pending(), q.InFlight(), q.PacketStats())
	}
}

func TestPacketAckBatchInvalidIsAtomic(t *testing.T) {
	q := packetQueue(t, true)
	now := time.Now()
	if err := q.Enqueue(1, make([]byte, 6), time.Second); err != nil {
		t.Fatal(err)
	}
	f1, _ := q.Next(now, 3)
	_, _ = q.Next(now, 3)
	before := q.PacketStats()
	var payload [16]byte
	binary.BigEndian.PutUint64(payload[:8], f1.PacketNumber)
	binary.BigEndian.PutUint64(payload[8:], 1<<63)
	if err := q.AckAt(Frame{Type: TypePacketAckBatch, Total: 2, Payload: payload[:]}, now.Add(time.Millisecond)); err == nil {
		t.Fatal("future packet number accepted")
	}
	if q.PacketStats() != before || q.InFlight() != 6 || q.Pending() != 1 {
		t.Fatal("invalid batch mutated state", before, q.PacketStats(), q.InFlight(), q.Pending())
	}
}

func TestPacketStorageReleasedWhenDirectionBecomesIdle(t *testing.T) {
	b, err := NewBudget(PacketStateCharge)
	if err != nil {
		t.Fatal(err)
	}
	q, err := NewQueue(64<<10, 64<<10)
	if err != nil {
		t.Fatal(err)
	}
	defer q.Close()
	if err := q.EnablePacketTracking(b, 900, 114); err != nil {
		t.Fatal(err)
	}
	now := time.Now()
	if err := q.Enqueue(1, bytes.Repeat([]byte{1}, 64), time.Second); err != nil {
		t.Fatal(err)
	}
	first, ok := q.Next(now, 900)
	if !ok || first.PacketNumber == 0 || b.Used() != PacketStateCharge {
		t.Fatal("first send did not allocate ring", ok, first.PacketNumber, b.Used())
	}
	firstNumber := first.PacketNumber
	if err := q.AckAt(packetReceipt(first), now.Add(time.Millisecond)); err != nil {
		t.Fatal(err)
	}
	if q.Pending() != 0 || q.PacketStats().Outstanding != 0 || b.Used() != PacketStateCharge || q.packets.records == nil {
		t.Fatal("short idle did not retain reusable packet ring", q.Pending(), q.PacketStats(), b.Used())
	}
	// The shared scheduler releases storage after a bounded idle grace, not on
	// every message boundary. This prevents allocation churn on long sessions.
	q.Wake(now.Add(packetStorageIdleGrace + 2*time.Millisecond))
	if b.Used() != 0 || q.packets.records != nil {
		t.Fatal("idle grace did not release packet ring", b.Used())
	}
	before := q.PacketStats()
	if err := q.AckAt(packetReceipt(first), now.Add(packetStorageIdleGrace+3*time.Millisecond)); err != ErrStale {
		t.Fatal("delayed ACK after release had authority", err)
	}
	if q.PacketStats() != before || b.Used() != 0 {
		t.Fatal("stale ACK changed released state", q.PacketStats(), before, b.Used())
	}
	if err := q.Enqueue(2, bytes.Repeat([]byte{2}, 64), time.Second); err != nil {
		t.Fatal(err)
	}
	second, ok := q.Next(now.Add(packetStorageIdleGrace+4*time.Millisecond), 900)
	if !ok || second.PacketNumber <= firstNumber || b.Used() != PacketStateCharge {
		t.Fatal("ring did not reacquire without packet-number reuse", ok, firstNumber, second.PacketNumber, b.Used())
	}
	if err := q.AckAt(packetReceipt(second), now.Add(packetStorageIdleGrace+5*time.Millisecond)); err != nil {
		t.Fatal(err)
	}
	if q.Pending() != 0 || b.Used() != PacketStateCharge || q.packets.records == nil || q.PacketStats().Sent != 2 || q.PacketStats().Receipts != 2 {
		t.Fatal("second short idle unexpectedly freed/stat preservation failed", q.PacketStats(), b.Used())
	}
	q.Wake(now.Add(2*packetStorageIdleGrace + 10*time.Millisecond))
	if b.Used() != 0 || q.packets.records != nil {
		t.Fatal("second idle grace leaked packet storage", b.Used())
	}
}

func TestPacketStorageNotAllocatedWhileWaitingForGrant(t *testing.T) {
	b, err := NewBudget(PacketStateCharge)
	if err != nil {
		t.Fatal(err)
	}
	q, err := NewQueue(64<<10, 64<<10)
	if err != nil {
		t.Fatal(err)
	}
	defer q.Close()
	if err := q.RequireCredits(); err != nil {
		t.Fatal(err)
	}
	if err := q.EnablePacketTracking(b, 900, 114); err != nil {
		t.Fatal(err)
	}
	if err := q.Enqueue(1, bytes.Repeat([]byte{3}, 64), time.Second); err != nil {
		t.Fatal(err)
	}
	now := time.Now()
	f, ok := q.Next(now, 900)
	if !ok || f.Type != TypeOpen {
		t.Fatal("expected admission probe", f, ok)
	}
	if b.Used() != 0 || q.packets.records != nil {
		t.Fatal("waiting admission allocated packet ring", b.Used())
	}
	grant := Frame{Type: TypeGrant, ID: 1, Total: 64}
	if changed, err := q.Grant(grant); err != nil || !changed {
		t.Fatal("grant failed", changed, err)
	}
	f, ok = q.Next(now.Add(time.Millisecond), 900)
	if !ok || f.Type != TypePacketData || b.Used() != PacketStateCharge {
		t.Fatal("granted DATA did not allocate packet ring", f, ok, b.Used())
	}
}
