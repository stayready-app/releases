package reliable

import (
	"bytes"
	"errors"
	"testing"
	"time"
)

func TestRetainedTransferKeepsBudgetAndReusesBuffers(t *testing.T) {
	b, _ := NewBudget(BufferCharge(4096))
	r, _ := NewReceiverWithLimits(LowMemoryReceiverLimits())
	_ = r.SetBudget(b)
	q, _ := NewQueue(1024, 4096)
	_ = q.SetBudget(b)
	payload := bytes.Repeat([]byte{0x39}, 4096)
	_, m, err := r.AcceptRetained(Frame{Type: TypeData, ID: 1, Total: 4096, Payload: payload})
	if err != nil || m == nil {
		t.Fatalf("receive: %v", err)
	}
	address := &m.Data[0]
	bitmap := &m.bitmap[0]
	if b.Used() != BufferCharge(4096) {
		t.Fatal("unaccounted completed payload")
	}
	if err := q.Enqueue(4, payload, time.Second); !errors.Is(err, ErrBudgetExceeded) {
		t.Fatalf("shared cap bypassed: %v", err)
	}
	if err := q.EnqueueReceived(2, m, time.Second); err != nil {
		t.Fatal(err)
	}
	m.Release() // Ownership has moved, must not release twice.
	if b.Used() != BufferCharge(4096) || &q.byID[2].data[0] != address || &q.byID[2].acked[0] != bitmap {
		t.Fatal("copy or lost credit during transfer")
	}
	now := time.Now()
	for q.Pending() != 0 {
		f, ok := q.Next(now, 256)
		if !ok {
			t.Fatal("stalled")
		}
		if err := q.Ack(acknowledgments(f)[0]); err != nil {
			t.Fatal(err)
		}
	}
	if b.Used() != 0 {
		t.Fatal("queue retained credit after final ACK")
	}
	q.Close()
	q.Close()
	r.Close()
	if b.Used() != 0 {
		t.Fatal("double release")
	}
}

func TestQueueCreditReleasedOnCloseAndInvalidEnqueue(t *testing.T) {
	b, _ := NewBudget(1000)
	q, _ := NewQueue(100, 1000)
	_ = q.SetBudget(b)
	if err := q.Enqueue(1, make([]byte, 100), 0); err == nil || b.Used() != 0 {
		t.Fatal("failed allocation leaked")
	}
	if err := q.Enqueue(1, make([]byte, 100), time.Second); err != nil {
		t.Fatal(err)
	}
	if b.Used() != BufferCharge(100) {
		t.Fatal("bitmap not charged")
	}
	q.Close()
	q.Close()
	if b.Used() != 0 {
		t.Fatal("queue close leaked")
	}
}

func TestReceivedReleaseAndBudgetMismatch(t *testing.T) {
	b, _ := NewBudget(1024)
	r, _ := NewReceiverWithLimits(LowMemoryReceiverLimits())
	_ = r.SetBudget(b)
	_, m, _ := r.AcceptRetained(Frame{Type: TypeData, ID: 1, Total: 3, Payload: []byte{1, 2, 3}})
	q, _ := NewQueue(100, 100)
	if q.EnqueueReceived(2, m, time.Second) == nil {
		t.Fatal("cross-budget transfer allowed")
	}
	if b.Used() == 0 {
		t.Fatal("failed transfer released ownership")
	}
	m.Release()
	m.Release()
	if b.Used() != 0 {
		t.Fatal("release leaked")
	}
}

func TestDecodeDoesNotAllocate(t *testing.T) {
	b, _ := Encode(Frame{Type: TypeData, ID: 1, Total: 3, Payload: []byte{1, 2, 3}})
	allocs := testing.AllocsPerRun(100, func() {
		if _, e := Decode(b); e != nil {
			panic(e)
		}
	})
	if allocs != 0 {
		t.Fatalf("decode allocations=%g", allocs)
	}
}

func FuzzDecode(f *testing.F) {
	f.Add([]byte{1})
	b, _ := Encode(Frame{Type: TypeData, ID: 1, Total: 3, Payload: []byte{1, 2, 3}})
	f.Add(b)
	for _, frame := range []Frame{
		{Type: TypeOpen, ID: 1, Total: 32768},
		{Type: TypeGrant, ID: 1, Total: 32768},
		{Type: TypeBlocked, ID: 1, Total: 32768, Offset: 250},
		{Type: TypeReject, ID: 1, Total: 32768, Offset: 1},
		{Type: TypeWindowOpen, ID: 7, Total: 32768},
		{Type: TypeWindowGrant, ID: 7, Total: 32768},
	} {
		encoded, _ := Encode(frame)
		f.Add(encoded)
	}
	padded, _ := EncodeEnvelopeInto(nil, Frame{Type: TypeWindowGrant, ID: 7, Total: 4096}, 64)
	f.Add(padded)
	f.Fuzz(func(t *testing.T, b []byte) {
		frame, err := Decode(b)
		if err != nil {
			return
		}
		encoded, err := Encode(frame)
		canonical, unwrapErr := unwrap(b)
		if err != nil || unwrapErr != nil || !bytes.Equal(encoded, canonical) {
			t.Fatal("canonical codec roundtrip")
		}
	})
}

func TestQueueRetriesUseDifferentPackingButFreshDataDoesNot(t *testing.T) {
	q, _ := NewQueue(1000, 1000)
	_ = q.Enqueue(1, make([]byte, 1000), time.Second)
	now := time.Now()
	f, ok := q.NextSized(now, 900, 350)
	if !ok || len(f.Payload) != 900 {
		t.Fatal("initial packing")
	}
	f, ok = q.NextSized(now.Add(2*time.Second), 900, 350)
	if !ok || f.Offset != 0 || len(f.Payload) != 350 {
		t.Fatal("retry packing")
	}
}

func TestSparseACKsDoNotExplodeRetryMetadata(t *testing.T) {
	s, _ := NewSender(1, make([]byte, 32768), time.Second)
	now := time.Now()
	for s.next < len(s.data) {
		if _, ok := s.Next(now, 900); !ok {
			t.Fatal("initial send stalled")
		}
	}
	for offset := 0; offset < len(s.data); offset += 2 {
		if err := s.Ack(Frame{Type: TypeAck, ID: 1, Offset: uint32(offset), Total: uint32(len(s.data)), Payload: []byte{0, 0, 0, 1}}); err != nil {
			t.Fatal(err)
		}
	}
	for n := 0; n < 20000; n++ {
		if _, ok := s.NextRetry(now.Add(2*time.Second), 350); !ok {
			break
		}
		if len(s.ranges) > 256 {
			t.Fatalf("ACK holes inflated retry state to %d ranges", len(s.ranges))
		}
	}
	if s.InFlight() != 16384 {
		t.Fatal("retry double-counted payload")
	}
}
