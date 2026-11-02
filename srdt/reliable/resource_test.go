package reliable

import (
	"encoding/binary"
	"testing"
	"time"
)

func TestLowMemoryReceiverBudget(t *testing.T) {
	r, err := NewReceiverWithLimits(LowMemoryReceiverLimits())
	if err != nil {
		t.Fatal(err)
	}
	for i := uint64(1); i <= 8; i++ {
		_, _, err = r.Accept(Frame{Type: TypeData, ID: i, Total: 32 << 10, Payload: []byte{1}})
		if err != nil {
			t.Fatal(err)
		}
	}
	if r.ActiveBytes() != 256<<10 {
		t.Fatalf("active bytes %d", r.ActiveBytes())
	}
	if _, _, err = r.Accept(Frame{Type: TypeData, ID: 9, Total: 1, Payload: []byte{1}}); err == nil {
		t.Fatal("budget bypass")
	}
}

func TestQueueInFlightAccounting(t *testing.T) {
	q, err := NewQueue(100, 1000)
	if err != nil {
		t.Fatal(err)
	}
	for i := uint64(1); i <= 3; i++ {
		if err := q.Enqueue(i, make([]byte, 200), time.Millisecond); err != nil {
			t.Fatal(err)
		}
	}
	now := time.Now()
	for i := 0; i < 4; i++ {
		q.Next(now, 50)
	}
	if q.InFlight() != 100 {
		t.Fatalf("inflight=%d", q.InFlight())
	}
	f := Frame{Type: TypeAck, ID: 1, Total: 200, Offset: 0, Payload: make([]byte, 4)}
	binary.BigEndian.PutUint32(f.Payload, 50)
	if err := q.Ack(f); err != nil {
		t.Fatal(err)
	}
	if q.InFlight() != 50 {
		t.Fatalf("after ack=%d", q.InFlight())
	}
	if err := q.Ack(f); err != nil {
		t.Fatal(err)
	}
	if q.InFlight() != 50 {
		t.Fatal("duplicate ACK changed in-flight")
	}
}
