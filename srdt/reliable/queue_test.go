package reliable

import (
	"bytes"
	"encoding/binary"
	"testing"
	"time"
)

func TestQueueMultiMessageWindowAndLoss(t *testing.T) {
	q, err := NewQueue(1024, 64<<10)
	if err != nil {
		t.Fatal(err)
	}
	r := NewReceiver()
	const messages = 12
	for id := uint64(1); id <= messages; id++ {
		if err := q.Enqueue(id, bytes.Repeat([]byte{byte(id)}, 4096), time.Millisecond); err != nil {
			t.Fatal(err)
		}
	}
	if err := q.Enqueue(1, []byte{1}, time.Millisecond); err == nil {
		t.Fatal("duplicate ID accepted")
	}
	if err := q.Enqueue(100, make([]byte, 40<<10), time.Millisecond); err == nil {
		t.Fatal("queue cap ignored")
	}
	now := time.Unix(10, 0)
	delivered := make(map[uint64]bool)
	dropped := 0
	for step := 0; step < 300000 && q.Pending() > 0; step++ {
		size := 900
		if step > 40 {
			size = 300
		}
		f, ok := q.Next(now, size)
		now = now.Add(2 * time.Millisecond)
		if !ok {
			continue
		}
		if q.InFlight() > 1024 {
			t.Fatalf("window exceeded: %d", q.InFlight())
		}
		if len(f.Payload) > 500 {
			dropped++
			continue
		}
		acks, data, err := r.Accept(f)
		if err != nil {
			t.Fatal(err)
		}
		if data != nil {
			if delivered[f.ID] || !bytes.Equal(data, bytes.Repeat([]byte{byte(f.ID)}, 4096)) {
				t.Fatal("duplicate/corruption")
			}
			delivered[f.ID] = true
		}
		for _, ack := range acks {
			if err := q.Ack(ack); err != nil {
				t.Fatal(err)
			}
		}
	}
	if q.Pending() != 0 || q.QueuedBytes() != 0 || len(delivered) != messages || dropped == 0 {
		t.Fatalf("pending=%d delivered=%d dropped=%d", q.Pending(), len(delivered), dropped)
	}
}

func TestQueueFullWindowRetriesAndBadAck(t *testing.T) {
	q, _ := NewQueue(32, 100)
	_ = q.Enqueue(1, make([]byte, 64), time.Millisecond)
	now := time.Unix(1, 0)
	f, ok := q.Next(now, 100)
	if !ok || len(f.Payload) != 32 {
		t.Fatal("window")
	}
	if _, ok = q.Next(now, 100); ok {
		t.Fatal("window bypass")
	}
	retry, ok := q.Next(now.Add(2*time.Millisecond), 16)
	if !ok || len(retry.Payload) != 16 {
		t.Fatal("retry blocked")
	}
	bad := Frame{Type: TypeAck, ID: 99, Total: 64, Payload: make([]byte, 4)}
	binary.BigEndian.PutUint32(bad.Payload, 1)
	if q.Ack(bad) == nil {
		t.Fatal("unknown ack accepted")
	}
}
