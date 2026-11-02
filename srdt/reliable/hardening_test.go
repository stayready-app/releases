package reliable

import (
	"encoding/binary"
	"testing"
	"time"
)

func TestRejectAckOfUnsentBytes(t *testing.T) {
	s, err := NewSender(99, make([]byte, 1000), time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := s.Next(time.Now(), 100); !ok {
		t.Fatal("no frame")
	}
	b := make([]byte, 4)
	binary.BigEndian.PutUint32(b, 100)
	if err := s.Ack(Frame{Type: TypeAck, ID: 99, Offset: 100, Total: 1000, Payload: b}); err == nil {
		t.Fatal("unsent ACK accepted")
	}
	if s.InFlight() != 100 {
		t.Fatalf("in flight changed: %d", s.InFlight())
	}
	if err := s.Ack(Frame{Type: TypeAck, ID: 99, Offset: 0, Total: 1000, Payload: b}); err != nil {
		t.Fatal(err)
	}
}

func TestReceiverBoundedActiveBytes(t *testing.T) {
	r := NewReceiver()
	for id := uint64(1); id <= 4; id++ {
		_, _, err := r.Accept(Frame{Type: TypeData, ID: id, Total: MaxMessageSize, Payload: []byte{1}})
		if err != nil {
			t.Fatal(err)
		}
	}
	if _, _, err := r.Accept(Frame{Type: TypeData, ID: 5, Total: MaxMessageSize, Payload: []byte{1}}); err == nil {
		t.Fatal("memory limit ignored")
	}
	if r.activeBytes != MaxActiveBytes {
		t.Fatal("wrong active byte count")
	}
}

func TestReceiverBoundedCompletedCache(t *testing.T) {
	r := NewReceiver()
	for id := uint64(1); id <= MaxCompletedMessages+50; id++ {
		_, data, err := r.Accept(Frame{Type: TypeData, ID: id, Total: 1, Payload: []byte{byte(id)}})
		if err != nil || len(data) != 1 {
			t.Fatalf("id %d: %v", id, err)
		}
	}
	if len(r.completed) != MaxCompletedMessages || len(r.completedOrder) != MaxCompletedMessages {
		t.Fatal("completed cache unbounded")
	}
	if _, exists := r.completed[1]; exists {
		t.Fatal("oldest entry not evicted")
	}
}
