package reliable

import (
	"encoding/binary"
	"testing"
	"time"
)

func TestWindowLimitsOutstandingDataAndAllowsRetries(t *testing.T) {
	now := time.Unix(100, 0)
	s, err := NewSender(77, make([]byte, 1000), 50*time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}
	if err := s.SetWindow(-1); err == nil {
		t.Fatal("negative window accepted")
	}
	if err := s.SetWindow(128); err != nil {
		t.Fatal(err)
	}
	f, ok := s.Next(now, 900)
	if !ok || len(f.Payload) != 128 || s.InFlight() != 128 {
		t.Fatalf("unexpected initial frame: %+v", f)
	}
	if _, ok := s.Next(now, 900); ok {
		t.Fatal("sent new bytes past window")
	}
	retry, ok := s.Next(now.Add(60*time.Millisecond), 40)
	if !ok || retry.Offset != 0 || len(retry.Payload) != 40 {
		t.Fatalf("retry blocked by window: %+v", retry)
	}
	ack := Frame{Type: TypeAck, ID: 77, Offset: 0, Total: 1000, Payload: make([]byte, 4)}
	binary.BigEndian.PutUint32(ack.Payload, 64)
	if err := s.Ack(ack); err != nil {
		t.Fatal(err)
	}
	if s.InFlight() != 64 {
		t.Fatalf("in flight=%d", s.InFlight())
	}
	f, ok = s.Next(now.Add(61*time.Millisecond), 900)
	if !ok || f.Offset != 128 || len(f.Payload) != 64 {
		t.Fatalf("window not reopened: %+v", f)
	}
	if err := s.Ack(ack); err != nil {
		t.Fatal(err)
	}
	if s.InFlight() != 128 {
		t.Fatalf("duplicate ack changed window: %d", s.InFlight())
	}
}

func TestWindowBoundedDelivery(t *testing.T) {
	s, err := NewSender(1, make([]byte, 32768), time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}
	if err := s.SetWindow(1024); err != nil {
		t.Fatal(err)
	}
	r := NewReceiver()
	now := time.Unix(200, 0)
	delivered := 0
	for i := 0; i < 10000 && !s.Done(); i++ {
		f, ok := s.Next(now, 768)
		if !ok {
			t.Fatal("unexpected stall")
		}
		acks, data, err := r.Accept(f)
		if err != nil {
			t.Fatal(err)
		}
		for _, a := range acks {
			if err := s.Ack(a); err != nil {
				t.Fatal(err)
			}
		}
		if data != nil {
			delivered++
		}
		if s.InFlight() > 1024 {
			t.Fatal("window exceeded")
		}
		now = now.Add(time.Millisecond)
	}
	if !s.Done() || delivered != 1 {
		t.Fatalf("done=%t deliveries=%d", s.Done(), delivered)
	}
}
