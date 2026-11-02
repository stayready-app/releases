package reliable

import (
	"bytes"
	"crypto/rand"
	"encoding/binary"
	"testing"
	"time"
)

func TestSelectiveRecoveryWithSizeBlackhole(t *testing.T) {
	data := make([]byte, 16000)
	if _, err := rand.Read(data); err != nil {
		t.Fatal(err)
	}
	sender, err := NewSender(77, data, 10*time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}
	receiver := NewReceiver()
	now := time.Unix(0, 0)
	lost, delivered, retransmitted := 0, 0, 0
	var result []byte
	for step := 0; step < 1000 && !sender.Done(); step++ {
		// Initially choose a blocked physical size, then retry with smaller frames.
		size := 2048
		if step > 10 {
			size = 700
		}
		frame, ok := sender.Next(now, size)
		now = now.Add(11 * time.Millisecond)
		if !ok {
			continue
		}
		if size == 2048 && len(frame.Payload) == 2048 {
			lost++
			continue
		}
		if int(frame.Offset) < sender.next-len(frame.Payload) {
			retransmitted++
		}
		raw, err := Encode(frame)
		if err != nil {
			t.Fatal(err)
		}
		decoded, err := Decode(raw)
		if err != nil {
			t.Fatal(err)
		}
		acks, message, err := receiver.Accept(decoded)
		if err != nil {
			t.Fatal(err)
		}
		if message != nil {
			delivered++
			result = message
		}
		for _, ack := range acks {
			wire, _ := Encode(ack)
			parsed, err := Decode(wire)
			if err != nil || sender.Ack(parsed) != nil {
				t.Fatal("invalid ack")
			}
		}
	}
	if !sender.Done() || delivered != 1 || !bytes.Equal(result, data) || lost == 0 {
		t.Fatalf("delivery failed: done=%v delivered=%d lost=%d retrans=%d", sender.Done(), delivered, lost, retransmitted)
	}
	// Duplicate physical data must not redeliver the logical message.
	acks, duplicate, err := receiver.Accept(Frame{Type: TypeData, ID: 77, Total: uint32(len(data)), Offset: 0, Payload: data[:100]})
	if err != nil || duplicate != nil || len(acks) != 1 {
		t.Fatal("duplicate handling")
	}
}

func TestInvalidFramesAndResourceLimits(t *testing.T) {
	if _, err := Decode([]byte{1}); err == nil {
		t.Fatal("truncated")
	}
	r := NewReceiver()
	for id := uint64(0); id < MaxActiveMessages; id++ {
		if _, _, err := r.Accept(Frame{Type: TypeData, ID: id, Total: 2, Payload: []byte{1}}); err != nil {
			t.Fatal(err)
		}
	}
	if _, _, err := r.Accept(Frame{Type: TypeData, ID: 1000, Total: 2, Payload: []byte{1}}); err == nil {
		t.Fatal("resource cap")
	}
	s, _ := NewSender(1, []byte{1, 2, 3}, time.Second)
	bad := Frame{Type: TypeAck, ID: 1, Total: 3, Offset: 2, Payload: make([]byte, 4)}
	binary.BigEndian.PutUint32(bad.Payload, 2)
	if err := s.Ack(bad); err == nil {
		t.Fatal("invalid ack range")
	}
}
