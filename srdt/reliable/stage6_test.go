package reliable

import (
	"crypto/sha256"
	"encoding/binary"
	"testing"
	"time"
)

func TestHundredMiBRangeRecovery(t *testing.T) {
	const total = 100 << 20
	const chunk = 4 << 20
	now := time.Unix(100, 0)
	receiver := NewReceiver()
	for index := 0; index < total/chunk; index++ {
		payload := make([]byte, chunk)
		for i := 0; i < len(payload); i += 8 {
			binary.LittleEndian.PutUint64(payload[i:], uint64(index)*chunk+uint64(i))
		}
		expected := sha256.Sum256(payload)
		sender, err := NewSender(uint64(index+1), payload, time.Millisecond)
		if err != nil {
			t.Fatal(err)
		}
		var result []byte
		for steps := 0; steps < 30000 && (!sender.Done() || result == nil); steps++ {
			size := 4096
			if sender.next == len(payload) {
				size = 768
			}
			frame, ok := sender.Next(now, size)
			now = now.Add(time.Millisecond)
			if !ok {
				continue
			}
			if len(frame.Payload) > 1000 {
				continue
			}
			acks, delivered, err := receiver.Accept(frame)
			if err != nil {
				t.Fatal(err)
			}
			if delivered != nil {
				result = delivered
			}
			for _, ack := range acks {
				if err := sender.Ack(ack); err != nil {
					t.Fatal(err)
				}
			}
		}
		if !sender.Done() || result == nil || sha256.Sum256(result) != expected {
			t.Fatalf("message %d failed", index)
		}
	}
}
