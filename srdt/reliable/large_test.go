package reliable

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"testing"
	"time"
)

// A deterministic 100 MiB logical transfer with systematic first-attempt
// loss. The retransmission is repacked into smaller fragments.
func TestEightMiBDeterministic(t *testing.T) {
	const total = 8 << 20
	const chunk = 1 << 20
	now := time.Unix(100, 0)
	receiver := NewReceiver()
	for index := 0; index < total/chunk; index++ {
		payload := make([]byte, chunk)
		for i := 0; i < len(payload); i += 8 {
			binary.LittleEndian.PutUint64(payload[i:], uint64(index)*uint64(chunk)+uint64(i))
		}
		expected := sha256.Sum256(payload)
		sender, err := NewSender(uint64(index+1), payload, 2*time.Millisecond)
		if err != nil {
			t.Fatal(err)
		}
		var delivered []byte
		iterations := 0
		for !sender.Done() || delivered == nil {
			iterations++
			if iterations > 12000 {
				t.Fatalf("message %d stalled", index)
			}
			size := 4096
			if iterations > 300 {
				size = 768
			}
			frame, ok := sender.Next(now, size)
			if !ok {
				now = now.Add(2 * time.Millisecond)
				continue
			}
			// Drop every initial large fragment: only smaller repackaging passes.
			if len(frame.Payload) > 1000 {
				now = now.Add(10 * time.Microsecond)
				continue
			}
			encoded, err := Encode(frame)
			if err != nil {
				t.Fatal(err)
			}
			decoded, err := Decode(encoded)
			if err != nil {
				t.Fatal(err)
			}
			acks, result, err := receiver.Accept(decoded)
			if err != nil {
				t.Fatal(err)
			}
			if result != nil {
				delivered = result
			}
			for _, ack := range acks {
				if err := sender.Ack(ack); err != nil {
					t.Fatal(err)
				}
			}
			now = now.Add(10 * time.Microsecond)
		}
		if !bytes.Equal(payload, delivered) || sha256.Sum256(delivered) != expected {
			t.Fatalf("message %d corrupted", index)
		}
	}
}
