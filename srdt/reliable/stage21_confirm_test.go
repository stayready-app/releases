package reliable

import (
	"testing"
	"time"
)

func TestStage21ApplicationConfirmationRetiresPacketStateWithoutReceipt(t *testing.T) {
	q, err := NewQueue(4096, 4096)
	if err != nil {
		t.Fatal(err)
	}
	defer q.Close()
	if err := q.EnableAdaptiveRTO(); err != nil {
		t.Fatal(err)
	}
	if err := q.EnableCongestionControl(900, 106); err != nil {
		t.Fatal(err)
	}
	if err := q.EnablePacketTracking(nil, 900, 114); err != nil {
		t.Fatal(err)
	}
	if err := q.Enqueue(1, make([]byte, 2048), 80*time.Millisecond); err != nil {
		t.Fatal(err)
	}
	now := time.Now()
	for n := 0; n < 4; n++ {
		if _, ok := q.NextSized(now, 900, 350); !ok {
			break
		}
	}
	before := q.PacketStats()
	if before.Outstanding == 0 || q.Pending() != 1 {
		t.Fatalf("test did not create packet state: %+v pending=%d", before, q.Pending())
	}
	if err := q.ConfirmDelivered(1, now.Add(time.Millisecond)); err != nil {
		t.Fatal(err)
	}
	after := q.PacketStats()
	if q.Pending() != 0 || q.InFlight() != 0 || after.WireInFlight != 0 || after.Outstanding != 0 {
		t.Fatalf("application confirmation left state: %+v pending=%d inflight=%d", after, q.Pending(), q.InFlight())
	}
	if after.Receipts != before.Receipts {
		t.Fatal("application proof fabricated packet receipt")
	}
}
