package emulator

import (
	"math"
	"testing"
)

func TestAuditQueueBudgetAndReleasedSlots(t *testing.T) {
	e, _ := NewWithLimits(1, Config{DelayMaxMs: 10}, Config{}, Limits{2, 16})
	if e.Submit(ClientToServer, make([]byte, 8)) != nil || e.Submit(ClientToServer, make([]byte, 8)) != nil {
		t.Fatal("initial")
	}
	if e.Submit(ClientToServer, []byte{1}) != ErrQueueFull {
		t.Fatal("unbounded queue")
	}
	_ = e.Drain()
	n, b := e.Pending()
	if n != 0 || b != 0 {
		t.Fatal("pending credit")
	}
	for _, v := range e.pending[:cap(e.pending)] {
		if v.Packet.Data != nil {
			t.Fatal("popped heap retains payload")
		}
	}
	if e.Submit(ClientToServer, []byte{1}) != nil {
		t.Fatal("credit not reusable")
	}
}
func TestAuditInvalidFloatingPointAndClock(t *testing.T) {
	for _, c := range []Config{{LossPercent: math.NaN()}, {DuplicatePercent: math.NaN()}, {CorruptPercent: math.NaN()}, {DelayMaxMs: math.MaxInt64}} {
		if _, err := New(1, c, Config{}); err == nil {
			t.Fatal("invalid config")
		}
	}
	e, _ := New(1, Config{DelayMaxMs: 10}, Config{})
	_, _ = e.AdvanceTo(math.MaxInt64)
	if err := e.Submit(ClientToServer, []byte{1}); err == nil {
		t.Fatal("clock overflow")
	}
}
