package emulator

import (
	"reflect"
	"testing"
)

func TestDeterministic(t *testing.T) {
	cfg := Config{LossPercent: 10, DuplicatePercent: 20, CorruptPercent: 10, DelayMinMs: 1, DelayMaxMs: 50}
	run := func() ([]Event, Stats) {
		e, err := New(5, cfg, Config{})
		if err != nil {
			t.Fatal(err)
		}
		for i := 0; i < 100; i++ {
			_ = e.Submit(ClientToServer, []byte{byte(i), 9})
		}
		return e.Drain(), e.Stats(ClientToServer)
	}
	a, as := run()
	b, bs := run()
	if !reflect.DeepEqual(a, b) || as != bs {
		t.Fatal("not deterministic")
	}
}
func TestFilterAndTime(t *testing.T) {
	e, _ := New(7, Config{DropSizes: []SizeRange{{Min: 5, Max: 10}}, DelayMinMs: 10, DelayMaxMs: 10}, Config{})
	_ = e.Submit(ClientToServer, make([]byte, 7))
	_ = e.Submit(ClientToServer, []byte{1, 2})
	early, _ := e.AdvanceTo(9)
	if len(early) != 0 {
		t.Fatal("early")
	}
	due, _ := e.AdvanceTo(10)
	if len(due) != 1 || e.Stats(ClientToServer).Dropped != 1 {
		t.Fatal("filter")
	}
	if _, err := e.AdvanceTo(9); err == nil {
		t.Fatal("time reversal")
	}
}
func TestInvalidConfig(t *testing.T) {
	if _, err := New(1, Config{LossPercent: 101}, Config{}); err == nil {
		t.Fatal("invalid accepted")
	}
}
func TestBothDirections(t *testing.T) {
	e, _ := New(1, Config{}, Config{})
	_ = e.Submit(ClientToServer, []byte("a"))
	_ = e.Submit(ServerToClient, []byte("b"))
	if len(e.Drain()) != 2 || e.Stats(ServerToClient).Delivered != 1 {
		t.Fatal("direction lost")
	}
}
