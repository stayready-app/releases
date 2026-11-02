package emulator

import (
	"math"
	"testing"
	"time"
)

func TestLinkFIFOQueueDropsAndDrains(t *testing.T) {
	l, err := NewLink(1000, 2000)
	if err != nil {
		t.Fatal(err)
	}
	a, ok := l.Schedule(0, 1000)
	if !ok || a != time.Second {
		t.Fatal(a, ok)
	}
	b, ok := l.Schedule(0, 1000)
	if !ok || b != 2*time.Second {
		t.Fatal(b, ok)
	}
	if _, ok = l.Schedule(0, 1); ok {
		t.Fatal("unbounded queue")
	}
	c, ok := l.Schedule(time.Second, 1000)
	if !ok || c != 3*time.Second {
		t.Fatal(c, ok)
	}
	drops, peak := l.Stats()
	if drops != 1 || peak != 2000 {
		t.Fatal(drops, peak)
	}
	if _, ok = l.Schedule(time.Duration(math.MaxInt64), 1000); ok {
		t.Fatal("overflow")
	}
}
func TestLinkDirectionsAndReproducibility(t *testing.T) {
	c := Config{RateBytesPerSecond: 1000, BottleneckBytes: 2000, DelayMinMs: 10, DelayMaxMs: 10}
	e, err := New(1, c, c)
	if err != nil {
		t.Fatal(err)
	}
	for n := 0; n < 3; n++ {
		if err = e.Submit(ClientToServer, make([]byte, 1000)); err != nil {
			t.Fatal(err)
		}
	}
	_ = e.Submit(ServerToClient, make([]byte, 1000))
	if e.Stats(ClientToServer).LinkDrops != 1 {
		t.Fatal("drop not recorded")
	}
	events, err := e.AdvanceTo(1010)
	if err != nil || len(events) != 2 {
		t.Fatal(len(events), err)
	}
	if len(e.Drain()) != 1 {
		t.Fatal("FIFO delivery lost")
	}
	for _, bad := range []Config{{RateBytesPerSecond: 100}, {BottleneckBytes: 10}, {RateBytesPerSecond: -1, BottleneckBytes: 10}} {
		if _, err := New(1, bad, Config{}); err == nil {
			t.Fatal("invalid accepted")
		}
	}
}
func TestLinkNoAllocations(t *testing.T) {
	l, _ := NewLink(1000000, 8192)
	now := time.Duration(0)
	n := testing.AllocsPerRun(1000, func() { now += time.Millisecond; l.Schedule(now, 900) })
	if n != 0 {
		t.Fatal(n)
	}
}
