package reliable

import (
	"errors"
	"sync"
	"testing"
)

func TestSharedBudgetAndClose(t *testing.T) {
	b, _ := NewBudget(BufferCharge(60))
	a, _ := NewReceiverWithLimits(LowMemoryReceiverLimits())
	c, _ := NewReceiverWithLimits(LowMemoryReceiverLimits())
	if err := a.SetBudget(b); err != nil {
		t.Fatal(err)
	}
	if err := c.SetBudget(b); err != nil {
		t.Fatal(err)
	}
	f := func(id uint64, total uint32) Frame {
		return Frame{Type: TypeData, ID: id, Total: total, Payload: []byte{42}}
	}
	if _, _, err := a.Accept(f(1, 60)); err != nil {
		t.Fatal(err)
	}
	if _, _, err := c.Accept(f(2, 60)); !errors.Is(err, ErrBudgetExceeded) {
		t.Fatalf("want budget rejection: %v", err)
	}
	if b.Used() != BufferCharge(60) {
		t.Fatalf("used=%d", b.Used())
	}
	a.Close()
	if b.Used() != 0 {
		t.Fatalf("leaked reservation: %d", b.Used())
	}
	if _, _, err := c.Accept(f(2, 60)); err != nil {
		t.Fatal(err)
	}
	c.Close()
	if b.Used() != 0 {
		t.Fatal("budget leak")
	}
}

func TestBudgetConcurrentReservation(t *testing.T) {
	b, _ := NewBudget(1024)
	var wg sync.WaitGroup
	for i := 0; i < 32; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 1000; j++ {
				if b.reserve(32) {
					b.release(32)
				}
			}
		}()
	}
	wg.Wait()
	if b.Used() != 0 {
		t.Fatalf("budget leak %d", b.Used())
	}
}

func TestBudgetReleaseAfterCompletion(t *testing.T) {
	b, _ := NewBudget(BufferCharge(4))
	r, _ := NewReceiverWithLimits(LowMemoryReceiverLimits())
	_ = r.SetBudget(b)
	f := Frame{Type: TypeData, ID: 1, Total: 4, Payload: []byte{1, 2, 3, 4}}
	_, data, err := r.Accept(f)
	if err != nil || len(data) != 4 || b.Used() != 0 {
		t.Fatalf("err=%v used=%d", err, b.Used())
	}
	r.Close()
}
