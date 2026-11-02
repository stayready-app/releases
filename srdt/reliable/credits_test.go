package reliable

import (
	"bytes"
	"errors"
	"testing"
	"time"
)

func TestCreditsFrameValidation(t *testing.T) {
	for _, f := range []Frame{
		{Type: TypeOpen, ID: 1, Total: 1024}, {Type: TypeGrant, ID: 1, Total: 1024},
		{Type: TypeBlocked, ID: 1, Total: 1024, Offset: 250}, {Type: TypeReject, ID: 1, Total: 1024, Offset: 1},
	} {
		encoded, err := Encode(f)
		if err != nil {
			t.Fatal(err)
		}
		got, err := Decode(encoded)
		if err != nil || got.Type != f.Type || got.Total != f.Total || got.ID != f.ID || got.Offset != f.Offset {
			t.Fatal(got, err)
		}
		if len(encoded) != HeaderSize {
			t.Fatal("control grew a payload")
		}
		f.Payload = []byte{1}
		if _, err := Encode(f); err == nil {
			t.Fatal("control payload accepted")
		}
	}
	for _, f := range []Frame{
		{Type: TypeOpen, Total: 1}, {Type: TypeGrant, ID: 1, Total: 0},
		{Type: TypeGrant, ID: 1, Total: 10, Offset: 1},
		{Type: TypeBlocked, ID: 1, Total: 100, Offset: 99},
		{Type: TypeBlocked, ID: 1, Total: 100, Offset: 1001},
		{Type: TypeReject, ID: 1, Total: 1, Offset: 0},
	} {
		if _, err := Encode(f); err == nil {
			t.Fatal("invalid control accepted", f)
		}
	}
}

func TestCreditsLazyReservationIdempotenceAndTransfer(t *testing.T) {
	const size = 32768
	budget, _ := NewBudget(BufferCharge(size))
	r := NewReceiver()
	_ = r.SetBudget(budget)
	if err := r.Reserve(1, size); err != nil {
		t.Fatal(err)
	}
	if budget.Used() != BufferCharge(size) || r.ActiveBytes() != size || r.AcceptedBytes() != 0 {
		t.Fatal("incorrect reservation")
	}
	if r.active[1].data != nil || r.active[1].got != nil {
		t.Fatal("OPEN eagerly allocated payload")
	}
	for n := 0; n < 100; n++ {
		if err := r.Reserve(1, size); err != nil {
			t.Fatal(err)
		}
	}
	if budget.Used() != BufferCharge(size) {
		t.Fatal("duplicate reserve double-charged")
	}
	if err := r.Reserve(1, size-1); err == nil {
		t.Fatal("length changed")
	}
	if err := r.Reserve(2, 1); !errors.Is(err, ErrBudgetExceeded) {
		t.Fatal("global cap ignored", err)
	}
	payload := bytes.Repeat([]byte{0xa5}, size)
	_, received, err := r.AcceptRetained(Frame{Type: TypeData, ID: 1, Total: size, Payload: payload})
	if err != nil || received == nil || !bytes.Equal(received.Data, payload) {
		t.Fatal("reserved receive failed", err)
	}
	q, _ := NewQueue(size, size)
	_ = q.SetBudget(budget)
	_ = q.RequireCredits()
	if err := q.EnqueueReceived(2, received, time.Second); err != nil {
		t.Fatal(err)
	}
	if budget.Used() != BufferCharge(size) || r.ActiveBytes() != 0 {
		t.Fatal("transfer lost/doubled credit")
	}
	if err := r.Reserve(1, size); err != nil {
		t.Fatal("duplicate completed reserve", err)
	}
	if r.ActiveBytes() != 0 {
		t.Fatal("completed message allocated again")
	}
	r.Close()
	q.Close()
	r.Close()
	q.Close()
	if budget.Used() != 0 {
		t.Fatal("credit leak/underflow")
	}
}

func TestCreditsBlockedQueueAndReorderedControls(t *testing.T) {
	q, _ := NewQueue(100, 100)
	_ = q.RequireCredits()
	_ = q.Enqueue(1, make([]byte, 100), time.Millisecond)
	now := time.Unix(1, 0)
	grant := Frame{Type: TypeGrant, ID: 1, Total: 100}
	if _, err := q.Grant(grant); err == nil {
		t.Fatal("grant before OPEN accepted")
	}
	f, ok := q.NextSized(now, 100, 30)
	if !ok || f.Type != TypeOpen || q.InFlight() != 0 {
		t.Fatal("data before admission", f)
	}
	for n := 0; n < 100; n++ {
		if _, ok := q.Next(now.Add(time.Millisecond), 100); ok {
			t.Fatal("unbounded OPEN probing")
		}
	}
	if _, err := q.Grant(Frame{Type: TypeGrant, ID: 1, Total: 99}); err == nil {
		t.Fatal("wrong-length grant accepted")
	}
	blocked := Frame{Type: TypeBlocked, ID: 1, Total: 100, Offset: 250}
	if err := q.Blocked(blocked, now); err != nil {
		t.Fatal(err)
	}
	if _, ok := q.Next(now.Add(200*time.Millisecond), 100); ok {
		t.Fatal("blocked timer ignored")
	}
	if changed, err := q.Grant(grant); err != nil || !changed {
		t.Fatal(changed, err)
	}
	if changed, err := q.Grant(grant); err != nil || changed {
		t.Fatal("duplicate grant changed progress")
	}
	_ = q.Blocked(blocked, now.Add(time.Millisecond))
	f, ok = q.Next(now.Add(time.Millisecond), 100)
	if !ok || f.Type != TypeData || q.InFlight() != 100 {
		t.Fatal("delayed BLOCKED revoked permission")
	}
	f, ok = q.NextSized(now.Add(10*time.Millisecond), 100, 30)
	if !ok || f.Type != TypeData || len(f.Payload) != 30 || q.InFlight() != 100 {
		t.Fatal("retry lost credit")
	}
}

func TestCreditsProbeBackoffBoundedAndNoPayload(t *testing.T) {
	q, _ := NewQueue(100, 100)
	_ = q.RequireCredits()
	_ = q.Enqueue(1, make([]byte, 100), time.Millisecond)
	start := time.Unix(1, 0)
	count := 0
	for n := 0; n < 10000; n++ {
		if f, ok := q.Next(start.Add(time.Duration(n)*time.Millisecond), 100); ok {
			if f.Type != TypeOpen || len(f.Payload) != 0 {
				t.Fatal("blocked queue sent DATA")
			}
			count++
		}
	}
	if count < 10 || count > 14 || q.InFlight() != 0 {
		t.Fatalf("probes=%d", count)
	}
	q.Close()
}

func TestCreditsReservationExpiryAndGlobalContention(t *testing.T) {
	b, _ := NewBudget(BufferCharge(100))
	a, c := NewReceiver(), NewReceiver()
	_ = a.SetBudget(b)
	_ = c.SetBudget(b)
	if err := a.Reserve(1, 100); err != nil {
		t.Fatal(err)
	}
	if err := c.Reserve(1, 100); !errors.Is(err, ErrBudgetExceeded) {
		t.Fatal(err)
	}
	a.Close()
	if err := c.Reserve(1, 100); err != nil {
		t.Fatal("reservation not released", err)
	}
	c.Close()
	if b.Used() != 0 {
		t.Fatal("reservation leak")
	}
}

func TestCreditsStrictReceiverAndZeroSenderID(t *testing.T) {
	r := NewReceiver()
	if err := r.RequireCredits(); err != nil {
		t.Fatal(err)
	}
	b, _ := NewBudget(BufferCharge(100))
	_ = r.SetBudget(b)
	f := Frame{Type: TypeData, ID: 1, Total: 100, Payload: make([]byte, 100)}
	if _, _, err := r.Accept(f); !errors.Is(err, ErrCreditRequired) || b.Used() != 0 {
		t.Fatal("implicit admission in strict receiver", err)
	}
	if err := r.Reserve(1, 100); err != nil {
		t.Fatal(err)
	}
	if _, data, err := r.Accept(f); err != nil || len(data) != 100 {
		t.Fatal("reserved strict receive", err)
	}
	q, _ := NewQueue(100, 100)
	_ = q.RequireCredits()
	if err := q.Enqueue(0, []byte{1}, time.Second); err == nil || q.Pending() != 0 {
		t.Fatal("invalid zero-ID entered credit queue")
	}
	r.Close()
	if err := r.RequireCredits(); err == nil {
		t.Fatal("closed receiver reused")
	}
}
