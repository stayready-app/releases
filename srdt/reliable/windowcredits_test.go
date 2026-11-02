package reliable

import (
	"bytes"
	"encoding/binary"
	"errors"
	"testing"
	"time"
	"unsafe"
)

func TestWindowCumulativeGrantsCannotMintCredit(t *testing.T) {
	q, _ := NewQueue(1024, 4096)
	if err := q.UseWindowCredits(1, 2, 1024); err != nil {
		t.Fatal(err)
	}
	if err := q.RequestWindow(7); err != nil {
		t.Fatal(err)
	}
	grant := Frame{Type: TypeWindowGrant, ID: 7, Total: 1024}
	if _, err := q.GrantWindow(grant); err == nil {
		t.Fatal("unsolicited grant")
	}
	now := time.Now()
	f, ok := q.Next(now, 900)
	if !ok || f.Type != TypeWindowOpen || f.ID != 7 {
		t.Fatal("missing demand")
	}
	bad := grant
	bad.ID = 9
	if _, err := q.GrantWindow(bad); err == nil {
		t.Fatal("unrequested ID")
	}
	bad = grant
	bad.Total++
	if _, err := q.GrantWindow(bad); err == nil {
		t.Fatal("changed capacity")
	}
	bad = grant
	bad.ID = 6
	if _, err := q.GrantWindow(bad); err == nil {
		t.Fatal("wrong ID domain")
	}
	if changed, err := q.GrantWindow(grant); err != nil || !changed {
		t.Fatal(err)
	}
	for i := 0; i < 100; i++ {
		grant.ID = 3
		if changed, err := q.GrantWindow(grant); err != nil || changed {
			t.Fatal("replay increased/revoked credit")
		}
	}
	if err := q.Enqueue(7, []byte("already reserved"), time.Second); err != nil {
		t.Fatal(err)
	}
	f, ok = q.Next(now, 900)
	if !ok || f.Type != TypeData || f.ID != 7 {
		t.Fatal("unexpected per-message round trip")
	}
	if _, err := q.Grant(Frame{Type: TypeGrant, ID: 7, Total: f.Total}); err == nil {
		t.Fatal("mixed credit modes")
	}
	if err := q.Enqueue(8, []byte{1}, time.Second); err == nil {
		t.Fatal("invalid domain")
	}
	if err := q.Enqueue(9, make([]byte, 1025), time.Second); err == nil {
		t.Fatal("oversized credit payload")
	}
}

func TestWindowCapacityBindingAndOwnership(t *testing.T) {
	b, _ := NewBudget(BufferCharge(4096))
	r, _ := NewReceiverWithLimits(ReceiverLimits{2, 4096, 4})
	_ = r.RequireCredits()
	_ = r.SetBudget(b)
	if err := r.ReserveCapacity(1, 4096); err != nil {
		t.Fatal(err)
	}
	if r.active[1].data != nil || b.Used() != BufferCharge(4096) {
		t.Fatal("preallocation/under-reservation")
	}
	f := Frame{Type: TypeData, ID: 1, Total: 4097, Payload: []byte{1}}
	if _, _, err := r.AcceptRetained(f); err == nil || b.Used() != BufferCharge(4096) {
		t.Fatal("oversized DATA changed state")
	}
	f.Total, f.Offset = 64, 64
	if _, _, err := r.AcceptRetained(f); err == nil || b.Used() != BufferCharge(4096) {
		t.Fatal("invalid DATA bound length")
	}
	f.Offset = 0
	f.Payload = bytes.Repeat([]byte{7}, 32)
	if _, m, err := r.AcceptRetained(f); err != nil || m != nil {
		t.Fatal(err)
	}
	if b.Used() != BufferCharge(64) || r.ActiveBytes() != 64 {
		t.Fatal("bound length not accounted")
	}
	f.Total = 65
	if _, _, err := r.AcceptRetained(f); err == nil || b.Used() != BufferCharge(64) {
		t.Fatal("second length binding")
	}
	f.Total, f.Offset = 64, 32
	_, m, err := r.AcceptRetained(f)
	if err != nil || m == nil {
		t.Fatal(err)
	}
	q, _ := NewQueue(4096, 4096)
	_ = q.SetBudget(b)
	if err := q.EnqueueReceived(2, m, time.Second); err != nil {
		t.Fatal(err)
	}
	if b.Used() != BufferCharge(64) {
		t.Fatal("transfer broke reservation")
	}
	q.Close()
	r.Close()
	if b.Used() != 0 {
		t.Fatal("capacity credit leak")
	}
}

func TestWindowReservationLimitsAndIrrevocability(t *testing.T) {
	b, _ := NewBudget(BufferCharge(1024) * 2)
	r, _ := NewReceiverWithLimits(ReceiverLimits{2, 2048, 1})
	_ = r.SetBudget(b)
	_ = r.RequireCredits()
	_ = r.ReserveCapacity(1, 1024)
	_ = r.ReserveCapacity(3, 1024)
	if err := r.ReserveCapacity(5, 1024); !errors.Is(err, ErrReceiveFull) {
		t.Fatal(err)
	}
	if err := r.ReserveCapacity(1, 1024); err == nil {
		t.Fatal("same grant reserved again")
	}
	if b.Used() != BufferCharge(1024)*2 {
		t.Fatal("overcommit")
	}
	r.Close()
	r.Close()
	if b.Used() != 0 {
		t.Fatal("close did not release")
	}
	if err := r.ReserveCapacity(7, 1); err == nil {
		t.Fatal("closed receiver revived")
	}
}

func TestWindowProbeBackoffAndEnvelopeBounds(t *testing.T) {
	w, _ := NewSendWindow(1, 2, 100)
	now := time.Now()
	_ = w.RequestThrough(5)
	_, _ = w.Next(now)
	if _, ok := w.Next(now.Add(99 * time.Millisecond)); ok {
		t.Fatal("busy probe")
	}
	if _, ok := w.Next(now.Add(100 * time.Millisecond)); !ok {
		t.Fatal("no recovery probe")
	}
	f := Frame{Type: TypeWindowGrant, ID: 5, Total: 100}
	for n := 1; n <= MaxEnvelopePadding; n++ {
		b, err := EncodeEnvelopeInto(nil, f, n)
		if err != nil {
			t.Fatal(err)
		}
		g, err := Decode(b)
		if err != nil || g.Type != f.Type || g.ID != f.ID || g.Total != f.Total {
			t.Fatal("padding corrupts frame")
		}
		b[1], b[2] = 0xff, 0xff
		if _, err := Decode(b); err == nil {
			t.Fatal("unbounded envelope length")
		}
	}
	for _, b := range [][]byte{{envelopeType}, {envelopeType, 0}, {envelopeType, 0, 0}, {envelopeType, 0, 17}} {
		if _, err := Decode(b); err == nil {
			t.Fatal("truncated envelope")
		}
	}
	b, _ := EncodeEnvelopeInto(nil, f, 1)
	b[3] = envelopeType
	if _, err := Decode(b); err == nil {
		t.Fatal("nested envelope")
	}
	if _, err := EncodeEnvelopeInto(nil, f, 257); err == nil {
		t.Fatal("unbounded padding")
	}
}

func TestAdaptiveRTOAndKarn(t *testing.T) {
	q, _ := NewQueue(4096, 4096)
	_ = q.EnableAdaptiveRTO()
	now := time.Now()
	_ = q.Enqueue(1, make([]byte, 100), 80*time.Millisecond)
	f, ok := q.Next(now, 100)
	if !ok {
		t.Fatal("no data")
	}
	ack := Frame{Type: TypeAck, ID: 1, Total: 100, Payload: make([]byte, 4)}
	binary.BigEndian.PutUint32(ack.Payload, 100)
	bad := ack
	bad.Total = 101
	if q.AckAt(bad, now.Add(80*time.Millisecond)) == nil || q.rtt.Samples() != 0 {
		t.Fatal("invalid ACK affects RTT")
	}
	if err := q.AckAt(ack, now.Add(80*time.Millisecond)); err != nil {
		t.Fatal(err)
	}
	if q.CurrentRTO() != 240*time.Millisecond || q.rtt.Samples() != 1 {
		t.Fatal(q.CurrentRTO())
	}
	_ = q.Enqueue(2, make([]byte, 100), 80*time.Millisecond)
	f, _ = q.Next(now.Add(time.Second), 100)
	if f.ID != 2 {
		t.Fatal(f)
	}
	if _, ok = q.Next(now.Add(time.Second+240*time.Millisecond), 100); !ok {
		t.Fatal("missing retry")
	}
	ack.ID = 2
	if err := q.AckAt(ack, now.Add(time.Second+250*time.Millisecond)); err != nil {
		t.Fatal(err)
	}
	if q.rtt.Samples() != 1 {
		t.Fatal("ambiguous retransmit ACK sampled")
	}
	q.Close()
}

func TestAdaptiveRTOBackoffAndLimits(t *testing.T) {
	q, _ := NewQueue(4096, 4096)
	_ = q.EnableAdaptiveRTO()
	now := time.Now()
	_ = q.Enqueue(1, make([]byte, 100), 80*time.Millisecond)
	_, _ = q.Next(now, 100)
	if _, ok := q.Next(now.Add(299*time.Millisecond), 100); ok {
		t.Fatal("premature retry")
	}
	if _, ok := q.Next(now.Add(300*time.Millisecond), 100); !ok {
		t.Fatal("missing initial RTO")
	}
	if _, ok := q.Next(now.Add(899*time.Millisecond), 100); ok {
		t.Fatal("backoff missing")
	}
	if _, ok := q.Next(now.Add(900*time.Millisecond), 100); !ok {
		t.Fatal("backoff stranded retry")
	}
	r := RTTEstimator{}
	for i := 0; i < 100; i++ {
		r.Observe(time.Nanosecond)
	}
	if r.RTO() != MinimumRTO {
		t.Fatal("RTO floor")
	}
	r.Observe(time.Duration(1<<63 - 1))
	if r.RTO() > MaximumRTO || r.RTO() <= 0 {
		t.Fatal("RTO overflow")
	}
}

func TestWindowMetadataChargeAndDecodeAllocations(t *testing.T) {
	if unsafe.Sizeof(sentRange{}) > rangeCharge {
		t.Fatal("range metadata exceeds reserved budget")
	}
	b, _ := EncodeEnvelopeInto(nil, Frame{Type: TypeWindowGrant, ID: 7, Total: 4096}, 64)
	if n := testing.AllocsPerRun(100, func() {
		if _, err := Decode(b); err != nil {
			panic(err)
		}
	}); n != 0 {
		t.Fatal("padded decode allocation", n)
	}
}

func TestWindowDelayedRejectCannotRevokeGrantedIDs(t *testing.T) {
	q, _ := NewQueue(4096, 4096)
	_ = q.UseWindowCredits(1, 2, 1024)
	_ = q.RequestWindow(7)
	_, _ = q.Next(time.Now(), 900)
	reject := Frame{Type: TypeReject, ID: 3, Total: 1024, Offset: 1}
	if !q.MatchesWindowRequest(reject) {
		t.Fatal("pending rejection lost")
	}
	_, err := q.GrantWindow(Frame{Type: TypeWindowGrant, ID: 5, Total: 1024})
	if err != nil {
		t.Fatal(err)
	}
	if q.MatchesWindowRequest(reject) {
		t.Fatal("stale rejection can revoke a granted window")
	}
	reject.ID = 7
	if !q.MatchesWindowRequest(reject) {
		t.Fatal("valid ungranted rejection ignored")
	}
}
