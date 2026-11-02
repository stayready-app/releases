package reliable

import (
	"bytes"
	"encoding/binary"
	"testing"
	"time"
)

func TestAuditConflictingFragmentIsAtomic(t *testing.T) {
	r := NewReceiver()
	_, _, _ = r.Accept(Frame{Type: TypeData, ID: 1, Total: 4, Offset: 2, Payload: []byte{3, 4}})
	before := r.AcceptedBytes()
	if _, _, err := r.Accept(Frame{Type: TypeData, ID: 1, Total: 4, Payload: []byte{9, 9, 8, 4}}); err == nil {
		t.Fatal("conflict accepted")
	}
	if r.AcceptedBytes() != before {
		t.Fatal("rejected fragment changed receiver state")
	}
	_, got, err := r.Accept(Frame{Type: TypeData, ID: 1, Total: 4, Payload: []byte{1, 2, 3, 4}})
	if err != nil || !bytes.Equal(got, []byte{1, 2, 3, 4}) {
		t.Fatalf("valid recovery failed: %v %v", got, err)
	}
}

func TestAuditRetiredIDCannotRedeliver(t *testing.T) {
	r, _ := NewReceiverWithLimits(ReceiverLimits{4, 1024, 2})
	for id := uint64(1); id <= 4; id++ {
		_, _, _ = r.Accept(Frame{Type: TypeData, ID: id, Total: 1, Payload: []byte{1}})
	}
	_, data, err := r.Accept(Frame{Type: TypeData, ID: 1, Total: 1, Payload: []byte{1}})
	if err == nil || data != nil {
		t.Fatal("evicted message delivered twice")
	}
}

func TestAuditQueueSmallMessagesBoundMetadata(t *testing.T) {
	q, _ := NewQueue(1024, 1<<20)
	for i := 0; i < MaxActiveMessages; i++ {
		if e := q.Enqueue(uint64(i), []byte{1}, time.Second); e != nil {
			t.Fatal(e)
		}
	}
	if err := q.Enqueue(9999, []byte{1}, time.Second); err == nil {
		t.Fatal("unbounded tiny-message object count")
	}
}

func TestAuditRetainedMutationRejected(t *testing.T) {
	b, _ := NewBudget(1024)
	r := NewReceiver()
	_ = r.SetBudget(b)
	_, m, _ := r.AcceptRetained(Frame{Type: TypeData, ID: 1, Total: 4, Payload: []byte{1, 2, 3, 4}})
	m.Data = m.Data[:1]
	q, _ := NewQueue(1024, 1024)
	_ = q.SetBudget(b)
	if e := q.EnqueueReceived(2, m, time.Second); e == nil {
		t.Fatal("mutated owner length accepted; budget release would be wrong")
	}
	m.Release()
	q.Close()
	if b.Used() != 0 {
		t.Fatal("reservation leaked")
	}
}

func TestAuditSenderSizeOverflow(t *testing.T) {
	s, _ := NewSender(1, make([]byte, 10), time.Second)
	_, _ = s.Next(time.Now(), 1)
	f, ok := s.Next(time.Now(), int(^uint(0)>>1))
	if !ok || len(f.Payload) != 9 {
		t.Fatal("incorrect capped payload")
	}
}

// Comparable hot-path benchmarks; also run against the unchanged Stage 14 source.
func BenchmarkAuditDuplicateACK(b *testing.B) {
	s, _ := NewSender(1, make([]byte, 32768), time.Second)
	f, _ := s.Next(time.Now(), 32768)
	ack := acknowledgments(f)[0]
	_ = s.Ack(ack)
	b.ReportAllocs()
	b.SetBytes(32768)
	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		if err := s.Ack(ack); err != nil {
			b.Fatal(err)
		}
	}
}
func BenchmarkAuditReceiveDuplicate(b *testing.B) {
	r := NewReceiver()
	f := Frame{Type: TypeData, ID: 1, Total: 32768, Payload: bytes.Repeat([]byte{1}, 900)}
	_, _, _ = r.Accept(f)
	b.ReportAllocs()
	b.SetBytes(900)
	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		if _, _, e := r.Accept(f); e != nil {
			b.Fatal(e)
		}
	}
}
func BenchmarkAuditRetry(b *testing.B) {
	s, _ := NewSender(1, make([]byte, 32768), time.Millisecond)
	now := time.Unix(100, 0)
	for s.next < len(s.data) {
		_, _ = s.Next(now, 900)
	}
	// ACK every other byte: worst sparse ACK layout, bounded input.
	for i := 0; i < 32768; i += 2 {
		buf := make([]byte, 4)
		binary.BigEndian.PutUint32(buf, 1)
		_ = s.Ack(Frame{Type: TypeAck, ID: 1, Offset: uint32(i), Total: 32768, Payload: buf})
	}
	b.ReportAllocs()
	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		now = now.Add(time.Millisecond)
		s.NextRetry(now, 350)
	}
}

func TestAuditRangeMetadataBoundedUnderTinyPackets(t *testing.T) {
	s, _ := NewSender(1, make([]byte, 32768), time.Millisecond)
	now := time.Unix(1, 0)
	for n := 0; n < 32768; n++ {
		if _, ok := s.Next(now, 1); !ok {
			break
		}
	}
	if len(s.ranges) > rangeLimit(32768) || cap(s.ranges) > rangeLimit(32768) || s.next == 32768 {
		t.Fatal("tiny sends inflate descriptors")
	}
	for n := 0; n < 10000; n++ {
		s.NextRetry(now.Add(time.Second), 1)
	}
	if len(s.ranges) > rangeLimit(32768) {
		t.Fatal("retry descriptors grew")
	}
}
func TestAuditRetryDoesNotAllocate(t *testing.T) {
	s, _ := NewSender(1, make([]byte, 4096), time.Millisecond)
	now := time.Unix(1, 0)
	for s.next < len(s.data) {
		s.Next(now, 900)
	}
	n := testing.AllocsPerRun(1000, func() { now = now.Add(time.Second); s.NextRetry(now, 350) })
	if n != 0 {
		t.Fatalf("retry allocated %g objects", n)
	}
}

func TestAuditRetryFairnessAcrossExpiredRanges(t *testing.T) {
	s, _ := NewSender(1, make([]byte, 300), time.Millisecond)
	now := time.Unix(1, 0)
	for s.next < len(s.data) {
		s.Next(now, 100)
	}
	seen := map[uint32]bool{}
	for i := 0; i < 3; i++ {
		now = now.Add(time.Second)
		f, ok := s.NextRetry(now, 100)
		if !ok {
			t.Fatal("retry absent")
		}
		seen[f.Offset] = true
	}
	if len(seen) != 3 {
		t.Fatalf("oldest range starves others: %v", seen)
	}
}
