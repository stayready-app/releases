package reliable

import (
	"encoding/binary"
	"sort"
	"time"
)

type sentRange struct {
	start         int
	end           int
	at            time.Time
	retry         int
	retransmitted bool
	attempts      uint8
	packingRound  uint8 // Cycles independently of capped RTO backoff.
}

type Sender struct {
	adaptive    bool
	needsGrant  bool
	openSent    bool
	openNext    time.Time
	openBackoff time.Duration
	id          uint64
	data        []byte
	acked       []uint64
	ackedCount  int
	ranges      []sentRange
	retryCursor int
	next        int
	rto         time.Duration
	complete    bool
	window      int // Maximum unacknowledged bytes; zero means unlimited (legacy behavior).
}

func NewSender(id uint64, data []byte, rto time.Duration) (*Sender, error) {
	if len(data) == 0 || len(data) > MaxMessageSize || rto <= 0 {
		return nil, ErrFrame
	}
	return &Sender{id: id, data: append([]byte(nil), data...), acked: make([]uint64, (len(data)+63)/64), rto: rto}, nil
}

// SetWindow bounds the number of distinct bytes transmitted but not yet acknowledged.
// It does not count retransmissions as new bytes. A zero window restores legacy behavior.
func (s *Sender) SetWindow(bytes int) error {
	if bytes < 0 {
		return ErrFrame
	}
	s.window = bytes
	return nil
}

func (s *Sender) InFlight() int { return s.next - s.ackedCount }

func (s *Sender) isAcked(i int) bool {
	return s.acked[i/64]&(uint64(1)<<uint(i%64)) != 0
}

func (s *Sender) Next(now time.Time, maxPayload int) (Frame, bool) {
	if f, ok := s.nextFresh(now, maxPayload); ok {
		return f, true
	}
	return s.NextRetry(now, maxPayload)
}

func (s *Sender) nextFresh(now time.Time, maxPayload int) (Frame, bool) {
	if maxPayload <= 0 || s.complete || s.needsGrant {
		return Frame{}, false
	}
	if s.next < len(s.data) && (s.window == 0 || s.InFlight() < s.window) {
		start := s.next
		end := start + min(maxPayload, len(s.data)-start)
		if s.window > 0 && end-start > s.window-s.InFlight() {
			end = start + s.window - s.InFlight()
		}
		if end > len(s.data) {
			end = len(s.data)
		}
		if len(s.ranges) == rangeLimit(len(s.data)) {
			return Frame{}, false
		}
		if len(s.ranges) == cap(s.ranges) {
			capacity := min(rangeLimit(len(s.data)), max(8, cap(s.ranges)*2))
			next := make([]sentRange, len(s.ranges), capacity)
			copy(next, s.ranges)
			s.ranges = next
		}
		s.next = end
		s.ranges = append(s.ranges, sentRange{start: start, end: end, at: now, retry: start})
		return Frame{Type: TypeData, ID: s.id, Offset: uint32(start), Total: uint32(len(s.data)), Payload: s.data[start:end]}, true
	}
	return Frame{}, false
}

// NextRetry only retransmits previously sent, expired ranges.
func (s *Sender) NextRetry(now time.Time, maxPayload int) (Frame, bool) {
	f, _, ok := s.nextRetry(now, maxPayload)
	return f, ok
}

// forceRetry makes the original range covering a packet-threshold loss eligible
// immediately. The next retry still skips already acknowledged bytes and uses
// the normal bounded repacking logic; no new range descriptors are allocated.
func (s *Sender) forceRetry(offset, length int, now time.Time) {
	if length <= 0 || offset < 0 || offset >= len(s.data) {
		return
	}
	end := min(len(s.data), offset+length)
	first := sort.Search(len(s.ranges), func(i int) bool { return s.ranges[i].end > offset })
	for i := first; i < len(s.ranges) && s.ranges[i].start < end; i++ {
		r := &s.ranges[i]
		if firstMissing(s.acked, max(offset, r.start), min(end, r.end)) == min(end, r.end) {
			continue
		}
		r.at = now.Add(-s.rangeRTO(r))
		r.retry = r.start
	}
}

// nextRetry also reports the previous attempt's time for a bounded loss epoch.
func (s *Sender) nextRetry(now time.Time, maxPayload int) (Frame, time.Time, bool) {
	if maxPayload <= 0 || s.complete || s.needsGrant {
		return Frame{}, time.Time{}, false
	}
	// A range is one ORIGINAL transmission. A cursor repacks retries without
	// splitting/allocating records, even when retryPayload is one byte.
	for n := 0; n < len(s.ranges); n++ {
		idx := (s.retryCursor + n) % len(s.ranges)
		r := &s.ranges[idx]
		if now.Sub(r.at) < s.rangeRTO(r) {
			continue
		}
		start := firstMissing(s.acked, r.retry, r.end)
		if start == r.end {
			r.at = now
			r.retry = r.start
			continue
		}
		// Repack even a short original range that already fits below maxPayload.
		// Otherwise a filter matching its fixed tail length prevents recovery forever.
		// Four bounded shapes rotate; no extra descriptors/copies and no permanently
		// tiny-packet mode after a long outage. ACK/range addressing is unchanged.
		packingLimit := min(maxPayload, max(1, (r.end-r.start)>>r.packingRound))
		end := start + min(packingLimit, r.end-start)
		previous := r.at
		r.retry = end
		r.retransmitted = true
		if end == r.end {
			if r.attempts < 6 {
				r.attempts++
			}
			r.at = now
			r.retry = r.start
			r.packingRound = (r.packingRound + 1) % 4
		}
		s.retryCursor = (idx + 1) % len(s.ranges)
		return Frame{Type: TypeData, ID: s.id, Offset: uint32(start), Total: uint32(len(s.data)), Payload: s.data[start:end]}, previous, true
	}
	return Frame{}, time.Time{}, false
}

func (s *Sender) Ack(f Frame) error {
	if f.Type != TypeAck || f.ID != s.id || int(f.Total) != len(s.data) || int(f.Offset) >= len(s.data) || len(f.Payload) != 4 {
		return ErrFrame
	}
	length := binary.BigEndian.Uint32(f.Payload)
	if length == 0 || uint64(f.Offset)+uint64(length) > uint64(f.Total) || uint64(f.Offset)+uint64(length) > uint64(s.next) {
		return ErrFrame
	}
	if s.complete {
		return nil
	}
	added := markBits(s.acked, int(f.Offset), int(f.Offset+length))
	s.ackedCount += added
	if added != 0 {
		// Original transmission ranges are ordered and disjoint. Only ranges that
		// overlap this ACK can become fully acknowledged; do not rescan the other
		// messages' bytes on every partial ACK (quadratic work under reordering).
		begin, end := int(f.Offset), int(f.Offset+length)
		first := sort.Search(len(s.ranges), func(i int) bool { return s.ranges[i].end > begin })
		last, out := first, first
		for last < len(s.ranges) && s.ranges[last].start < end {
			r := s.ranges[last]
			if firstMissing(s.acked, r.start, r.end) != r.end {
				s.ranges[out] = r
				out++
			}
			last++
		}
		if out < last {
			removed := last - out
			copy(s.ranges[out:], s.ranges[last:])
			clear(s.ranges[len(s.ranges)-removed:])
			s.ranges = s.ranges[:len(s.ranges)-removed]
		}
	}

	if s.ackedCount == len(s.data) {
		s.complete = true
		s.ranges = nil
	}
	return nil
}

func (s *Sender) confirmDelivered() {
	if s.complete {
		return
	}
	s.next = len(s.data)
	s.ackedCount = len(s.data)
	s.complete = true
	s.ranges = nil
}

func (s *Sender) Done() bool { return s.complete }

// MaxTrackedRanges bounds one sender's descriptors, regardless of fragment size.
const MaxTrackedRanges = 1024
const rangeCharge = 64           // conservative bound for sentRange on supported 64-bit builds
func rangeLimit(payload int) int { return min(payload, MaxTrackedRanges, max(8, (payload+255)/256)) }

// Wake returns the next useful attempt. No polling of every idle sender is needed.
func (s *Sender) wake(now time.Time, freshAllowed bool) (time.Time, bool) {
	if s.complete {
		return time.Time{}, false
	}
	if s.needsGrant {
		if s.openNext.IsZero() || s.openNext.Before(now) {
			return now, true
		}
		return s.openNext, true
	}
	if freshAllowed && s.next < len(s.data) && len(s.ranges) < rangeLimit(len(s.data)) {
		return now, true
	}
	var next time.Time
	for _, r := range s.ranges {
		at := r.at.Add(s.rangeRTO(&r))
		if next.IsZero() || at.Before(next) {
			next = at
		}
	}
	return next, !next.IsZero()
}

// OPEN probes have their own bounded exponential timer. Network injection cannot
// accelerate it; data loss and receiver backpressure are separate mechanisms.
func (s *Sender) nextOpen(now time.Time) (Frame, bool) {
	if !s.needsGrant || now.Before(s.openNext) {
		return Frame{}, false
	}
	if s.openBackoff == 0 {
		s.openBackoff = 100 * time.Millisecond
	} else {
		s.openBackoff = min(time.Second, 2*s.openBackoff)
	}
	s.openSent = true
	s.openNext = now.Add(s.openBackoff)
	return Frame{Type: TypeOpen, ID: s.id, Total: uint32(len(s.data))}, true
}

func (s *Sender) rangeRTO(r *sentRange) time.Duration {
	if !s.adaptive {
		return s.rto
	}
	return min(MaximumRTO, s.rto*time.Duration(uint64(1)<<min(r.attempts, 6)))
}

// sampleRTT accepts only a fully acknowledged original transmission which has
// NEVER been retransmitted. ACKs have no transmission number, so sampling a
// retransmitted range would be ambiguous (Karn's rule).
func (s *Sender) sampleRTT(f Frame, now time.Time) time.Duration {
	if now.IsZero() || f.Type != TypeAck || len(f.Payload) != 4 {
		return 0
	}
	start, end := uint64(f.Offset), uint64(f.Offset)+uint64(binary.BigEndian.Uint32(f.Payload))
	first := sort.Search(len(s.ranges), func(i int) bool { return uint64(s.ranges[i].end) > start })
	for _, r := range s.ranges[first:] {
		if uint64(r.start) >= end {
			break
		}
		if !r.retransmitted && start <= uint64(r.start) && end >= uint64(r.end) && now.After(r.at) {
			return now.Sub(r.at)
		}
	}
	return 0
}

// growthEligible is conservative after congestion: old/retransmitted ranges
// cannot inflate the window even when a delayed valid ACK makes progress.
func (s *Sender) growthEligible(f Frame, recovery time.Time) bool {
	if f.Type != TypeAck || len(f.Payload) != 4 {
		return false
	}
	start := uint64(f.Offset)
	end := start + uint64(binary.BigEndian.Uint32(f.Payload))
	first := sort.Search(len(s.ranges), func(i int) bool { return uint64(s.ranges[i].end) > start })
	if first == len(s.ranges) {
		return false
	}
	for _, r := range s.ranges[first:] {
		if uint64(r.start) >= end {
			break
		}
		if r.retransmitted || (!recovery.IsZero() && !r.at.After(recovery)) {
			return false
		}
	}
	return true
}
