package reliable

import (
	"errors"
	"time"
)

var ErrQueueFull = errors.New("reliable send queue full")

// Queue schedules multiple independent messages under a shared outstanding-byte
// budget. The caller owns synchronization and must process authenticated ACKs.
type Queue struct {
	closed                    bool
	senders                   []*Sender
	byID                      map[uint64]*Sender
	maxInFlight               int
	maxQueued                 int
	queuedBytes               int
	cursor                    int
	inFlight                  int
	budget                    *Budget
	credits                   bool
	creditWindow              *SendWindow
	rtt                       *RTTEstimator
	nextRTTSample             time.Time
	congestion                *congestion
	freshSize                 int
	packets                   *packetLedger
	packetMSS, packetOverhead int
}

func NewQueue(maxInFlight, maxQueued int) (*Queue, error) {
	if maxInFlight <= 0 || maxQueued <= 0 {
		return nil, ErrFrame
	}
	return &Queue{byID: make(map[uint64]*Sender), maxInFlight: maxInFlight, maxQueued: maxQueued}, nil
}

func (q *Queue) Enqueue(id uint64, data []byte, rto time.Duration) error {
	if q.closed {
		return ErrFrame
	}
	if (q.credits && id == 0) || (q.creditWindow != nil && !q.creditWindow.ValidMessage(id, len(data))) {
		return ErrFrame
	}
	if _, exists := q.byID[id]; exists {
		return ErrFrame
	}
	if len(q.senders) >= MaxActiveMessages || len(data) == 0 || len(data) > MaxMessageSize || len(data) > q.maxQueued-q.queuedBytes {
		return ErrQueueFull
	}
	charge := BufferCharge(len(data))
	if q.budget != nil && !q.budget.reserve(charge) {
		return ErrBudgetExceeded
	}
	s, err := NewSender(id, data, rto)
	if err != nil {
		if q.budget != nil {
			q.budget.release(charge)
		}
		return err
	}
	s.needsGrant = q.credits
	if q.creditWindow != nil {
		_ = q.creditWindow.RequestThrough(id)
		s.needsGrant = !q.creditWindow.Allowed(id, len(s.data))
	}
	q.senders = append(q.senders, s)
	q.byID[id] = s
	q.queuedBytes += len(data)
	return nil
}

func (q *Queue) InFlight() int { return q.inFlight }

func (q *Queue) QueuedBytes() int { return q.queuedBytes }
func (q *Queue) Pending() int     { return len(q.senders) }

func (q *Queue) Ack(f Frame) error { return q.AckAt(f, time.Time{}) }

// AckAt uses a real/virtual monotonic receipt time; invalid ACKs never affect RTT.
func (q *Queue) AckAt(f Frame, now time.Time) error {
	if q.packets != nil {
		if f.Type == TypePacketAckBatch {
			return q.ackPacketBatch(f, now)
		}
		return q.ackPacket(f, now)
	}
	return q.ackLogical(f, now, true)
}
func (q *Queue) ackLogical(f Frame, now time.Time, feedback bool) error {
	s := q.byID[f.ID]
	if s == nil {
		return ErrFrame
	}
	before := s.InFlight()
	flightBefore := q.inFlight
	eligible := feedback && q.congestion != nil && s.growthEligible(f, q.congestion.recoveryAt)
	var sample time.Duration
	if feedback && q.rtt != nil && !now.Before(q.nextRTTSample) {
		sample = s.sampleRTT(f, now)
	}
	if err := s.Ack(f); err != nil {
		return err
	}
	q.inFlight += s.InFlight() - before
	if feedback && q.congestion != nil {
		q.congestion.refill(now, q.rtt)
		q.congestion.onAck(before-s.InFlight(), flightBefore, eligible)
	}
	if q.rtt != nil && sample > 0 && s.InFlight() < before && !now.Before(q.nextRTTSample) {
		q.rtt.Observe(sample)
		// One statistically useful sample per RTT; ACK compression must not
		// collapse variance after hundreds of correlated ACKs in one burst.
		q.nextRTTSample = now.Add(max(time.Millisecond, q.rtt.smoothed))
	}
	if !s.Done() {
		return nil
	}
	q.removeSender(f.ID, s)
	return nil
}

func (q *Queue) removeSender(id uint64, s *Sender) {
	q.queuedBytes -= len(s.data)
	if q.budget != nil {
		q.budget.release(BufferCharge(len(s.data)))
	}
	delete(q.byID, id)
	for i, candidate := range q.senders {
		if candidate == s {
			copy(q.senders[i:], q.senders[i+1:])
			q.senders[len(q.senders)-1] = nil
			q.senders = q.senders[:len(q.senders)-1]
			if i < q.cursor {
				q.cursor--
			}
			if q.cursor >= len(q.senders) {
				q.cursor = 0
			}
			break
		}
	}
}

// ConfirmDelivered retires one queued logical message using authenticated
// application-level proof (for example, the echo response ID=request+1). It is
// intentionally neutral for RTT and congestion growth: no packet receipt is
// fabricated. This method is application-specific evidence, not a generic ACK.
func (q *Queue) ConfirmDelivered(id uint64, now time.Time) error {
	if q.closed {
		return ErrFrame
	}
	s := q.byID[id]
	if s == nil {
		return ErrFrame
	}
	before := s.InFlight()
	s.confirmDelivered()
	q.inFlight -= before
	if q.inFlight < 0 {
		q.inFlight = 0
	}
	if q.packets != nil {
		q.packets.retireMessage(id)
	}
	q.removeSender(id, s)
	q.maybeReleasePacketStorage(now)
	return nil
}

// Next uses round-robin scheduling and a global window. Retransmissions
// remain possible even when the window is full.
func (q *Queue) Next(now time.Time, maxPayload int) (Frame, bool) {
	return q.NextSized(now, maxPayload, maxPayload)
}

// NextSized decouples fresh packet packing from retransmission packing. Size
// changes apply to the retried logical range, not to a copied ciphertext packet.
func (q *Queue) NextSized(now time.Time, freshPayload, retryPayload int) (Frame, bool) {
	if q.closed {
		return Frame{}, false
	}
	if q.packets != nil {
		q.packets.releaseIdleStorage(now)
		q.packets.expire(now, q.congestion)
	}
	if q.creditWindow != nil {
		if f, ok := q.creditWindow.Next(now); ok {
			return f, true
		}
	}
	count := len(q.senders)
	if count == 0 || freshPayload <= 0 || retryPayload <= 0 {
		return Frame{}, false
	}
	available := q.maxInFlight - q.inFlight
	// Do not allocate the bounded packet ledger merely because payload is queued.
	// A peer may never grant receive credit; only a sender that is actually
	// eligible to emit DATA needs physical-transmission state.
	hasDataCandidate := false
	for _, s := range q.senders {
		if !s.needsGrant {
			hasDataCandidate = true
			break
		}
	}
	dataReady := hasDataCandidate && q.packetReady()
	if hasDataCandidate && q.packets != nil && !q.packets.canSend() {
		q.packets.stats.RingWaits++
	}
	if q.congestion != nil {
		mss := q.congestion.mss
		charge := 0
		if q.packets == nil {
			available = min(available, q.congestion.window-q.inFlight)
		} else {
			mss = q.packetMSS
			charge = q.packetOverhead
		}
		freshPayload = min(freshPayload, mss)
		retryPayload = min(retryPayload, mss)
		q.freshSize = freshPayload
		paced := !q.congestion.readyAt(now, max(freshPayload, retryPayload)+charge, q.rtt).After(now)
		if !paced {
			q.congestion.stats.PacingWaits++
		}
		dataReady = dataReady && paced
		if available <= 0 || !dataReady {
			q.congestion.stats.WindowWaits++
		}
	}
	for i := 0; i < count; i++ {
		index := (q.cursor + i) % count
		s := q.senders[index]
		if q.rtt != nil {
			s.rto, s.adaptive = q.rtt.RTO(), true
		}
		before := s.InFlight()
		var f Frame
		var ok bool
		retry := false
		var previous time.Time
		if q.creditWindow == nil {
			f, ok = s.nextOpen(now)
		}
		if !ok && dataReady {
			f, previous, ok = s.nextRetry(now, retryPayload)
			retry = ok
			if ok && q.congestion != nil && q.packets == nil {
				q.congestion.onTimeout(previous, now)
				available = min(available, q.congestion.window-q.inFlight)
			}
		}
		freshAllowed := available > 0
		if q.congestion != nil {
			freshAllowed = q.canSendFresh(s, freshPayload)
		}
		if !ok && dataReady && freshAllowed {
			_ = s.SetWindow(s.InFlight() + available)
			f, ok = s.nextFresh(now, freshPayload)
		}
		if ok {
			if f.Type == TypeData {
				if q.packets != nil {
					f = q.packets.sent(f, now, q.CurrentRTO(), q.packetOverhead)
					if q.congestion != nil {
						q.congestion.onSend(now, len(f.Payload)+q.packetOverhead, retry, q.rtt, s.rto)
						if retry {
							q.congestion.stats.RetryBytes -= uint64(q.packetOverhead)
						}
					}
				} else if q.congestion != nil {
					q.congestion.onSend(now, len(f.Payload), retry, q.rtt, s.rto)
				}
			}
			q.inFlight += s.InFlight() - before
			q.cursor = (index + 1) % count
			return f, true
		}
	}
	return Frame{}, false
}

// SetBudget must be called before enqueueing. It reserves payload, bitmap and range metadata.
func (q *Queue) SetBudget(b *Budget) error {
	if q.closed || len(q.senders) != 0 {
		return ErrFrame
	}
	q.budget = b
	return nil
}

// EnqueueReceived transfers both payload and bitmap without an additional copy
// or an unaccounted gap in the shared budget. On failure the caller still owns m.
func (q *Queue) EnqueueReceived(id uint64, m *Received, rto time.Duration) error {
	if q.closed {
		return ErrFrame
	}
	if q.credits && id == 0 {
		return ErrFrame
	}
	if m == nil || m.released || m.budget != q.budget || rto <= 0 ||
		len(m.Data) != len(m.original) || len(m.Data) == 0 || &m.Data[0] != &m.original[0] {
		return ErrFrame
	}
	if q.creditWindow != nil && !q.creditWindow.ValidMessage(id, len(m.Data)) {
		return ErrFrame
	}
	if _, exists := q.byID[id]; exists {
		return ErrFrame
	}
	if len(q.senders) >= MaxActiveMessages || len(m.Data) == 0 || len(m.Data) > q.maxQueued-q.queuedBytes {
		return ErrQueueFull
	}
	clear(m.bitmap)
	s := &Sender{id: id, data: m.Data, acked: m.bitmap, rto: rto}
	s.needsGrant = q.credits
	if q.creditWindow != nil {
		_ = q.creditWindow.RequestThrough(id)
		s.needsGrant = !q.creditWindow.Allowed(id, len(s.data))
	}
	q.senders = append(q.senders, s)
	q.byID[id] = s
	q.queuedBytes += len(s.data)
	m.released = true
	m.Data, m.original, m.bitmap = nil, nil, nil
	return nil
}

// Close releases all outgoing credit and drops references to payloads.
func (q *Queue) Close() {
	q.closed = true
	if q.packets != nil {
		q.packets.close()
	}
	for i, s := range q.senders {
		if q.budget != nil {
			q.budget.release(BufferCharge(len(s.data)))
		}
		q.senders[i] = nil
	}
	q.senders = nil
	clear(q.byID)
	q.queuedBytes, q.inFlight, q.cursor = 0, 0, 0
}

// Wake reports the earliest useful send; fresh data requires global credit.
func (q *Queue) Wake(now time.Time) (time.Time, bool) {
	if q.closed {
		return time.Time{}, false
	}
	var next time.Time
	if q.packets != nil {
		q.packets.releaseIdleStorage(now)
		q.packets.expire(now, q.congestion)
		next, _ = q.packets.wake()
	}
	if q.creditWindow != nil {
		if at, ok := q.creditWindow.Wake(now); ok && (next.IsZero() || at.Before(next)) {
			next = at
		}
	}
	for _, s := range q.senders {
		if q.rtt != nil {
			s.rto, s.adaptive = q.rtt.RTO(), true
		}
		if q.creditWindow != nil && s.needsGrant {
			continue
		}
		fresh := q.inFlight < q.maxInFlight
		if q.congestion != nil {
			fresh = fresh && q.canSendFresh(s, max(1, q.freshSize))
		}
		if at, ok := s.wake(now, fresh); ok {
			if q.packets != nil && !s.needsGrant && !q.packetCanSend() {
				if deadline, waiting := q.packets.wake(); waiting {
					at = maxTimeValue(at, deadline)
				} else {
					continue
				}
			}
			if q.congestion != nil && !s.needsGrant {
				paced := q.congestion.readyAt(now, q.congestion.mss, q.rtt)
				if paced.After(at) {
					at = paced
				}
			}
			if next.IsZero() || at.Before(next) {
				next = at
			}
		}
	}
	return next, !next.IsZero()
}

// RequireCredits enables receiver admission for future enqueues. Must be set on
// an empty queue, before any DATA can be transmitted. No automatic fallback.
func (q *Queue) RequireCredits() error {
	if q.closed || len(q.senders) != 0 {
		return ErrFrame
	}
	q.credits = true
	return nil
}

// Grant authorizes DATA only for the exact queued message and immutable length.
// The boolean reports new progress, not a duplicate permission.
func (q *Queue) Grant(f Frame) (bool, error) {
	s := q.byID[f.ID]
	if q.creditWindow != nil || !q.credits || s == nil || f.Type != TypeGrant || validateFrame(f) != nil || int(f.Total) != len(s.data) || !s.openSent {
		return false, ErrFrame
	}
	changed := s.needsGrant
	s.needsGrant = false
	return changed, nil
}

// Blocked changes only the next small OPEN probe, never DATA retransmission or
// an existing GRANT. Delayed/reordered BLOCKED frames cannot revoke permission.
func (q *Queue) Blocked(f Frame, now time.Time) error {
	s := q.byID[f.ID]
	if q.creditWindow != nil || !q.credits || s == nil || f.Type != TypeBlocked || validateFrame(f) != nil || int(f.Total) != len(s.data) || !s.openSent {
		return ErrFrame
	}
	if s.needsGrant {
		at := now.Add(time.Duration(f.Offset) * time.Millisecond)
		if at.After(s.openNext) {
			s.openNext = at
		}
	}
	return nil
}

// MatchesOpen identifies an outstanding admission operation, including GRANT
// loss/reordering. Rejections for unrelated or already granted IDs are ignored.
func (q *Queue) MatchesOpen(f Frame) bool {
	s := q.byID[f.ID]
	return q.creditWindow == nil && q.credits && s != nil && s.openSent && s.needsGrant && int(f.Total) == len(s.data)
}

// PendingID reports whether the queue still owns this message (including a
// message waiting for admission), independently of its outstanding-byte count.
func (q *Queue) PendingID(id uint64) bool { return q.byID[id] != nil }

// UseWindowCredits is an explicit per-session mode, not an implicit fallback.
// Only cumulative authenticated WINDOW_GRANT can authorize these senders.
func (q *Queue) UseWindowCredits(first, step uint64, capacity int) error {
	if q.closed || len(q.senders) != 0 || q.creditWindow != nil {
		return ErrFrame
	}
	w, err := NewSendWindow(first, step, capacity)
	if err != nil {
		return err
	}
	q.credits, q.creditWindow = true, w
	return nil
}

// RequestWindow announces bounded application demand ahead of DATA. It holds
// no payload and grants no permission. The receiver still reserves every ID.
func (q *Queue) RequestWindow(through uint64) error {
	if q.closed || q.creditWindow == nil {
		return ErrFrame
	}
	return q.creditWindow.RequestThrough(through)
}

func (q *Queue) GrantWindow(f Frame) (bool, error) {
	if q.closed || q.creditWindow == nil {
		return false, ErrFrame
	}
	changed, err := q.creditWindow.Grant(f)
	if err != nil {
		return false, err
	}
	if changed {
		for _, s := range q.senders {
			if q.creditWindow.Allowed(s.id, len(s.data)) {
				s.needsGrant = false
			}
		}
	}
	return changed, nil
}

func (q *Queue) MatchesWindowRequest(f Frame) bool {
	w := q.creditWindow
	return w != nil && w.validID(f.ID) && f.ID > w.granted && f.ID <= w.sentThrough && int(f.Total) == w.capacity
}

// EnableAdaptiveRTO shares one estimator across this direction of a session.
// It must be enabled before sending. No timer or goroutine is created.
func (q *Queue) EnableAdaptiveRTO() error {
	if q.closed || q.inFlight != 0 || (q.packets != nil && q.packets.stats.Sent != 0) {
		return ErrFrame
	}
	q.rtt = &RTTEstimator{}
	return nil
}
func (q *Queue) CurrentRTO() time.Duration {
	if q.rtt == nil {
		return 0
	}
	return q.rtt.RTO()
}

// CreditBlockedMessages counts queued payloads actually waiting for permission.
// Future unused grants alone are not application/data-delivery progress.
func (q *Queue) CreditBlockedMessages() int {
	n := 0
	for _, s := range q.senders {
		if s.needsGrant {
			n++
		}
	}
	return n
}

// EnableCongestionControl must precede enqueue/send. Callers pass their maximum
// DATA payload and conservative framing+crypto+UDP+IP overhead. Only DATA is
// paced; ACK/receive-credit liveness must not depend on the outbound data window.
func (q *Queue) EnableCongestionControl(maxPayload, wireOverhead int) error {
	if q.closed || len(q.senders) != 0 || q.inFlight != 0 || q.congestion != nil || q.packets != nil {
		return ErrFrame
	}
	c, err := newCongestion(q.maxInFlight, maxPayload, wireOverhead)
	if err != nil {
		return err
	}
	q.congestion = c
	q.freshSize = maxPayload
	if q.rtt == nil {
		q.rtt = &RTTEstimator{}
	}
	return nil
}
func (q *Queue) CongestionStats() CongestionStats {
	if q.congestion == nil {
		return CongestionStats{}
	}
	s := q.congestion.stats
	s.WindowBytes = q.congestion.window
	return s
}

// Do not turn small ACK/window increments into persistent tiny DATA ranges.
// Wait for one complete configured fragment (or the actual message tail), bounded
// by configured flight/window capacity. Otherwise congestion shrink can create
// thousands of tiny original ranges and magnify retries/CPU. ACKs still wake us.
func (q *Queue) canSendFresh(s *Sender, payload int) bool {
	remaining := len(s.data) - s.next
	if remaining <= 0 {
		return false
	}
	window := q.maxInFlight
	if q.packets == nil {
		window = min(window, q.congestion.window)
	}
	need := min(payload, remaining, window)
	return window-q.inFlight >= need
}

func maxTimeValue(a, b time.Time) time.Time {
	if a.After(b) {
		return a
	}
	return b
}
