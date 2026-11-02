package reliable

import (
	"encoding/binary"
	"time"
)

// PacketSlots is a hard per-direction cap, not a multiplier of payload size.
// No payload references, maps, per-packet timers or goroutines are stored here.
const PacketSlots = 128
const PacketStateCharge int64 = 8192
const packetStorageIdleGrace = 100 * time.Millisecond

const (
	packetEmpty uint8 = iota
	packetInFlight
	packetAcked
	packetLost
	packetRetired
)

type packetRecord struct {
	number, id                  uint64
	sent, expires               int64 // Monotonic offsets relative to packetLedger.epoch.
	offset, length, total, wire uint32
	state                       uint8
}

type PacketStats struct {
	Sent               uint64 `json:"sent"`
	Receipts           uint64 `json:"receipts"`
	ReceiptFrames      uint64 `json:"receipt_frames"`
	BatchReceiptFrames uint64 `json:"batch_receipt_frames"`
	DuplicateReceipts  uint64 `json:"duplicate_receipts"`
	LateReceipts       uint64 `json:"late_receipts"`
	Lost               uint64 `json:"timeout_losses"`
	ThresholdLost      uint64 `json:"threshold_losses"`
	RingWaits          uint64 `json:"ring_waits"`
	WireInFlight       int    `json:"wire_in_flight"`
	PeakWireInFlight   int    `json:"peak_wire_in_flight"`
	Outstanding        int    `json:"outstanding"`
	PeakOutstanding    int    `json:"peak_outstanding"`
}

type packetLedger struct {
	records     *[PacketSlots]packetRecord
	epoch       time.Time
	next        uint64
	nextExpiry  int64
	wire, count int
	budget      *Budget
	released    bool
	stats       PacketStats
	releaseAt   time.Time
}

func newPacketLedger(b *Budget) (*packetLedger, error) {
	return &packetLedger{next: 1, budget: b}, nil
}
func (p *packetLedger) ensureStorage() bool {
	if p == nil || p.released {
		return false
	}
	if p.records != nil {
		return true
	}
	if p.budget != nil && !p.budget.reserve(PacketStateCharge) {
		return false
	}
	p.records = &[PacketSlots]packetRecord{}
	return true
}
func (p *packetLedger) releaseStorage() {
	if p == nil {
		return
	}
	p.releaseAt = time.Time{}
	if p.records == nil {
		return
	}
	p.records = nil
	if p.budget != nil {
		p.budget.release(PacketStateCharge)
	}
}
func (p *packetLedger) scheduleStorageRelease(now time.Time) {
	if p == nil || p.records == nil || p.count != 0 {
		return
	}
	p.releaseAt = now.Add(packetStorageIdleGrace)
}
func (p *packetLedger) releaseIdleStorage(now time.Time) {
	if p == nil || p.records == nil || p.releaseAt.IsZero() || now.Before(p.releaseAt) || p.count != 0 {
		return
	}
	p.releaseStorage()
}
func (p *packetLedger) close() {
	if p == nil || p.released {
		return
	}
	p.releaseStorage()
	p.wire, p.count, p.nextExpiry = 0, 0, 0
	p.released = true
}
func (p *packetLedger) now(now time.Time) int64 {
	if p.epoch.IsZero() {
		p.epoch = now
	}
	return int64(now.Sub(p.epoch)) + 1 // zero is an empty deadline, even for the first send.
}
func (p *packetLedger) at(n int64) time.Time { return p.epoch.Add(time.Duration(n - 1)) }
func (p *packetLedger) canSend() bool {
	// Never overwrite a still-live transmission, even if other ring slots are free.
	if p.released || p.next == 0 || p.next == ^uint64(0) {
		return false
	}
	return p.records == nil || p.records[p.next%PacketSlots].state != packetInFlight
}
func (p *packetLedger) sent(f Frame, now time.Time, rto time.Duration, overhead int) Frame {
	p.releaseAt = time.Time{}
	n := p.now(now)
	r := &p.records[p.next%PacketSlots]
	*r = packetRecord{number: p.next, id: f.ID, sent: n, expires: n + int64(rto), offset: f.Offset,
		length: uint32(len(f.Payload)), total: f.Total, wire: uint32(len(f.Payload) + overhead), state: packetInFlight}
	f.Type, f.PacketNumber = TypePacketData, p.next
	p.next++
	p.wire += int(r.wire)
	p.count++
	if p.nextExpiry == 0 || r.expires < p.nextExpiry {
		p.nextExpiry = r.expires
	}
	p.stats.Sent++
	p.stats.PeakWireInFlight = max(p.stats.PeakWireInFlight, p.wire)
	p.stats.PeakOutstanding = max(p.stats.PeakOutstanding, p.count)
	return f
}

// check validates the entire receipt before either packet or logical state moves.
// An overwritten old receipt has no authority to acknowledge any logical data.
func (p *packetLedger) lookup(number uint64) (*packetRecord, error) {
	if p.released || number == 0 || number >= p.next {
		return nil, ErrFrame
	}
	if p.records == nil {
		return nil, ErrStale
	}
	r := &p.records[number%PacketSlots]
	if r.number != number {
		return nil, ErrStale
	}
	return r, nil
}

func (p *packetLedger) check(f Frame) (*packetRecord, error) {
	if f.Type != TypePacketAck || validateFrame(f) != nil {
		return nil, ErrFrame
	}
	r, err := p.lookup(f.PacketNumber)
	if err != nil {
		return nil, err
	}
	if r.id != f.ID || r.offset != f.Offset || r.total != f.Total || r.length != binary.BigEndian.Uint32(f.Payload) {
		return nil, ErrFrame
	}
	return r, nil
}
func (p *packetLedger) acknowledge(r *packetRecord) {
	if r.state == packetAcked {
		p.stats.DuplicateReceipts++
		return
	}
	if r.state == packetInFlight {
		p.wire -= int(r.wire)
		p.count--
	} else {
		p.stats.LateReceipts++
	}
	r.state = packetAcked
	p.stats.Receipts++
	if p.count == 0 {
		p.nextExpiry = 0
	}
}

// expire runs only when the cached earliest deadline is reached, not on every
// packet/ACK. One bounded scan expires physical COPIES independently of payload
// ownership. Payload retransmission remains the existing selective-range engine.
func (p *packetLedger) expire(now time.Time, c *congestion) {
	if p.records == nil || p.nextExpiry == 0 || p.epoch.IsZero() {
		return
	}
	n := int64(now.Sub(p.epoch)) + 1
	if n < p.nextExpiry {
		return
	}
	p.nextExpiry = 0
	for i := range p.records {
		r := &p.records[i]
		if r.state != packetInFlight {
			continue
		}
		if r.expires <= n {
			r.state = packetLost
			p.wire -= int(r.wire)
			p.count--
			p.stats.Lost++
			if c != nil {
				c.onTimeout(p.at(r.sent), now)
			}
		} else if p.nextExpiry == 0 || r.expires < p.nextExpiry {
			p.nextExpiry = r.expires
		}
	}
}
func (p *packetLedger) wake() (time.Time, bool) {
	var at time.Time
	if p.nextExpiry != 0 {
		at = p.at(p.nextExpiry)
	}
	if !p.releaseAt.IsZero() && (at.IsZero() || p.releaseAt.Before(at)) {
		at = p.releaseAt
	}
	return at, !at.IsZero()
}

// retireMessage stops accounting physical copies whose logical message has
// been proven delivered by an authenticated higher-level application response.
// It deliberately does not create RTT/cwnd growth or pretend that a packet ACK
// arrived. A later exact receipt is merely late/stale feedback.
func (p *packetLedger) retireMessage(id uint64) {
	if p == nil || p.records == nil {
		return
	}
	changed := false
	for i := range p.records {
		r := &p.records[i]
		if r.number == 0 || r.id != id || r.state == packetAcked || r.state == packetRetired {
			continue
		}
		if r.state == packetInFlight {
			p.wire -= int(r.wire)
			p.count--
			changed = true
		}
		r.state = packetRetired
	}
	if changed {
		p.nextExpiry = 0
		for i := range p.records {
			r := &p.records[i]
			if r.state == packetInFlight && (p.nextExpiry == 0 || r.expires < p.nextExpiry) {
				p.nextExpiry = r.expires
			}
		}
	}
}

func (p *packetLedger) snapshot() PacketStats {
	s := p.stats
	s.WireInFlight = p.wire
	s.Outstanding = p.count
	return s
}

// EnablePacketTracking selects Stage 19 exact DATA receipts for this direction.
// Must precede enqueues. State is capped and separately budgeted; do NOT charge
// it to the payload admission budget, which may be fully reserved by the peer.
// No automatic fallback to range-only ACKs is allowed in this mode.
func (q *Queue) EnablePacketTracking(b *Budget, payloadMSS, overhead int) error {
	if q.closed || len(q.senders) != 0 || q.inFlight != 0 || q.packets != nil || payloadMSS < 1 || payloadMSS > 65535 || overhead < 1 || overhead > 4096 {
		return ErrFrame
	}
	var c *congestion
	var err error
	if q.congestion != nil {
		maximum := q.maxInFlight + ((q.maxInFlight+payloadMSS-1)/payloadMSS)*overhead
		c, err = newCongestion(maximum, min(payloadMSS, q.maxInFlight)+overhead, 0)
		if err != nil {
			return err
		}
	}
	p, err := newPacketLedger(b)
	if err != nil {
		return err
	}
	q.packets, q.packetMSS, q.packetOverhead = p, min(payloadMSS, q.maxInFlight), overhead
	if c != nil {
		q.congestion = c
	}
	if q.rtt == nil {
		q.rtt = &RTTEstimator{}
	}
	q.freshSize = q.packetMSS
	return nil
}
func (q *Queue) PacketStats() PacketStats {
	if q.packets == nil {
		return PacketStats{}
	}
	return q.packets.snapshot()
}
func (q *Queue) packetCanSend() bool {
	if q.packets == nil {
		return true
	}
	if !q.packets.canSend() {
		return false
	}
	return q.congestion == nil || q.congestion.window-q.packets.wire >= q.packetMSS+q.packetOverhead
}

// packetReady is used only on the actual send path. Wake/scheduling must not
// allocate the bounded packet ring just to calculate a future deadline.
func (q *Queue) packetReady() bool {
	if !q.packetCanSend() {
		return false
	}
	return q.packets == nil || q.packets.ensureStorage()
}
func (q *Queue) maybeReleasePacketStorage(now time.Time) {
	if q.packets != nil && q.packets.count == 0 && len(q.senders) == 0 {
		q.packets.scheduleStorageRelease(now)
	}
}

func (q *Queue) ackPacket(f Frame, now time.Time) error {
	p := q.packets
	if p == nil {
		return ErrFrame
	}
	r, err := p.check(f)
	if err != nil {
		return err
	}
	p.stats.ReceiptFrames++
	newReceipt := r.state != packetAcked
	if err := q.ackPacketRecord(r, now); err != nil {
		return err
	}
	if newReceipt {
		q.detectPacketThresholdLoss(f.PacketNumber, now)
	}
	q.maybeReleasePacketStorage(now)
	return nil
}

func (q *Queue) ackPacketBatch(f Frame, now time.Time) error {
	p := q.packets
	if p == nil || validateFrame(f) != nil || f.Type != TypePacketAckBatch {
		return ErrFrame
	}
	var records [PacketAckBatchMax]*packetRecord
	count := int(f.Total)
	var largestNew uint64
	for i := 0; i < count; i++ {
		number := binary.BigEndian.Uint64(f.Payload[i*8:])
		r, err := p.lookup(number)
		if err == ErrStale {
			continue // Authenticated delayed receipt for an already retired ring slot.
		}
		if err != nil {
			return err // Validate the entire batch before changing any state.
		}
		records[i] = r
		if r.state != packetAcked && number > largestNew {
			largestNew = number
		}
	}
	p.stats.ReceiptFrames++
	p.stats.BatchReceiptFrames++
	for i := 0; i < count; i++ {
		if records[i] != nil {
			if err := q.ackPacketRecord(records[i], now); err != nil {
				return err
			}
		}
	}
	if largestNew != 0 {
		q.detectPacketThresholdLoss(largestNew, now)
	}
	q.maybeReleasePacketStorage(now)
	return nil
}

func (q *Queue) detectPacketThresholdLoss(largest uint64, now time.Time) {
	p := q.packets
	if p == nil || largest < 4 {
		return
	}
	changed := false
	for i := range p.records {
		r := &p.records[i]
		if r.state != packetInFlight || largest <= r.number || largest-r.number < 5 {
			continue
		}
		r.state = packetLost
		p.wire -= int(r.wire)
		p.count--
		p.stats.ThresholdLost++
		changed = true
		if q.congestion != nil {
			q.congestion.onTimeout(p.at(r.sent), now)
		}
		if sender := q.byID[r.id]; sender != nil {
			sender.forceRetry(int(r.offset), int(r.length), now)
		}
	}
	if changed {
		p.nextExpiry = 0
		for i := range p.records {
			r := &p.records[i]
			if r.state == packetInFlight && (p.nextExpiry == 0 || r.expires < p.nextExpiry) {
				p.nextExpiry = r.expires
			}
		}
	}
}

func (q *Queue) ackPacketRecord(r *packetRecord, now time.Time) error {
	p := q.packets
	if p == nil {
		return ErrFrame
	}
	if r.state == packetAcked {
		p.acknowledge(r)
		return nil
	}
	before := p.wire
	wasFlight := r.state == packetInFlight
	sentAt := p.at(r.sent)
	// The authenticated packet number identifies the exact logical bytes; batch
	// ACKs do not need to repeat message metadata for every transmission.
	var length [4]byte
	binary.BigEndian.PutUint32(length[:], r.length)
	logical := Frame{Type: TypeAck, ID: r.id, Offset: r.offset, Total: r.total, Payload: length[:]}
	if q.byID[r.id] != nil {
		if err := q.ackLogical(logical, now, false); err != nil {
			return err
		}
	}
	p.acknowledge(r)
	if q.congestion != nil && wasFlight {
		q.congestion.refill(now, q.rtt)
		eligible := q.congestion.recoveryAt.IsZero() || sentAt.After(q.congestion.recoveryAt)
		q.congestion.onAck(int(r.wire), before, eligible)
	}
	if wasFlight && !now.IsZero() && now.After(sentAt) && !now.Before(q.nextRTTSample) {
		// Exact physical receipts disambiguate retransmissions; no Karn exclusion.
		q.rtt.Observe(now.Sub(sentAt))
		q.nextRTTSample = now.Add(max(time.Millisecond, q.rtt.smoothed))
	}
	return nil
}
