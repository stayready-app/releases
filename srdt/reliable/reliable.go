// Package reliable implements bounded, selective-acknowledgment message delivery.
// It is transport-independent: callers encrypt and transmit encoded frames.
package reliable

import (
	"bytes"
	"encoding/binary"
	"errors"
	"math/bits"
	"time"
)

const (
	TypeData                byte = 1
	TypeAck                 byte = 2
	TypeOpen                byte = 3  // Request a receiver reservation for one logical message.
	TypeGrant               byte = 4  // Reservation made; DATA may now be sent.
	TypeBlocked             byte = 5  // Request pending; Offset is a bounded probe delay in ms.
	TypeReject              byte = 6  // Request cannot be admitted; Offset is a reason code.
	TypeWindowOpen          byte = 7  // Cumulative requested message ID; Total is per-message capacity.
	TypePacketData          byte = 9  // DATA with an authenticated physical transmission number.
	TypePacketAck           byte = 10 // Legacy receipt of one exact transmission plus its logical byte range.
	TypePacketAckBatch      byte = 11 // Authenticated receipt of a small set of exact physical transmission numbers.
	TypeStreamOpen          byte = 12 // Open independent bidirectional byte streams; Total=chunk capacity, Offset=max active chunks.
	TypeStreamOpenAck       byte = 13 // Acknowledge StreamOpen with the negotiated fixed stream parameters.
	TypeStreamFin           byte = 14 // Half-close one direction; ID is the final logical message ID (or zero), Offset is direction.
	TypeStreamFinAck        byte = 15 // Acknowledge StreamFin.
	TypeStreamReset         byte = 16 // Abort the stream application; Offset is a bounded reason code.
	TypePathPing            byte = 17 // Authenticated path-liveness probe; ID is a nonzero probe token.
	TypePathPong            byte = 18 // Authenticated response carrying the PathPing token.
	TypeWindowGrant         byte = 8  // Cumulative reserved message ID; Total is immutable capacity.
	HeaderSize                   = 1 + 8 + 4 + 4
	MaxMessageSize               = 8 << 20
	MaxActiveMessages            = 64
	MaxActiveBytes               = 32 << 20
	MaxCompletedMessages         = 4096
	PacketAckBatchMax            = 4
	PacketAckBatchThreshold      = 2
)

var ErrFrame = errors.New("invalid reliable frame")
var ErrStale = errors.New("retired message ID")
var ErrCreditRequired = errors.New("message has no receiver reservation")
var ErrReceiveFull = errors.New("reliable receive capacity exhausted")

type Frame struct {
	Type         byte
	ID           uint64
	Offset       uint32
	Total        uint32
	Payload      []byte
	PacketNumber uint64 // Nonzero only in TypePacketData/TypePacketAck. Never reused in a direction.
}

func validateStreamControl(f Frame) (bool, error) {
	switch f.Type {
	case TypeStreamOpen, TypeStreamOpenAck:
		if f.ID != 0 || f.Total == 0 || f.Total > MaxMessageSize || f.Offset == 0 || f.Offset > MaxActiveMessages || len(f.Payload) != 0 || f.PacketNumber != 0 {
			return true, ErrFrame
		}
		return true, nil
	case TypeStreamFin, TypeStreamFinAck:
		if f.Total != 0 || len(f.Payload) != 0 || f.PacketNumber != 0 || (f.Offset != 1 && f.Offset != 2) {
			return true, ErrFrame
		}
		if f.ID != 0 {
			if (f.Offset == 1 && f.ID%2 == 0) || (f.Offset == 2 && f.ID%2 != 0) {
				return true, ErrFrame
			}
		}
		return true, nil
	case TypeStreamReset:
		if f.ID != 0 || f.Total != 0 || len(f.Payload) != 0 || f.PacketNumber != 0 || f.Offset == 0 || f.Offset > 4 {
			return true, ErrFrame
		}
		return true, nil
	case TypePathPing, TypePathPong:
		if f.ID == 0 || f.Offset != 0 || f.Total != 0 || len(f.Payload) != 0 || f.PacketNumber != 0 {
			return true, ErrFrame
		}
		return true, nil
	default:
		return false, nil
	}
}

func validateFrame(f Frame) error {
	if handled, err := validateStreamControl(f); handled {
		return err
	}
	if f.Type == TypePacketAckBatch {
		if f.PacketNumber != 0 || f.ID != 0 || f.Offset != 0 || f.Total == 0 || f.Total > PacketAckBatchMax || len(f.Payload) != int(f.Total)*8 {
			return ErrFrame
		}
		for i := 0; i < int(f.Total); i++ {
			number := binary.BigEndian.Uint64(f.Payload[i*8:])
			if number == 0 {
				return ErrFrame
			}
			for j := 0; j < i; j++ {
				if binary.BigEndian.Uint64(f.Payload[j*8:]) == number {
					return ErrFrame
				}
			}
		}
		return nil
	}
	if f.Type == TypePacketData || f.Type == TypePacketAck {
		if f.PacketNumber == 0 {
			return ErrFrame
		}
		if f.Type == TypePacketData {
			f.Type = TypeData
		} else {
			f.Type = TypeAck
		}
		f.PacketNumber = 0
	} else if f.PacketNumber != 0 {
		return ErrFrame
	}
	if f.Type >= TypeOpen && f.Type <= TypeWindowGrant {
		if f.ID == 0 || f.Total == 0 || f.Total > MaxMessageSize || len(f.Payload) != 0 {
			return ErrFrame
		}
		switch f.Type {
		case TypeOpen, TypeGrant, TypeWindowOpen, TypeWindowGrant:
			if f.Offset != 0 {
				return ErrFrame
			}
		case TypeBlocked:
			if f.Offset < 100 || f.Offset > 1000 {
				return ErrFrame
			}
		case TypeReject:
			if f.Offset < 1 || f.Offset > 3 {
				return ErrFrame
			}
		}
		return nil
	}
	if f.Type != TypeData && f.Type != TypeAck {
		return ErrFrame
	}
	if f.Total == 0 || f.Total > MaxMessageSize || f.Offset > f.Total {
		return ErrFrame
	}
	if f.Type == TypeData && (len(f.Payload) == 0 || uint64(f.Offset)+uint64(len(f.Payload)) > uint64(f.Total)) {
		return ErrFrame
	}
	if f.Type == TypeAck && (len(f.Payload) != 4 || f.Offset >= f.Total || binary.BigEndian.Uint32(f.Payload) == 0 || uint64(f.Offset)+uint64(binary.BigEndian.Uint32(f.Payload)) > uint64(f.Total)) {
		return ErrFrame
	}
	return nil
}

func Encode(f Frame) ([]byte, error) { return EncodeInto(nil, f) }

// EncodeInto reuses dst; dst must not alias f.Payload.
func EncodeInto(dst []byte, f Frame) ([]byte, error) {
	if err := validateFrame(f); err != nil {
		return nil, err
	}
	size := HeaderSize + len(f.Payload)
	if f.PacketNumber != 0 {
		size += 8
	}
	var b []byte
	if cap(dst) < size {
		b = make([]byte, size)
	} else {
		b = dst[:size]
	}
	b[0] = f.Type
	binary.BigEndian.PutUint64(b[1:], f.ID)
	binary.BigEndian.PutUint32(b[9:], f.Offset)
	binary.BigEndian.PutUint32(b[13:], f.Total)
	offset := HeaderSize
	if f.PacketNumber != 0 {
		binary.BigEndian.PutUint64(b[offset:], f.PacketNumber)
		offset += 8
	}
	copy(b[offset:], f.Payload)
	return b, nil
}

func Decode(b []byte) (Frame, error) {
	var err error
	b, err = unwrap(b)
	if err != nil {
		return Frame{}, err
	}
	if len(b) < HeaderSize {
		return Frame{}, ErrFrame
	}
	f := Frame{Type: b[0], ID: binary.BigEndian.Uint64(b[1:]), Offset: binary.BigEndian.Uint32(b[9:]), Total: binary.BigEndian.Uint32(b[13:]), Payload: b[HeaderSize:]}
	if f.Type == TypePacketData || f.Type == TypePacketAck {
		if len(f.Payload) < 8 {
			return Frame{}, ErrFrame
		}
		f.PacketNumber = binary.BigEndian.Uint64(f.Payload)
		f.Payload = f.Payload[8:]
	}
	if err := validateFrame(f); err != nil {
		return Frame{}, err
	}
	return f, nil
}

type receivedMessage struct {
	total    uint32 // Reserved length/capacity before the first DATA arrives.
	flexible bool   // A rolling-window reservation may bind to a smaller length once.
	data     []byte
	got      []uint64 // compact bitmap
	count    int
	charge   int64
}

type ReceiverLimits struct {
	MaxActiveMessages    int
	MaxActiveBytes       int
	MaxCompletedMessages int
}

// LowMemoryReceiverLimits is intended for resource-constrained nodes.
func LowMemoryReceiverLimits() ReceiverLimits {
	return ReceiverLimits{MaxActiveMessages: 8, MaxActiveBytes: 256 << 10, MaxCompletedMessages: 128}
}

type Receiver struct {
	limits           ReceiverLimits
	budget           *Budget
	active           map[uint64]*receivedMessage
	completed        map[uint64]uint32
	activeBytes      int
	activeCharge     int64
	acceptedBytes    uint64
	completedOrder   []uint64
	completedCursor  int
	retired          uint64
	hasRetired       bool
	closed           bool
	credits          bool
	batchPacketAcks  bool
	packetAckWarmup  int
	packetAckCount   int
	packetAckNumbers [PacketAckBatchMax]uint64
	packetAckPayload [PacketAckBatchMax * 8]byte
	packetAckFrames  [1]Frame
	packetAckDue     time.Time
}

func NewReceiver() *Receiver {
	r, _ := NewReceiverWithLimits(ReceiverLimits{MaxActiveMessages, MaxActiveBytes, MaxCompletedMessages})
	return r
}

// NewReceiverWithLimits bounds per-session allocations and replay metadata.
func NewReceiverWithLimits(l ReceiverLimits) (*Receiver, error) {
	if l.MaxActiveMessages <= 0 || l.MaxActiveBytes <= 0 || l.MaxCompletedMessages <= 0 || l.MaxActiveBytes > MaxActiveBytes || l.MaxActiveMessages > MaxActiveMessages || l.MaxCompletedMessages > MaxCompletedMessages {
		return nil, ErrFrame
	}
	return &Receiver{limits: l, active: make(map[uint64]*receivedMessage), completed: make(map[uint64]uint32)}, nil
}

// SetBudget attaches a shared budget before any messages are accepted.
// Receivers must not change budgets while holding unfinished messages.
func (r *Receiver) SetBudget(b *Budget) error {
	if r.activeBytes != 0 || r.closed {
		return ErrFrame
	}
	r.budget = b
	return nil
}

// EnablePacketAckBatching compresses exact physical receipts into bounded ACK
// frames. It must be selected before DATA. The receiver remains single-owner;
// returned ACK payload storage is reused by the next Accept call.
func (r *Receiver) EnablePacketAckBatching() error {
	if r.closed || r.activeBytes != 0 || r.acceptedBytes != 0 || r.packetAckCount != 0 {
		return ErrFrame
	}
	r.batchPacketAcks = true
	// Seed RTT/pacing with immediate receipts. Delaying the very first ACK can
	// make a fast path look milliseconds slower and throttle the whole session.
	r.packetAckWarmup = 2
	return nil
}

// Close releases all outstanding reservations. It must not race with Accept.
func (r *Receiver) Close() {
	if r.budget != nil {
		r.budget.release(r.activeCharge)
	}
	r.activeBytes = 0
	r.activeCharge = 0
	r.active = make(map[uint64]*receivedMessage)
	r.completed = make(map[uint64]uint32)
	r.completedOrder = nil
	r.completedCursor = 0
	r.packetAckWarmup = 0
	r.packetAckCount = 0
	clear(r.packetAckNumbers[:])
	clear(r.packetAckPayload[:])
	r.packetAckFrames[0] = Frame{}
	r.packetAckDue = time.Time{}
	// Close releases memory, not the lifetime replay watermark. Reuse requires
	// constructing a new Receiver for a NEW authenticated session.
	r.closed = true
}

// ActiveBytes returns the current memory budget charged to unfinished payloads.
func (r *Receiver) ActiveBytes() int { return r.activeBytes }

// Accept emits acknowledgments for every valid byte, including duplicates.
// Completed IDs are retained in a bounded replay cache. IDs must never be
// reused during a session, including after eviction; session lifetimes and
// sender ID allocation must enforce this requirement.
func (r *Receiver) Accept(f Frame) ([]Frame, []byte, error) {
	return r.AcceptAt(f, time.Now())
}

func (r *Receiver) AcceptAt(f Frame, now time.Time) ([]Frame, []byte, error) {
	acks, owned, err := r.AcceptRetainedAt(f, now)
	if owned == nil {
		return acks, nil, err
	}
	data := owned.Data
	owned.Release()
	return acks, data, err
}

// AcceptRetained keeps completed payload/bitmap arrays charged to Budget until
// the caller releases them or transfers ownership to Queue.EnqueueReceived.
func (r *Receiver) AcceptRetained(f Frame) ([]Frame, *Received, error) {
	return r.AcceptRetainedAt(f, time.Now())
}

func (r *Receiver) AcceptRetainedAt(f Frame, now time.Time) ([]Frame, *Received, error) {
	if f.Type == TypePacketData {
		if validateFrame(f) != nil {
			return nil, nil, ErrFrame
		}
		f.Type = TypeData // Keep PacketNumber for the exact receipt; logical reassembly is unchanged.
	} else if f.PacketNumber != 0 {
		return nil, nil, ErrFrame
	}
	if r.closed || f.Type != TypeData || len(f.Payload) == 0 || f.Total == 0 || f.Total > MaxMessageSize || uint64(f.Offset)+uint64(len(f.Payload)) > uint64(f.Total) {
		return nil, nil, ErrFrame
	}
	if total, ok := r.completed[f.ID]; ok {
		if total != f.Total {
			return nil, nil, ErrFrame
		}
		return r.acknowledgments(f, true, now), nil, nil
	}
	if r.credits && r.active[f.ID] == nil {
		return nil, nil, ErrCreditRequired
	}
	if m := r.active[f.ID]; m != nil && m.flexible {
		// Bind once. Until this authenticated DATA arrives the FULL offered
		// capacity is reserved. Shrinking cannot overcommit an existing grant.
		if f.Total > m.total {
			return nil, nil, ErrFrame
		}
		charge := BufferCharge(int(f.Total))
		r.activeBytes -= int(m.total - f.Total)
		r.activeCharge -= m.charge - charge
		if r.budget != nil {
			r.budget.release(m.charge - charge)
		}
		m.total, m.charge, m.flexible = f.Total, charge, false
	}
	if err := r.Reserve(f.ID, f.Total); err != nil {
		return nil, nil, err
	}
	m := r.active[f.ID]
	if m.data == nil {
		m.data = make([]byte, m.total)
		m.got = make([]uint64, (int(m.total)+63)/64)
	}
	// Validate ALL overlapping bytes before committing any change. A rejected
	// frame must not poison a partially assembled message or its progress timer.
	start, end := int(f.Offset), int(f.Offset)+len(f.Payload)
	for pos := start; pos < end; {
		limit := min(end, (pos/64+1)*64)
		mask := wordMask(pos%64, limit-pos)
		overlap := m.got[pos/64] & mask
		if overlap == mask {
			if !bytes.Equal(m.data[pos:limit], f.Payload[pos-start:limit-start]) {
				return nil, nil, ErrFrame
			}
		} else {
			for overlap != 0 {
				bit := bits.TrailingZeros64(overlap)
				at := pos/64*64 + bit
				if m.data[at] != f.Payload[at-start] {
					return nil, nil, ErrFrame
				}
				overlap &= overlap - 1
			}
		}
		pos = limit
	}
	copy(m.data[start:end], f.Payload)
	added := markBits(m.got, start, end)
	m.count += added
	r.acceptedBytes += uint64(added)

	complete := m.count == len(m.data)
	acks := r.acknowledgments(f, complete, now)
	if !complete {
		return acks, nil, nil
	}
	delete(r.active, f.ID)
	r.activeBytes -= len(m.data)
	r.activeCharge -= m.charge
	r.completed[f.ID] = f.Total
	if len(r.completedOrder) < r.limits.MaxCompletedMessages {
		r.completedOrder = append(r.completedOrder, f.ID)
	} else {
		oldest := r.completedOrder[r.completedCursor]
		r.completedOrder[r.completedCursor] = f.ID
		r.completedCursor = (r.completedCursor + 1) % len(r.completedOrder)
		delete(r.completed, oldest)
		if !r.hasRetired || oldest > r.retired {
			r.retired, r.hasRetired = oldest, true
		}
	}

	return acks, &Received{Data: m.data, original: m.data, bitmap: m.got, budget: r.budget, charge: m.charge}, nil
}

func acknowledgments(f Frame) []Frame {
	b := make([]byte, 4)
	binary.BigEndian.PutUint32(b, uint32(len(f.Payload)))
	kind := TypeAck
	if f.PacketNumber != 0 {
		kind = TypePacketAck
	}
	return []Frame{{Type: kind, ID: f.ID, Offset: f.Offset, Total: f.Total, Payload: b, PacketNumber: f.PacketNumber}}
}

func (r *Receiver) acknowledgments(f Frame, force bool, now time.Time) []Frame {
	if f.PacketNumber == 0 || !r.batchPacketAcks {
		return acknowledgments(f)
	}
	if r.packetAckWarmup > 0 {
		r.packetAckWarmup--
		return acknowledgments(f)
	}
	for i := 0; i < r.packetAckCount; i++ {
		if r.packetAckNumbers[i] == f.PacketNumber {
			if !force {
				return nil
			}
			return r.flushPacketAcks()
		}
	}
	if r.packetAckCount == PacketAckBatchMax {
		// This should not occur with the threshold below, but fail closed without
		// losing authority for already collected authenticated packet numbers.
		return r.flushPacketAcks()
	}
	r.packetAckNumbers[r.packetAckCount] = f.PacketNumber
	r.packetAckCount++
	if r.packetAckCount == 1 {
		r.packetAckDue = now.Add(5 * time.Millisecond)
	}
	if r.packetAckCount < PacketAckBatchThreshold && !force {
		return nil
	}
	return r.flushPacketAcks()
}

func (r *Receiver) flushPacketAcks() []Frame {
	if r.packetAckCount == 0 {
		return nil
	}
	for i := 0; i < r.packetAckCount; i++ {
		binary.BigEndian.PutUint64(r.packetAckPayload[i*8:], r.packetAckNumbers[i])
		r.packetAckNumbers[i] = 0
	}
	r.packetAckFrames[0] = Frame{Type: TypePacketAckBatch, Total: uint32(r.packetAckCount), Payload: r.packetAckPayload[:r.packetAckCount*8]}
	r.packetAckCount = 0
	r.packetAckDue = time.Time{}
	return r.packetAckFrames[:1]
}

// PacketAckWake lets an external event loop flush a partial ACK batch without
// per-session goroutines or timers.
func (r *Receiver) PacketAckWake() (time.Time, bool) {
	if !r.batchPacketAcks || r.packetAckCount == 0 || r.packetAckDue.IsZero() {
		return time.Time{}, false
	}
	return r.packetAckDue, true
}

func (r *Receiver) FlushPacketAcks(now time.Time) []Frame {
	if !r.batchPacketAcks || r.packetAckCount == 0 || r.packetAckDue.IsZero() || now.Before(r.packetAckDue) {
		return nil
	}
	return r.flushPacketAcks()
}

// Known identifies an active or recently completed message without allocating.
func (r *Receiver) Known(id uint64) bool {
	_, active := r.active[id]
	_, completed := r.completed[id]
	return active || completed
}
func (r *Receiver) ActiveMessages() int { return len(r.active) }

// Received owns retained buffer credit. Treat Data as immutable until ownership
// transfer, and do not retain a reference after transfer. Not concurrent-safe.
type Received struct {
	Data     []byte
	original []byte
	bitmap   []uint64
	budget   *Budget
	charge   int64
	released bool
}

func (m *Received) Release() {
	if m == nil || m.released {
		return
	}
	if m.budget != nil {
		m.budget.release(m.charge)
	}
	m.Data, m.original, m.bitmap = nil, nil, nil
	m.released = true
}

// AcceptedBytes counts distinct payload bytes, not retransmitted duplicates.
func (r *Receiver) AcceptedBytes() uint64 { return r.acceptedBytes }

// Reserve admits a whole message before DATA transmission. It accounts for the
// eventual payload, bitmap AND retransmit metadata without allocating arrays.
// Reservations cannot be revoked while a session is live: a delayed GRANT must
// never authorize DATA against released/reused memory. Close releases them all.
func (r *Receiver) Reserve(id uint64, total uint32) error {
	if r.closed || total == 0 || total > MaxMessageSize {
		return ErrFrame
	}
	if known, ok := r.Total(id); ok {
		if known != total {
			return ErrFrame
		}
		return nil
	}
	if r.hasRetired && id <= r.retired {
		return ErrStale
	}
	if len(r.active) >= r.limits.MaxActiveMessages || int(total) > r.limits.MaxActiveBytes-r.activeBytes {
		return ErrReceiveFull
	}
	charge := BufferCharge(int(total))
	if r.budget != nil && !r.budget.reserve(charge) {
		return ErrBudgetExceeded
	}
	r.active[id] = &receivedMessage{total: total, charge: charge}
	r.activeBytes += int(total)
	r.activeCharge += charge
	return nil
}

// Total returns immutable reservation metadata for active or recently completed
// messages. It does not allocate or expose the underlying receive buffers.
func (r *Receiver) Total(id uint64) (uint32, bool) {
	if m := r.active[id]; m != nil {
		return m.total, true
	}
	total, ok := r.completed[id]
	return total, ok
}

// RequireCredits disables implicit admission by DATA. Enable on a fresh receiver
// before use; future carrier adapters cannot accidentally bypass reservation.
func (r *Receiver) RequireCredits() error {
	if r.closed || r.activeBytes != 0 {
		return ErrFrame
	}
	r.credits = true
	return nil
}

// ReserveCapacity reserves a single future message ID up to capacity bytes.
// The first valid DATA binds its immutable actual length. Grants are irrevocable
// within a session; a new ID is required to reuse released space. Call only when
// advancing the grant watermark, never to regenerate a cumulative grant.
func (r *Receiver) ReserveCapacity(id uint64, capacity uint32) error {
	if r.Known(id) {
		return ErrFrame
	}
	if err := r.Reserve(id, capacity); err != nil {
		return err
	}
	r.active[id].flexible = true
	return nil
}
