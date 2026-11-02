package emulator

import (
	"container/heap"
	"errors"
	"math"
	"math/rand"
	"time"
)

type Direction string

const (
	ClientToServer Direction = "client_to_server"
	ServerToClient Direction = "server_to_client"
)

type SizeRange struct {
	Min int
	Max int
}
type Config struct {
	RateBytesPerSecond int64 // Zero disables serialization; counts UDP payload bytes.
	BottleneckBytes    int   // Link tail-drop queue capacity; paired with nonzero rate.
	LossPercent        float64
	DuplicatePercent   float64
	CorruptPercent     float64
	DelayMinMs         int64
	DelayMaxMs         int64
	DropSizes          []SizeRange
}
type Packet struct {
	Direction Direction
	Data      []byte
}
type Event struct {
	AtMs   int64
	Order  uint64
	Packet Packet
}
type queue []Event

func (q queue) Len() int { return len(q) }
func (q queue) Less(i, j int) bool {
	if q[i].AtMs == q[j].AtMs {
		return q[i].Order < q[j].Order
	}
	return q[i].AtMs < q[j].AtMs
}
func (q queue) Swap(i, j int) { q[i], q[j] = q[j], q[i] }
func (q *queue) Push(x any)   { *q = append(*q, x.(Event)) }
func (q *queue) Pop() any {
	old := *q
	n := len(old)
	v := old[n-1]
	old[n-1] = Event{}
	*q = old[:n-1]
	return v
}

type Stats struct {
	Submitted  int
	Dropped    int
	Duplicated int
	Corrupted  int
	Delivered  int
	QueueDrops int
	LinkDrops  int
}
type Emulator struct {
	rng          *rand.Rand
	configs      map[Direction]Config
	pending      queue
	now          int64
	order        uint64
	stats        map[Direction]*Stats
	limits       Limits
	pendingBytes int
	links        map[Direction]*Link
}

func validate(c Config) error {
	if (c.RateBytesPerSecond == 0) != (c.BottleneckBytes == 0) {
		return errors.New("link rate and queue must both be set")
	}
	if c.RateBytesPerSecond != 0 {
		if _, err := NewLink(c.RateBytesPerSecond, c.BottleneckBytes); err != nil {
			return err
		}
	}
	if math.IsNaN(c.LossPercent) || math.IsNaN(c.DuplicatePercent) || math.IsNaN(c.CorruptPercent) || c.DelayMaxMs == math.MaxInt64 || c.LossPercent < 0 || c.LossPercent > 100 || c.DuplicatePercent < 0 || c.DuplicatePercent > 100 || c.CorruptPercent < 0 || c.CorruptPercent > 100 || c.DelayMinMs < 0 || c.DelayMaxMs < c.DelayMinMs {
		return errors.New("invalid emulator configuration")
	}
	for _, r := range c.DropSizes {
		if r.Min < 0 || r.Max < r.Min {
			return errors.New("invalid size range")
		}
	}
	return nil
}

// Limits bound queued payload and event objects, not process RSS.
type Limits struct {
	Packets int
	Bytes   int
}

var ErrQueueFull = errors.New("emulator pending queue capacity exhausted")

func New(seed int64, up, down Config) (*Emulator, error) {
	return NewWithLimits(seed, up, down, Limits{Packets: 65536, Bytes: 64 << 20})
}
func NewWithLimits(seed int64, up, down Config, limits Limits) (*Emulator, error) {
	if limits.Packets < 1 || limits.Bytes < 1 {
		return nil, errors.New("invalid emulator limits")
	}
	// Copy configuration so caller mutations cannot change a running experiment.
	up.DropSizes = append([]SizeRange(nil), up.DropSizes...)
	down.DropSizes = append([]SizeRange(nil), down.DropSizes...)
	if err := validate(up); err != nil {
		return nil, err
	}
	if err := validate(down); err != nil {
		return nil, err
	}
	e := &Emulator{limits: limits, rng: rand.New(rand.NewSource(seed)), configs: map[Direction]Config{ClientToServer: up, ServerToClient: down}, stats: map[Direction]*Stats{ClientToServer: {}, ServerToClient: {}}}
	e.links = make(map[Direction]*Link)
	for d, c := range e.configs {
		if c.RateBytesPerSecond != 0 {
			e.links[d], _ = NewLink(c.RateBytesPerSecond, c.BottleneckBytes)
		}
	}
	heap.Init(&e.pending)
	return e, nil
}
func (e *Emulator) Stats(d Direction) Stats {
	if s := e.stats[d]; s != nil {
		return *s
	}
	return Stats{}
}
func (e *Emulator) Submit(d Direction, data []byte) error {
	c, ok := e.configs[d]
	if !ok {
		return errors.New("unknown direction")
	}
	s := e.stats[d]
	s.Submitted++
	for _, r := range c.DropSizes {
		if len(data) >= r.Min && len(data) <= r.Max {
			s.Dropped++
			return nil
		}
	}
	if e.rng.Float64()*100 < c.LossPercent {
		s.Dropped++
		return nil
	}
	n := 1
	if e.rng.Float64()*100 < c.DuplicatePercent {
		n++
		s.Duplicated++
	}
	if n > e.limits.Packets-len(e.pending) || len(data) > (e.limits.Bytes-e.pendingBytes)/n {
		s.QueueDrops++
		return ErrQueueFull
	}
	if c.DelayMaxMs > math.MaxInt64-e.now || uint64(n) > math.MaxUint64-e.order {
		return errors.New("emulator clock/order overflow")
	}
	for i := 0; i < n; i++ {
		departure := e.now
		if link := e.links[d]; link != nil {
			if e.now > (math.MaxInt64-int64(time.Millisecond))/int64(time.Millisecond) {
				return errors.New("link clock overflow")
			}
			at, accepted := link.Schedule(time.Duration(e.now)*time.Millisecond, len(data))
			if !accepted {
				s.Dropped++
				s.LinkDrops++
				continue
			}
			departure = int64(at / time.Millisecond)
			if at%time.Millisecond != 0 {
				departure++
			}
		}
		b := append([]byte(nil), data...)
		if len(b) > 0 && e.rng.Float64()*100 < c.CorruptPercent {
			b[e.rng.Intn(len(b))] ^= 1 << uint(e.rng.Intn(8))
			s.Corrupted++
		}
		delay := c.DelayMinMs
		if c.DelayMaxMs > delay {
			delay += e.rng.Int63n(c.DelayMaxMs - delay + 1)
		}
		if delay > math.MaxInt64-departure {
			return errors.New("link delivery overflow")
		}
		e.order++
		e.pendingBytes += len(b)
		heap.Push(&e.pending, Event{AtMs: departure + delay, Order: e.order, Packet: Packet{Direction: d, Data: b}})
	}
	return nil
}
func (e *Emulator) AdvanceTo(ms int64) ([]Event, error) {
	if ms < e.now {
		return nil, errors.New("time reversal")
	}
	var result []Event
	for e.pending.Len() > 0 && e.pending[0].AtMs <= ms {
		v := heap.Pop(&e.pending).(Event)
		e.pendingBytes -= len(v.Packet.Data)
		result = append(result, v)
		e.stats[v.Packet.Direction].Delivered++
	}
	e.now = ms
	return result, nil
}
func (e *Emulator) Drain() []Event {
	var result []Event
	for e.pending.Len() > 0 {
		v := heap.Pop(&e.pending).(Event)
		e.pendingBytes -= len(v.Packet.Data)
		result = append(result, v)
		e.stats[v.Packet.Direction].Delivered++
		e.now = v.AtMs
	}
	return result
}

func (e *Emulator) Pending() (packets, bytes int) { return len(e.pending), e.pendingBytes }
