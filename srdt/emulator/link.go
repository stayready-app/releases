package emulator

import (
	"errors"
	"math"
	"time"
)

// Link models a FIFO serializer and a byte-bounded tail-drop queue. It owns no
// payload, goroutine or timer. Delayed packets in flight after serialization do
// not occupy its queue. It is a deterministic laboratory bottleneck, not an IP
// fragmentation, kernel qdisc, or radio-link simulator.
type Link struct {
	rate     int64
	capacity int
	finish   time.Duration
	drops    uint64
	peak     int
}

func NewLink(bytesPerSecond int64, queueBytes int) (*Link, error) {
	if bytesPerSecond < 1 || bytesPerSecond > 1<<40 || queueBytes < 1 || queueBytes > 64<<20 {
		return nil, errors.New("invalid link rate/queue")
	}
	return &Link{rate: bytesPerSecond, capacity: queueBytes}, nil
}

// Schedule returns the last-bit departure time. now must be nonnegative and
// monotonic. FIFO admission/drop is calculated before committing finish.
func (l *Link) Schedule(now time.Duration, size int) (time.Duration, bool) {
	if now < 0 || size < 1 || size > l.capacity {
		l.drops++
		return 0, false
	}
	// At most capacity bytes can be queued, so use a time horizon to avoid
	// multiplying an arbitrary virtual timestamp by the rate.
	backlog := max(time.Duration(0), l.finish-now)
	limit := time.Duration(int64(l.capacity) * int64(time.Second) / l.rate)
	if backlog > limit {
		l.drops++
		return 0, false
	}
	queued := int((int64(backlog)*l.rate + int64(time.Second) - 1) / int64(time.Second))
	if queued > l.capacity-size {
		l.drops++
		return 0, false
	}
	duration := time.Duration((int64(size)*int64(time.Second) + l.rate - 1) / l.rate)
	start := max(now, l.finish)
	if start > time.Duration(math.MaxInt64)-duration {
		l.drops++
		return 0, false
	}
	l.finish = start + duration
	l.peak = max(l.peak, queued+size)
	return l.finish, true
}
func (l *Link) Stats() (drops uint64, peakQueueBytes int) { return l.drops, l.peak }
