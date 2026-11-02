package reliable

import (
	"errors"
	"sync/atomic"
)

var ErrBudgetExceeded = errors.New("shared buffer memory budget exceeded")

// Budget caps retained payload/bitmap arrays and reserved range descriptors shared by receivers and queues.
// It is NOT a process RSS limit: maps, allocation overhead, crypto and runtime are extra.
type Budget struct {
	limit int64
	used  atomic.Int64
}

func NewBudget(limit int64) (*Budget, error) {
	if limit <= 0 {
		return nil, ErrFrame
	}
	return &Budget{limit: limit}, nil
}

func (b *Budget) Used() int64  { return b.used.Load() }
func (b *Budget) Limit() int64 { return b.limit }

func (b *Budget) reserve(n int64) bool {
	for {
		old := b.used.Load()
		if n < 0 || n > b.limit-old {
			return false
		}
		if b.used.CompareAndSwap(old, old+n) {
			return true
		}
	}
}

func (b *Budget) release(n int64) {
	if n <= 0 {
		return
	}
	b.used.Add(-n)
}

// BufferCharge reserves payload, per-byte bitmap and bounded retransmit metadata.
// RX reserves future TX metadata too, so an ownership transfer cannot deadlock
// on a full shared budget. Descriptors are allocated lazily.
// Allocator size classes and object metadata are deliberately not estimated here.
func BufferCharge(payload int) int64 {
	if payload <= 0 || payload > MaxMessageSize {
		return 0
	}
	return int64(payload) + int64((payload+63)/64)*8 + int64(rangeLimit(payload))*rangeCharge
}
