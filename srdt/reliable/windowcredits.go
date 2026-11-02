package reliable

import "time"

// SendWindow holds constant-size cumulative credit state, not one grant object
// per message. Callers own synchronization and authenticate frames before use.
// IDs are never reused; first and step define the application's message domain.
type SendWindow struct {
	first, step                     uint64
	capacity                        int
	requested, sentThrough, granted uint64
	next                            time.Time
	backoff                         time.Duration
}

func NewSendWindow(first, step uint64, capacity int) (*SendWindow, error) {
	if first == 0 || step == 0 || capacity <= 0 || capacity > MaxMessageSize {
		return nil, ErrFrame
	}
	return &SendWindow{first: first, step: step, capacity: capacity}, nil
}

func (w *SendWindow) validID(id uint64) bool {
	return id >= w.first && (id-w.first)%w.step == 0
}

func (w *SendWindow) ValidMessage(id uint64, size int) bool {
	return w.validID(id) && size > 0 && size <= w.capacity
}

func (w *SendWindow) RequestThrough(id uint64) error {
	if !w.validID(id) {
		return ErrFrame
	}
	if id > w.requested {
		w.requested = id
		w.next = time.Time{}
		w.backoff = 0
	}
	return nil
}

func (w *SendWindow) Next(now time.Time) (Frame, bool) {
	if w.requested <= w.granted || now.Before(w.next) {
		return Frame{}, false
	}
	if w.backoff == 0 {
		w.backoff = 100 * time.Millisecond
	} else {
		w.backoff = min(time.Second, w.backoff*2)
	}
	w.next = now.Add(w.backoff)
	w.sentThrough = w.requested
	return Frame{Type: TypeWindowOpen, ID: w.requested, Total: uint32(w.capacity)}, true
}

func (w *SendWindow) Grant(f Frame) (bool, error) {
	if f.Type != TypeWindowGrant || validateFrame(f) != nil ||
		!w.validID(f.ID) || int(f.Total) != w.capacity || f.ID > w.sentThrough {
		return false, ErrFrame
	}
	if f.ID <= w.granted {
		return false, nil // A stale/reordered grant can neither revoke nor add credit.
	}
	w.granted = f.ID
	return true, nil
}

func (w *SendWindow) Allowed(id uint64, size int) bool {
	return w.ValidMessage(id, size) && id <= w.granted
}

func (w *SendWindow) Wake(now time.Time) (time.Time, bool) {
	if w.requested <= w.granted {
		return time.Time{}, false
	}
	if w.next.IsZero() || w.next.Before(now) {
		return now, true
	}
	return w.next, true
}
