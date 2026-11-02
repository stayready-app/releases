package reliable

import "time"

// RTTEstimator uses smoothed RTT and variation (Jacobson/Karels). This UDP
// experiment uses a 40ms floor, NOT RFC 6298's TCP one-second lower bound.
// It is not congestion control. A clock and receipt time are supplied by caller.
type RTTEstimator struct {
	smoothed  time.Duration
	variation time.Duration
	samples   uint64
}

const (
	InitialRTO = 300 * time.Millisecond
	MinimumRTO = 40 * time.Millisecond
	MaximumRTO = 3 * time.Second
)

func (r *RTTEstimator) Observe(sample time.Duration) {
	if sample <= 0 {
		return
	}
	// Bound input before arithmetic; an authenticated slow peer cannot overflow.
	sample = min(sample, MaximumRTO)
	if r.samples == 0 {
		r.smoothed, r.variation = sample, sample/2
	} else {
		delta := r.smoothed - sample
		if delta < 0 {
			delta = -delta
		}
		r.variation = (3*r.variation + delta) / 4
		r.smoothed = (7*r.smoothed + sample) / 8
	}
	r.samples++
}

func (r *RTTEstimator) RTO() time.Duration {
	if r.samples == 0 {
		return InitialRTO
	}
	return max(MinimumRTO, min(MaximumRTO, r.smoothed+max(time.Millisecond, 4*r.variation)))
}

func (r *RTTEstimator) Samples() uint64 { return r.samples }
