package reliable

import (
	"time"
)

// CongestionStats describes one sending direction. In legacy mode WindowBytes
// is logical payload; with packet receipts it is estimated IP+UDP+SRDT bytes.
// PacketStats separately reports exact tracked transmission counts and charges.
type CongestionStats struct {
	WindowBytes     int    `json:"window_bytes"`
	PeakWindowBytes int    `json:"peak_window_bytes"`
	FreshPackets    uint64 `json:"fresh_packets"`
	RetryPackets    uint64 `json:"retry_packets"`
	RetryBytes      uint64 `json:"retry_payload_bytes"`
	LossEvents      uint64 `json:"timeout_loss_events"`
	PacingWaits     uint64 `json:"pacing_waits"`
	WindowWaits     uint64 `json:"window_waits"`
}

// congestion is a conservative, experimental loss/window controller, not QUIC
// or a claim of RFC 9002 compliance. It creates no timers or goroutines.
// Legacy mode uses range ACKs; Stage 19's bounded packetLedger supplies exact
// physical receipts, flight charges and per-transmission timeout evidence.
// There is not yet packet-threshold loss detection, PTO probing or ECN.
type congestion struct {
	mss, overhead, maximum, initial, minimum int
	window, threshold, ackCredit             int
	recoveryAt                               time.Time
	lastSent, refillAt                       time.Time
	tokens                                   float64
	stats                                    CongestionStats
}

const congestionBurstPackets = 4
const congestionMaximumBurstPackets = 64
const congestionPacingQuantum = time.Millisecond
const congestionInitialRTT = 100 * time.Millisecond

func newCongestion(maximum, mss, overhead int) (*congestion, error) {
	if maximum < 1 || mss < 1 || mss > 65535 || overhead < 0 || overhead > 4096 {
		return nil, ErrFrame
	}
	c := &congestion{mss: mss, overhead: overhead, maximum: maximum,
		initial: min(maximum, 10*mss), minimum: min(maximum, 2*mss), threshold: maximum}
	c.window = c.initial
	c.tokens = float64(2 * (mss + overhead))
	c.stats.PeakWindowBytes = c.window
	return c, nil
}

func (c *congestion) rtt(r *RTTEstimator) time.Duration {
	if r == nil || r.samples == 0 {
		return congestionInitialRTT
	}
	// Avoid runaway pacing from unrealistically tiny loopback/ACK-compressed RTT.
	return max(100*time.Microsecond, r.smoothed)
}
func (c *congestion) rate(r *RTTEstimator) float64 {
	// Convert logical-byte window into estimated on-wire bytes/second. No gain
	// above window/RTT: control traffic is already sent separately.
	return float64(c.window) * float64(c.mss+c.overhead) / float64(c.mss) / c.rtt(r).Seconds()
}

// burstLimit amortizes timer granularity without buying an unbounded burst.
// On a slow path the four-packet limit is unchanged. On a fast path, permit
// at most one scheduler quantum, one congestion window, and 64 full packets.
// This is a cap in bytes: tiny packets still pay their individual overhead.
func (c *congestion) burstLimit(r *RTTEstimator) float64 {
	unit := float64(c.mss + c.overhead)
	window := float64(c.window) * unit / float64(c.mss)
	quantum := c.rate(r) * congestionPacingQuantum.Seconds()
	return max(float64(congestionBurstPackets)*unit,
		min(window, quantum, float64(congestionMaximumBurstPackets)*unit))
}
func (c *congestion) refill(now time.Time, r *RTTEstimator) {
	if c.refillAt.IsZero() {
		c.refillAt = now
		return
	}
	if now.After(c.refillAt) {
		c.tokens = min(c.burstLimit(r), c.tokens+now.Sub(c.refillAt).Seconds()*c.rate(r))
		c.refillAt = now
	}
}
func (c *congestion) readyAt(now time.Time, payload int, r *RTTEstimator) time.Time {
	c.refill(now, r)
	need := float64(min(c.mss, payload)+c.overhead) - c.tokens
	if need <= 0 {
		return now
	}
	wait := time.Duration(need/c.rate(r)*float64(time.Second)) + time.Nanosecond
	return now.Add(max(time.Microsecond, wait))
}
func (c *congestion) onSend(now time.Time, payload int, retry bool, r *RTTEstimator, rto time.Duration) {
	// Idle periods do not buy an unlimited burst or retain a formerly large cwnd.
	if !c.lastSent.IsZero() && now.Sub(c.lastSent) >= max(rto, congestionInitialRTT) {
		c.window = min(c.window, c.initial)
		c.ackCredit = 0
		c.tokens = min(c.tokens, float64(2*(c.mss+c.overhead)))
	}
	c.refill(now, r)
	c.tokens -= float64(payload + c.overhead)
	c.lastSent = now
	if retry {
		c.stats.RetryPackets++
		c.stats.RetryBytes += uint64(payload)
	} else {
		c.stats.FreshPackets++
	}
}
func (c *congestion) onTimeout(sentAt, now time.Time) {
	// A burst of expired ranges from one original flight is ONE congestion event,
	// not one reduction per fragment. A later timed-out retry starts a new epoch.
	if !c.recoveryAt.IsZero() && !sentAt.After(c.recoveryAt) {
		return
	}
	c.threshold = max(c.minimum, c.window/2)
	c.window = c.minimum // RTO, not fast loss detection: use conservative restart.
	c.ackCredit = 0
	c.recoveryAt = now
	c.tokens = min(c.tokens, float64(c.mss+c.overhead))
	c.stats.LossEvents++
}
func (c *congestion) onAck(added, before int, eligible bool) {
	if added <= 0 || !eligible || before < c.window/2 {
		return
	}
	if c.window < c.threshold {
		c.window = min(c.maximum, c.window+added)
	} else {
		// Appropriate-byte counting. Duplicated/split ACKs cannot repeatedly credit
		// the same payload. At most roughly one MSS growth per window of new bytes.
		c.ackCredit += added
		for c.ackCredit >= c.window && c.window < c.maximum {
			c.ackCredit -= c.window
			c.window = min(c.maximum, c.window+c.mss)
		}
	}
	if c.window == c.maximum {
		c.ackCredit = 0
	}
	c.stats.PeakWindowBytes = max(c.stats.PeakWindowBytes, c.window)
}
