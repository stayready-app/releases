package netreliable

import (
	"bytes"
	"context"
	"example.com/srdt/emulator"
	"testing"
	"time"
)

// Both endpoints, real UDP relay and same deterministic bottleneck parameters.
// No speed assertions: process scheduling is intentionally not deterministic.
func TestCongestionRealUDPBottleneck(t *testing.T) {
	for _, disabled := range []bool{false, true} {
		name := "controlled"
		if disabled {
			name = "uncontrolled-lab-baseline"
		}
		t.Run(name, func(t *testing.T) {
			_, identity, c := testMulti(t)
			c.AllowLegacyData = false
			c.Options.DisableCongestion = disabled
			server, addr, stop := startMulti(t, c, identity)
			defer stop()
			link := emulator.Config{RateBytesPerSecond: 250000, BottleneckBytes: 6000, DelayMinMs: 20, DelayMaxMs: 20}
			proxy, stopProxy := faultProxy(t, addr, faultSpec{up: link, down: link})
			defer stopProxy()
			data := make([][]byte, 64)
			for n := range data {
				data[n] = bytes.Repeat([]byte{byte(n)}, 4096)
			}
			begin := time.Now()
			if err := ExchangeBatch(context.Background(), proxy, identity.Public, identity.PSK, data, Options{Timeout: 45 * time.Second, DisableCongestion: disabled}); err != nil {
				t.Fatal(err)
			}
			stats := server.Stats()
			t.Logf("verified=%d elapsed_ms=%d stats=%+v", len(data)*4096, time.Since(begin).Milliseconds(), stats)
			if stats.DeliveredMessages != 64 || stats.ResourceDrops != 0 || stats.CreditViolations != 0 {
				t.Fatal(stats)
			}
			if !disabled && stats.DataFreshPackets == 0 {
				t.Fatal("controller not active")
			}
			if disabled && stats.DataFreshPackets != 0 {
				t.Fatal("laboratory opt-out ignored")
			}
		})
	}
}

func TestCongestionRealUDPReverseAsymmetricBottleneck(t *testing.T) {
	_, identity, c := testMulti(t)
	c.AllowLegacyData = false
	server, addr, stop := startMulti(t, c, identity)
	defer stop()
	proxy, stopProxy := faultProxy(t, addr, faultSpec{
		up:   emulator.Config{RateBytesPerSecond: 1000000, BottleneckBytes: 16000, DelayMinMs: 10, DelayMaxMs: 30},
		down: emulator.Config{RateBytesPerSecond: 100000, BottleneckBytes: 4000, DelayMinMs: 20, DelayMaxMs: 40, LossPercent: 1}, inject: true,
	})
	defer stopProxy()
	data := make([][]byte, 16)
	for n := range data {
		data[n] = bytes.Repeat([]byte{byte(n)}, 8192)
	}
	if err := ExchangeBatch(context.Background(), proxy, identity.Public, identity.PSK, data, Options{Timeout: 30 * time.Second}); err != nil {
		t.Fatal(err)
	}
	if s := server.Stats(); s.DeliveredMessages != 16 || s.ResourceDrops != 0 {
		t.Fatal(s)
	}
}
