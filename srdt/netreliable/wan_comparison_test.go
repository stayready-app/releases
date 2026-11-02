package netreliable

import (
	"bytes"
	"context"
	"example.com/srdt/emulator"
	"testing"
	"time"
)

// The same test is copied into the baseline source for a paired run. This is a
// synthetic loopback relay delay, not a remote WAN benchmark. No timing asserts.
func TestStage16WANComparison(t *testing.T) {
	_, i, c := testMulti(t)
	s, addr, stop := startMulti(t, c, i)
	defer stop()
	delay := emulator.Config{DelayMinMs: 40, DelayMaxMs: 40}
	proxy, stopProxy := faultProxy(t, addr, faultSpec{up: delay, down: delay})
	defer stopProxy()
	messages := make([][]byte, 32)
	for n := range messages {
		messages[n] = bytes.Repeat([]byte{byte(n)}, 4096)
	}
	start := time.Now()
	if err := ExchangeBatch(context.Background(), proxy, i.Public, i.PSK, messages, Options{Timeout: 30 * time.Second}); err != nil {
		t.Fatal(err)
	}
	t.Logf("verified_bytes=%d elapsed_ms=%d stats=%+v", len(messages)*4096, time.Since(start).Milliseconds(), s.Stats())
}
