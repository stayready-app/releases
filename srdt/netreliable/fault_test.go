package netreliable

import (
	"bytes"
	"context"
	"fmt"
	"net"
	"sync/atomic"
	"testing"
	"time"

	"example.com/srdt/emulator"
	"example.com/srdt/secureudp"
)

type faultSpec struct {
	up, down           emulator.Config
	firstWelcome       bool
	downBlackout       time.Duration
	downBlackoutAfter  time.Duration
	blackout           bool
	learnFirstUpSize   bool
	learnFirstDownSize bool
	controlBlackout    time.Duration
	dropControlUp      int
	dropControlDown    int
	inject             bool
}

// A real, separate UDP relay sits BETWEEN the endpoints, not in the sender's
// send callback. One goroutine owns the deterministic emulator and all buffers.
func faultProxy(t *testing.T, target string, spec faultSpec) (string, func()) {
	t.Helper()
	server, e := net.ResolveUDPAddr("udp", target)
	if e != nil {
		t.Fatal(e)
	}
	conn, e := net.ListenPacket("udp", "127.0.0.1:0")
	if e != nil {
		t.Fatal(e)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	var injected atomic.Int64
	go func() {
		defer close(done)
		defer conn.Close()
		e, _ := emulator.NewWithLimits(17, spec.up, spec.down, emulator.Limits{Packets: 4096, Bytes: 4 << 20})
		var client net.Addr
		buf := make([]byte, 65535)
		start := time.Now()
		first := true
		seq := 0
		controlUp, controlDown := 0, 0
		learnedUpSize := 0
		learnedDownSize := 0
		directDrops := 0
		writeDrops := 0
		for ctx.Err() == nil {
			_ = conn.SetReadDeadline(time.Now().Add(time.Millisecond))
			n, from, err := conn.ReadFrom(buf)
			if err == nil {
				direction := emulator.ClientToServer
				if from.String() == server.String() {
					direction = emulator.ServerToClient
				} else if client == nil || from.String() == client.String() {
					client = from
				} else {
					continue
				}
				drop := false
				if direction == emulator.ServerToClient && first {
					first = false
					if spec.firstWelcome {
						drop = true
					}
				}
				if spec.blackout && !first {
					drop = true
				}
				// With authenticated variable-size control padding, ACKs no longer have a
				// reliable observable wire length. Model a real reverse-path outage instead
				// of pretending a fixed-size filter still isolates ACKs.
				if direction == emulator.ServerToClient && time.Since(start) < spec.downBlackout {
					drop = true
				}
				if direction == emulator.ServerToClient && spec.downBlackoutAfter > 0 && time.Since(start) >= spec.downBlackoutAfter {
					drop = true
				}
				if direction == emulator.ClientToServer && spec.learnFirstUpSize {
					if learnedUpSize == 0 {
						learnedUpSize = n
					}
					if n == learnedUpSize {
						drop = true
					}
				}
				if direction == emulator.ServerToClient && spec.learnFirstDownSize {
					if learnedDownSize == 0 {
						learnedDownSize = n
					}
					if n == learnedDownSize {
						drop = true
					}
				}
				// Legacy controls have length 58. V1 controls are padded
				// into a broader range (up to ~350 bytes for ACK batches). This is a SIZE filter, not decryption: tiny
				// DATA of the same size can also be dropped by this rule.
				if n == 58 || (n >= 77 && n <= 360 && buf[0] == 3) {
					if time.Since(start) < spec.controlBlackout {
						drop = true
					}
					if direction == emulator.ClientToServer {
						controlUp++
						if controlUp <= spec.dropControlUp {
							drop = true
						}
					} else {
						controlDown++
						if controlDown <= spec.dropControlDown {
							drop = true
						}
					}
				}
				if drop {
					directDrops++
				}
				if !drop {
					if err := e.Submit(direction, buf[:n]); err != nil {
						t.Errorf("fault queue: %v", err)
						return
					}
				}
			} else if ne, ok := err.(net.Error); !ok || !ne.Timeout() {
				if ctx.Err() == nil {
					t.Error(err)
				}
				return
			}
			events, err := e.AdvanceTo(time.Since(start).Milliseconds())
			if err != nil {
				t.Error(err)
				return
			}
			for _, ev := range events {
				to := net.Addr(server)
				if ev.Packet.Direction == emulator.ServerToClient {
					to = client
				}
				if to == nil {
					continue
				}
				b := ev.Packet.Data
				_ = conn.SetWriteDeadline(time.Now().Add(100 * time.Millisecond))
				seq++
				if spec.inject && seq%7 == 0 && len(b) > 30 {
					// A forged copy retains routing ID but fails authentication. The valid
					// original following it must still be accepted (anti-replay not poisoned).
					fake := bytes.Clone(b)
					fake[len(fake)-1] ^= 0x80
					_, _ = conn.WriteTo(fake, to)
					injected.Add(1)
				}
				if _, err := conn.WriteTo(b, to); err != nil {
					if transientSendError(err) {
						// A scheduler/OS send timeout is one additional lost datagram,
						// not a permanent shutdown of the entire simulated network.
						writeDrops++
						continue
					}
					t.Error(err)
					return
				}
			}
		}
		up, down := e.Stats(emulator.ClientToServer), e.Stats(emulator.ServerToClient)
		t.Logf("relay: up=%+v down=%+v forged=%d direct_drops=%d write_drops=%d", up, down, injected.Load(), directDrops, writeDrops)
	}()
	stop := func() {
		cancel()
		select {
		case <-done:
		case <-time.After(time.Second):
			t.Error("relay failed to stop")
		}
	}
	return conn.LocalAddr().String(), stop
}

func TestAuditRealUDPNetworkFaults(t *testing.T) {
	noisy := emulator.Config{LossPercent: 5, DuplicatePercent: 5, CorruptPercent: 2, DelayMinMs: 1, DelayMaxMs: 15}
	filtered := emulator.Config{LossPercent: 2, DuplicatePercent: 3, DelayMaxMs: 8, DropSizes: []emulator.SizeRange{{Min: 800, Max: 1100}}}
	specs := []struct {
		name string
		spec faultSpec
	}{
		{"loss-reorder-corrupt-replay-inject", faultSpec{up: noisy, down: noisy, inject: true}},
		{"size-blackhole-and-loss", faultSpec{up: filtered, down: filtered, inject: true}},
		{"lost-welcome", faultSpec{firstWelcome: true}},
		{"wan-rtt-above-fixed-rto", faultSpec{up: emulator.Config{LossPercent: 1, DelayMinMs: 40, DelayMaxMs: 100}, down: emulator.Config{LossPercent: 1, DelayMinMs: 40, DelayMaxMs: 100}, inject: true}},
		{"reverse-path-blackout", faultSpec{downBlackout: 350 * time.Millisecond}},
		{"lost-open-and-grant", faultSpec{dropControlUp: 1, dropControlDown: 1}},
		{"credit-control-blackout", faultSpec{controlBlackout: 500 * time.Millisecond}},
	}
	for _, test := range specs {
		t.Run(test.name, func(t *testing.T) {
			_, i, c := testMulti(t)
			s, addr, stop := startMulti(t, c, i)
			defer stop()
			proxy, stopProxy := faultProxy(t, addr, test.spec)
			defer stopProxy()
			messages := make([][]byte, 16)
			for n := range messages {
				messages[n] = bytes.Repeat([]byte{byte(n), 0x5a, 0xc3, 0xff}, 2048)
			}
			ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
			defer cancel()
			if err := ExchangeBatch(ctx, proxy, i.Public, i.PSK, messages, Options{Timeout: 20 * time.Second}); err != nil {
				t.Fatal(err)
			}
			stats := s.Stats()
			t.Logf("verified=%d bytes; invalid=%d resources=%d", len(messages)*len(messages[0]), stats.InvalidPackets, stats.ResourceDrops)
			if stats.DeliveredMessages != 16 || stats.ResourceDrops != 0 || stats.CreditViolations != 0 {
				t.Fatal("duplicate/lost delivery", stats)
			}
		})
	}
}

func TestAuditPermanentBlackholeTimesOut(t *testing.T) {
	_, i, c := testMulti(t)
	_, addr, stop := startMulti(t, c, i)
	defer stop()
	proxy, stopProxy := faultProxy(t, addr, faultSpec{blackout: true})
	defer stopProxy()
	ctx, cancel := context.WithTimeout(context.Background(), 400*time.Millisecond)
	defer cancel()
	start := time.Now()
	err := ExchangeBatch(ctx, proxy, i.Public, i.PSK, [][]byte{[]byte("blocked")}, Options{Timeout: 5 * time.Second})
	if err == nil || time.Since(start) > 2*time.Second {
		t.Fatalf("blackhole completion: %v", err)
	}
}

func TestAuditLegacyReliableRecoversLostWelcome(t *testing.T) {
	for _, count := range []int{1, 4} {
		t.Run(fmt.Sprint(count), func(t *testing.T) {
			i, _ := secureudp.NewIdentity(make([]byte, 32))
			socket, err := net.ListenPacket("udp", "127.0.0.1:0")
			if err != nil {
				t.Fatal(err)
			}
			addr := socket.LocalAddr().String()
			_ = socket.Close()
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			done := make(chan error, 1)
			go func() {
				if count == 1 {
					done <- Serve(ctx, addr, i, Options{Timeout: 5 * time.Second})
				} else {
					done <- ServeBatch(ctx, addr, i, count, Options{Timeout: 5 * time.Second})
				}
			}()
			proxy, stopProxy := faultProxy(t, addr, faultSpec{firstWelcome: true})
			defer stopProxy()
			messages := make([][]byte, count)
			for n := range messages {
				messages[n] = bytes.Repeat([]byte{byte(n)}, 1000)
			}
			if err := ExchangeBatch(ctx, proxy, i.Public, i.PSK, messages, Options{Timeout: 5 * time.Second}); err != nil {
				t.Fatal(err)
			}
			select {
			case err := <-done:
				if err != nil {
					t.Fatal(err)
				}
			case <-ctx.Done():
				t.Fatal("legacy server failed to complete")
			}
		})
	}
}
