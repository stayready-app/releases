package netreliable

import (
	"bytes"
	"context"
	"crypto/rand"
	"example.com/srdt/secureudp"
	"net"
	"testing"
	"time"
)

func TestBatchRealUDP(t *testing.T) {
	for _, tc := range []struct {
		name     string
		min, max int
	}{{"baseline", 0, 0}, {"size-filter", 800, 1100}} {
		t.Run(tc.name, func(t *testing.T) {
			psk := make([]byte, 32)
			if _, err := rand.Read(psk); err != nil {
				t.Fatal(err)
			}
			id, err := secureudp.NewIdentity(psk)
			if err != nil {
				t.Fatal(err)
			}
			probe, err := net.ListenPacket("udp", "127.0.0.1:0")
			if err != nil {
				t.Fatal(err)
			}
			address := probe.LocalAddr().String()
			_ = probe.Close()
			ctx, cancel := context.WithTimeout(context.Background(), 12*time.Second)
			defer cancel()
			opts := Options{DropMin: tc.min, DropMax: tc.max, Timeout: 10 * time.Second, Fragment: 900, RetryFragment: 350}
			done := make(chan error, 1)
			go func() { done <- ServeBatch(ctx, address, id, 12, opts) }()
			time.Sleep(25 * time.Millisecond)
			messages := make([][]byte, 12)
			for i := range messages {
				messages[i] = bytes.Repeat([]byte{byte(i), 0x5a, 0xff}, 400+i*70)
			}
			if err := ExchangeBatch(ctx, address, id.Public, psk, messages, opts); err != nil {
				t.Fatal(err)
			}
			select {
			case err := <-done:
				if err != nil {
					t.Fatal(err)
				}
			case <-time.After(time.Second):
				t.Fatal("server did not finish")
			}
		})
	}
}
