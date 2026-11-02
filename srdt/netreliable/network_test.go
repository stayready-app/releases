package netreliable

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"example.com/srdt/secureudp"
	"net"
	"testing"
	"time"
)

func TestRealUDPReliability(t *testing.T) {
	for _, tc := range []struct {
		name             string
		dropMin, dropMax int
	}{
		{"baseline", 0, 0}, {"selective-size-drop", 800, 1100},
	} {
		t.Run(tc.name, func(t *testing.T) {
			psk := make([]byte, 32)
			if _, err := rand.Read(psk); err != nil {
				t.Fatal(err)
			}
			identity, err := secureudp.NewIdentity(psk)
			if err != nil {
				t.Fatal(err)
			}
			probe, err := net.ListenPacket("udp", "127.0.0.1:0")
			if err != nil {
				t.Fatal(err)
			}
			address := probe.LocalAddr().String()
			probe.Close()
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			opts := Options{DropMin: tc.dropMin, DropMax: tc.dropMax, Timeout: 8 * time.Second, Fragment: 900, RetryFragment: 350}
			done := make(chan error, 1)
			go func() { done <- Serve(ctx, address, identity, opts) }()
			time.Sleep(25 * time.Millisecond)
			data := bytes.Repeat([]byte("SRDT reliable test "), 200)
			if err := Exchange(ctx, address, identity.Public, psk, data, opts); err != nil {
				t.Fatalf("exchange: %v (psk=%s)", err, hex.EncodeToString(psk))
			}
			select {
			case err := <-done:
				if err != nil {
					t.Fatal(err)
				}
			case <-time.After(3 * time.Second):
				t.Fatal("server did not finish")
			}
		})
	}
}
