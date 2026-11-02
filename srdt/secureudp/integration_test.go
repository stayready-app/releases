package secureudp

import (
	"context"
	"crypto/rand"
	"net"
	"testing"
	"time"
)

func TestRealUDPEcho(t *testing.T) {
	probe, err := net.ListenPacket("udp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := probe.LocalAddr().String()
	probe.Close()
	psk := make([]byte, 32)
	if _, err := rand.Read(psk); err != nil {
		t.Fatal(err)
	}
	identity, err := NewIdentity(psk)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- ServeEcho(ctx, addr, identity) }()
	var last error
	for i := 0; i < 30; i++ {
		last = RunEcho(addr, identity.Public, psk, []byte("real UDP test"))
		if last == nil {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if last != nil {
		t.Fatal(last)
	}
	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("server did not stop")
	}
}
