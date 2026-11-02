package netreliable

import (
	"context"
	"fmt"
	"io"
	"net"
	"sync"
	"testing"
	"time"
)

func startConcurrentEchoUpstream(t *testing.T) (string, func()) {
	t.Helper()
	ln, err := net.ListenTCP("tcp", &net.TCPAddr{IP: net.ParseIP("127.0.0.1")})
	if err != nil {
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	done := make(chan struct{})
	go func() {
		defer close(done)
		for {
			c, err := ln.AcceptTCP()
			if err != nil {
				return
			}
			wg.Add(1)
			go func(c *net.TCPConn) {
				defer wg.Done()
				defer c.Close()
				buf := make([]byte, 32<<10)
				_, _ = io.CopyBuffer(c, c, buf)
				_ = c.CloseWrite()
			}(c)
		}
	}()
	return ln.Addr().String(), func() {
		_ = ln.Close()
		select {
		case <-done:
		case <-time.After(time.Second):
		}
		wg.Wait()
	}
}

func TestStage23PendingUpstreamReadResumesAfterInboundWrite(t *testing.T) {
	upstream, stopUpstream := startConcurrentEchoUpstream(t)
	defer stopUpstream()
	_, identity, config := testMulti(t)
	config.AllowLegacyHandshake = false
	config.AllowLegacyData = false
	config.Upstream = upstream
	config.IdleTimeout = 10 * time.Second
	config.ProgressTimeout = 10 * time.Second
	_, addr, stop := startMulti(t, config, identity)
	defer stop()

	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
	defer cancel()
	const clients = 10
	const total = 128 << 10
	errs := make(chan error, clients)
	var wg sync.WaitGroup
	for n := 0; n < clients; n++ {
		n := n
		wg.Add(1)
		go func() {
			defer wg.Done()
			app, transport := tcpPair(t)
			defer app.Close()
			result := make(chan error, 1)
			go func() {
				_, err := ExchangeIndependentDuplex(ctx, addr, identity.Public, identity.PSK, transport,
					StreamConfig{ChunkSize: 16 << 10, MaxBuffered: 64 << 10, MaxActive: 4}, Options{Timeout: 8 * time.Second})
				result <- err
			}()
			payload := make([]byte, total)
			for i := range payload {
				payload[i] = byte(i*17 + n*31)
			}
			writeDone := make(chan error, 1)
			go func() {
				_, err := app.Write(payload)
				if err == nil {
					err = app.CloseWrite()
				}
				writeDone <- err
			}()
			got, err := io.ReadAll(app)
			if err == nil && len(got) != len(payload) {
				err = fmt.Errorf("received %d/%d", len(got), len(payload))
			}
			if err == nil {
				for i := range got {
					if got[i] != payload[i] {
						err = fmt.Errorf("mismatch at %d", i)
						break
					}
				}
			}
			if werr := <-writeDone; err == nil {
				err = werr
			}
			if xerr := <-result; err == nil {
				err = xerr
			}
			if err != nil {
				errs <- fmt.Errorf("client %d: %w", n, err)
			}
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Error(err)
	}
}

func TestStage23LongSingleBridgeNoDirectionalStarvation(t *testing.T) {
	upstream, stopUpstream := startConcurrentEchoUpstream(t)
	defer stopUpstream()
	_, identity, config := testMulti(t)
	config.AllowLegacyHandshake = false
	config.AllowLegacyData = false
	config.Upstream = upstream
	config.IdleTimeout = 10 * time.Second
	config.ProgressTimeout = 10 * time.Second
	_, addr, stop := startMulti(t, config, identity)
	defer stop()

	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
	defer cancel()
	app, transport := tcpPair(t)
	defer app.Close()
	result := make(chan error, 1)
	go func() {
		_, err := ExchangeIndependentDuplex(ctx, addr, identity.Public, identity.PSK, transport,
			StreamConfig{ChunkSize: 16 << 10, MaxBuffered: 64 << 10, MaxActive: 4}, Options{Timeout: 8 * time.Second})
		result <- err
	}()

	const total = 8 << 20
	writeDone := make(chan error, 1)
	go func() {
		buf := make([]byte, 32<<10)
		offset := 0
		for offset < total {
			n := min(len(buf), total-offset)
			for i := 0; i < n; i++ {
				buf[i] = byte((offset+i)*37 + (offset+i)/19 + 11)
			}
			if _, err := app.Write(buf[:n]); err != nil {
				writeDone <- err
				return
			}
			offset += n
		}
		writeDone <- app.CloseWrite()
	}()

	buf := make([]byte, 32<<10)
	offset := 0
	for {
		n, err := app.Read(buf)
		for i := 0; i < n; i++ {
			want := byte((offset+i)*37 + (offset+i)/19 + 11)
			if buf[i] != want {
				t.Fatalf("bridge mismatch at %d", offset+i)
			}
		}
		offset += n
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
	}
	if err := <-writeDone; err != nil {
		t.Fatal(err)
	}
	if offset != total {
		t.Fatalf("received %d/%d", offset, total)
	}
	if err := <-result; err != nil {
		t.Fatal(err)
	}
}
