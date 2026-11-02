package netreliable

import (
	"bytes"
	"context"
	"io"
	"net"
	"testing"
	"time"

	"example.com/srdt/emulator"
	"example.com/srdt/reliable"
)

func startIndependentUpstream(t *testing.T, banner, tail []byte) (string, <-chan []byte, func()) {
	t.Helper()
	ln, err := net.ListenTCP("tcp", &net.TCPAddr{IP: net.ParseIP("127.0.0.1")})
	if err != nil {
		t.Fatal(err)
	}
	received := make(chan []byte, 1)
	done := make(chan struct{})
	go func() {
		defer close(done)
		conn, err := ln.AcceptTCP()
		if err != nil {
			return
		}
		defer conn.Close()
		if len(banner) != 0 {
			_, _ = conn.Write(banner)
		}
		data, _ := io.ReadAll(conn)
		received <- data
		if len(tail) != 0 {
			_, _ = conn.Write(tail)
		}
		_ = conn.CloseWrite()
	}()
	stop := func() {
		_ = ln.Close()
		select {
		case <-done:
		case <-time.After(time.Second):
		}
	}
	return ln.Addr().String(), received, stop
}

func TestStage23IndependentServerPushAndHalfClose(t *testing.T) {
	banner := []byte("server-banner-before-request\n")
	tail := []byte("server-tail-after-client-fin\n")
	upstream, upstreamReceived, stopUpstream := startIndependentUpstream(t, banner, tail)
	defer stopUpstream()

	_, identity, config := testMulti(t)
	config.AllowLegacyHandshake = false
	config.AllowLegacyData = false
	config.Upstream = upstream
	config.IdleTimeout = 10 * time.Second
	config.ProgressTimeout = 10 * time.Second
	server, addr, stop := startMulti(t, config, identity)
	defer stop()

	app, transport := tcpPair(t)
	defer app.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	result := make(chan error, 1)
	statsCh := make(chan StreamStats, 1)
	go func() {
		stats, err := ExchangeIndependentDuplex(ctx, addr, identity.Public, identity.PSK, transport,
			StreamConfig{ChunkSize: 4096, MaxBuffered: 16 << 10, MaxActive: 4}, Options{Timeout: 10 * time.Second})
		statsCh <- stats
		result <- err
	}()

	_ = app.SetReadDeadline(time.Now().Add(3 * time.Second))
	gotBanner := make([]byte, len(banner))
	if _, err := io.ReadFull(app, gotBanner); err != nil {
		t.Fatalf("server push stalled: %v", err)
	}
	if !bytes.Equal(gotBanner, banner) {
		t.Fatal("server push mismatch")
	}

	payload := make([]byte, 192<<10)
	for i := range payload {
		payload[i] = byte(i*29 + i/31 + 7)
	}
	_ = app.SetWriteDeadline(time.Now().Add(5 * time.Second))
	if _, err := app.Write(payload); err != nil {
		t.Fatal(err)
	}
	if err := app.CloseWrite(); err != nil {
		t.Fatal(err)
	}

	gotTail, err := io.ReadAll(app)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(gotTail, tail) {
		t.Fatalf("tail mismatch: got=%q", gotTail)
	}

	select {
	case got := <-upstreamReceived:
		if !bytes.Equal(got, payload) {
			t.Fatalf("upstream payload mismatch: %d/%d", len(got), len(payload))
		}
	case <-time.After(3 * time.Second):
		t.Fatal("upstream did not observe client half-close")
	}
	select {
	case err := <-result:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("independent bridge did not finish")
	}
	stats := <-statsCh
	if stats.SentBytes != uint64(len(payload)) || stats.ReceivedBytes != uint64(len(banner)+len(tail)) {
		t.Fatalf("unexpected stats: %+v", stats)
	}
	if server.Stats().ResourceDrops != 0 {
		t.Fatalf("unexpected resource drops: %+v", server.Stats())
	}
}

func TestStage23StreamControlValidation(t *testing.T) {
	valid := []reliable.Frame{
		{Type: reliable.TypeStreamOpen, Total: 4096, Offset: 4},
		{Type: reliable.TypeStreamOpenAck, Total: 4096, Offset: 4},
		{Type: reliable.TypeStreamFin, ID: 9, Offset: streamDirectionClientToServer},
		{Type: reliable.TypeStreamFin, ID: 10, Offset: streamDirectionServerToClient},
		{Type: reliable.TypeStreamFinAck, ID: 0, Offset: streamDirectionServerToClient},
		{Type: reliable.TypeStreamReset, Offset: 1},
	}
	for _, f := range valid {
		b, err := reliable.Encode(f)
		if err != nil {
			t.Fatalf("encode %+v: %v", f, err)
		}
		got, err := reliable.Decode(b)
		if err != nil {
			t.Fatalf("decode %+v: %v", f, err)
		}
		if got.Type != f.Type || got.ID != f.ID || got.Offset != f.Offset || got.Total != f.Total {
			t.Fatalf("roundtrip %+v -> %+v", f, got)
		}
	}
	invalid := []reliable.Frame{
		{Type: reliable.TypeStreamOpen, ID: 1, Total: 4096, Offset: 4},
		{Type: reliable.TypeStreamFin, ID: 2, Offset: streamDirectionClientToServer},
		{Type: reliable.TypeStreamFin, ID: 3, Offset: streamDirectionServerToClient},
		{Type: reliable.TypeStreamReset, Offset: 0},
	}
	for _, f := range invalid {
		if _, err := reliable.Encode(f); err == nil {
			t.Fatalf("accepted invalid control %+v", f)
		}
	}
}

func TestStage23IndependentThroughFaultRelay(t *testing.T) {
	banner := bytes.Repeat([]byte("B"), 5000)
	tail := bytes.Repeat([]byte("T"), 7000)
	upstream, upstreamReceived, stopUpstream := startIndependentUpstream(t, banner, tail)
	defer stopUpstream()

	_, identity, config := testMulti(t)
	config.AllowLegacyHandshake = false
	config.AllowLegacyData = false
	config.Upstream = upstream
	config.IdleTimeout = 20 * time.Second
	config.ProgressTimeout = 20 * time.Second
	_, addr, stop := startMulti(t, config, identity)
	defer stop()

	faults := emulator.Config{LossPercent: 2, DuplicatePercent: 2, DelayMinMs: 1, DelayMaxMs: 8,
		DropSizes: []emulator.SizeRange{{Min: 800, Max: 900}}}
	proxy, stopProxy := faultProxy(t, addr, faultSpec{up: faults, down: faults, inject: true})
	defer stopProxy()

	app, transport := tcpPair(t)
	defer app.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	done := make(chan error, 1)
	go func() {
		_, err := ExchangeIndependentDuplex(ctx, proxy, identity.Public, identity.PSK, transport,
			StreamConfig{ChunkSize: 8 << 10, MaxBuffered: 32 << 10, MaxActive: 4}, Options{Timeout: 30 * time.Second})
		done <- err
	}()

	_ = app.SetReadDeadline(time.Now().Add(10 * time.Second))
	gotBanner := make([]byte, len(banner))
	if _, err := io.ReadFull(app, gotBanner); err != nil {
		t.Fatalf("banner through faults: %v", err)
	}
	if !bytes.Equal(gotBanner, banner) {
		t.Fatal("fault banner mismatch")
	}

	payload := make([]byte, 512<<10)
	for i := range payload {
		payload[i] = byte(i*17 + i/23 + 3)
	}
	writeDone := make(chan error, 1)
	go func() {
		_ = app.SetWriteDeadline(time.Now().Add(20 * time.Second))
		_, err := app.Write(payload)
		if err == nil {
			err = app.CloseWrite()
		}
		writeDone <- err
	}()
	rest, err := io.ReadAll(app)
	if err != nil {
		t.Fatal(err)
	}
	if err := <-writeDone; err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(rest, tail) {
		t.Fatalf("fault tail mismatch %d/%d", len(rest), len(tail))
	}
	select {
	case got := <-upstreamReceived:
		if !bytes.Equal(got, payload) {
			t.Fatalf("fault upstream mismatch %d/%d", len(got), len(payload))
		}
	case <-time.After(5 * time.Second):
		t.Fatal("fault upstream completion timeout")
	}
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("fault bridge did not finish")
	}
}

func TestStage23UpstreamDialFailureFailsExplicitly(t *testing.T) {
	dead, err := net.ListenTCP("tcp", &net.TCPAddr{IP: net.ParseIP("127.0.0.1")})
	if err != nil {
		t.Fatal(err)
	}
	upstream := dead.Addr().String()
	_ = dead.Close()

	_, identity, config := testMulti(t)
	config.AllowLegacyHandshake = false
	config.AllowLegacyData = false
	config.Upstream = upstream
	config.UpstreamDialTimeout = 200 * time.Millisecond
	_, addr, stop := startMulti(t, config, identity)
	defer stop()

	app, transport := tcpPair(t)
	defer app.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	_, err = ExchangeIndependentDuplex(ctx, addr, identity.Public, identity.PSK, transport,
		StreamConfig{ChunkSize: 4096, MaxBuffered: 16 << 10, MaxActive: 4}, Options{Timeout: 3 * time.Second})
	if err == nil {
		t.Fatal("dial failure unexpectedly succeeded")
	}
}

func TestStage23RejectsFinBeforeObservedData(t *testing.T) {
	for _, tc := range []struct {
		highest uint64
		fin     uint64
		ok      bool
	}{
		{0, 0, true},
		{0, 2, true},
		{2, 2, true},
		{2, 4, true},
		{4, 2, false},
		{2, 0, false},
	} {
		if got := streamFinConsistent(tc.highest, tc.fin); got != tc.ok {
			t.Fatalf("highest=%d fin=%d: got %v want %v", tc.highest, tc.fin, got, tc.ok)
		}
	}
}
