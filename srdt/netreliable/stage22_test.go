package netreliable

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net"
	"testing"
	"time"

	"example.com/srdt/emulator"
)

func TestStage22DuplexReadBlockDoesNotStopNetwork(t *testing.T) {
	_, identity, config := testMulti(t)
	config.AllowLegacyHandshake = false
	_, addr, stop := startMulti(t, config, identity)
	defer stop()

	app, transport := tcpPair(t)
	defer app.Close()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	result := make(chan error, 1)
	go func() {
		_, err := ExchangeDuplex(ctx, addr, identity.Public, identity.PSK, transport,
			StreamConfig{ChunkSize: 4096, MaxBuffered: 16 << 10, MaxActive: 4}, Options{Timeout: 5 * time.Second})
		result <- err
	}()

	payload := bytes.Repeat([]byte{0x42}, 4096)
	_ = app.SetDeadline(time.Now().Add(2 * time.Second))
	if _, err := app.Write(payload); err != nil {
		t.Fatal(err)
	}
	// Do not write EOF or another chunk. The transport's application reader is
	// now blocked, while its network owner must still deliver the echo.
	got := make([]byte, len(payload))
	if _, err := io.ReadFull(app, got); err != nil {
		t.Fatalf("echo stalled behind blocked application read: %v", err)
	}
	if !bytes.Equal(got, payload) {
		t.Fatal("duplex echo mismatch")
	}
	if err := app.CloseWrite(); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-result:
		if err != nil {
			t.Fatalf("unexpected shutdown: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("duplex transport did not finish after application EOF")
	}
}

func TestStage22DuplexWriteBlockDoesNotStopNetwork(t *testing.T) {
	_, identity, config := testMulti(t)
	config.AllowLegacyHandshake = false
	server, addr, stop := startMulti(t, config, identity)
	defer stop()

	app, transport := tcpPair(t)
	defer app.Close()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	statsCh := make(chan StreamStats, 1)
	errCh := make(chan error, 1)
	go func() {
		stats, err := ExchangeDuplex(ctx, addr, identity.Public, identity.PSK, transport,
			StreamConfig{ChunkSize: 4096, MaxBuffered: 16 << 10, MaxActive: 4}, Options{Timeout: 8 * time.Second})
		statsCh <- stats
		errCh <- err
	}()

	payload := make([]byte, 16<<10)
	for i := range payload {
		payload[i] = byte(i*31 + 9)
	}
	_ = app.SetWriteDeadline(time.Now().Add(2 * time.Second))
	if _, err := app.Write(payload); err != nil {
		t.Fatal(err)
	}
	if err := app.CloseWrite(); err != nil {
		t.Fatal(err)
	}
	// Intentionally do not read the echo yet. The application writer inside the
	// transport blocks on net.Pipe, but ACK/retransmission and server receive must
	// keep progressing until the bounded receive window is full.
	deadline := time.Now().Add(3 * time.Second)
	for server.Stats().DeliveredBytes < uint64(len(payload)) && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	if got := server.Stats().DeliveredBytes; got < uint64(len(payload)) {
		t.Fatalf("network progress stopped behind blocked application write: delivered=%d", got)
	}

	_ = app.SetReadDeadline(time.Now().Add(3 * time.Second))
	got := make([]byte, len(payload))
	if _, err := io.ReadFull(app, got); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, payload) {
		t.Fatal("duplex echo mismatch")
	}
	select {
	case stats := <-statsCh:
		if stats.PeakQueueBytes > 16<<10 || stats.PeakTransactions > 4 {
			t.Fatalf("application bounds exceeded: %+v", stats)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("missing duplex stats")
	}
	select {
	case err := <-errCh:
		if err != nil {
			t.Fatalf("unexpected shutdown: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("duplex transport did not finish after application EOF")
	}
}

func TestStage22DuplexThroughFaultRelay(t *testing.T) {
	_, identity, config := testMulti(t)
	config.AllowLegacyHandshake = false
	_, addr, stop := startMulti(t, config, identity)
	defer stop()
	faults := emulator.Config{LossPercent: 2, DuplicatePercent: 2, DelayMinMs: 1, DelayMaxMs: 8, DropSizes: []emulator.SizeRange{{Min: 800, Max: 900}}}
	proxy, stopProxy := faultProxy(t, addr, faultSpec{up: faults, down: faults, inject: true})
	defer stopProxy()

	app, transport := tcpPair(t)
	defer app.Close()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	statsCh := make(chan StreamStats, 1)
	errCh := make(chan error, 1)
	go func() {
		stats, err := ExchangeDuplex(ctx, proxy, identity.Public, identity.PSK, transport,
			StreamConfig{ChunkSize: 8 << 10, MaxBuffered: 32 << 10, MaxActive: 4}, Options{Timeout: 30 * time.Second})
		statsCh <- stats
		errCh <- err
	}()

	input := make([]byte, 512<<10)
	for i := range input {
		input[i] = byte(i*17 + i/23 + 3)
	}
	writeErr := make(chan error, 1)
	go func() {
		_ = app.SetWriteDeadline(time.Now().Add(20 * time.Second))
		_, err := app.Write(input)
		if err == nil {
			err = app.CloseWrite()
		}
		writeErr <- err
	}()
	output := make([]byte, len(input))
	_ = app.SetReadDeadline(time.Now().Add(25 * time.Second))
	if _, err := io.ReadFull(app, output); err != nil {
		t.Fatal(err)
	}
	if err := <-writeErr; err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(input, output) {
		t.Fatal("faulted duplex output mismatch")
	}
	stats := <-statsCh
	if stats.SentBytes != uint64(len(input)) || stats.ReceivedBytes != uint64(len(input)) || stats.PeakQueueBytes > 32<<10 || stats.PeakTransactions > 4 {
		t.Fatalf("unexpected duplex stats %+v", stats)
	}
	if err := <-errCh; err != nil {
		t.Fatalf("unexpected duplex shutdown: %v", err)
	}
}

func tcpPair(t *testing.T) (*net.TCPConn, *net.TCPConn) {
	t.Helper()
	listener, err := net.ListenTCP("tcp", &net.TCPAddr{IP: net.ParseIP("127.0.0.1")})
	if err != nil {
		t.Fatal(err)
	}
	accepted := make(chan *net.TCPConn, 1)
	errs := make(chan error, 1)
	go func() {
		conn, err := listener.AcceptTCP()
		if err != nil {
			errs <- err
			return
		}
		accepted <- conn
	}()
	client, err := net.DialTCP("tcp", nil, listener.Addr().(*net.TCPAddr))
	if err != nil {
		listener.Close()
		t.Fatal(err)
	}
	defer listener.Close()
	select {
	case server := <-accepted:
		return client, server
	case err := <-errs:
		client.Close()
		t.Fatal(err)
	case <-time.After(2 * time.Second):
		client.Close()
		t.Fatal("TCP pair accept timeout")
	}
	return nil, nil
}

func TestStage22DuplexCancellationClosesOwnedEndpoint(t *testing.T) {
	_, identity, config := testMulti(t)
	config.AllowLegacyHandshake = false
	_, addr, stop := startMulti(t, config, identity)
	defer stop()

	app, transport := tcpPair(t)
	defer app.Close()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		_, err := ExchangeDuplex(ctx, addr, identity.Public, identity.PSK, transport,
			StreamConfig{ChunkSize: 4096, MaxBuffered: 16 << 10, MaxActive: 4}, Options{Timeout: 10 * time.Second})
		done <- err
	}()
	// Allow the authenticated session to establish while the application side is
	// otherwise completely idle/blocking.
	time.Sleep(100 * time.Millisecond)
	cancel()
	select {
	case err := <-done:
		if err != nil && !errors.Is(err, context.Canceled) {
			t.Fatalf("unexpected cancellation result: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("owned duplex endpoint did not unblock on cancellation")
	}
}
