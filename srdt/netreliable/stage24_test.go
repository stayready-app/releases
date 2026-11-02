package netreliable

import (
	"bytes"
	"context"
	"errors"
	"io"
	"strings"
	"testing"
	"time"

	"example.com/srdt/reliable"
	"example.com/srdt/secureudp"
)

func TestStage24PathControlRoundTrip(t *testing.T) {
	for _, typ := range []byte{reliable.TypePathPing, reliable.TypePathPong} {
		encoded, err := reliable.Encode(reliable.Frame{Type: typ, ID: 77})
		if err != nil {
			t.Fatal(err)
		}
		decoded, err := reliable.Decode(encoded)
		if err != nil {
			t.Fatal(err)
		}
		if decoded.Type != typ || decoded.ID != 77 {
			t.Fatalf("roundtrip mismatch: %+v", decoded)
		}
	}
	invalid := []reliable.Frame{
		{Type: reliable.TypePathPing},
		{Type: reliable.TypePathPong, ID: 1, Offset: 1},
		{Type: reliable.TypePathPing, ID: 1, Total: 1},
		{Type: reliable.TypePathPong, ID: 1, Payload: []byte{1}},
	}
	for _, frame := range invalid {
		if _, err := reliable.Encode(frame); err == nil {
			t.Fatalf("accepted invalid path frame %+v", frame)
		}
	}
}

func TestStage24SilentBridgeSurvivesServerIdleTimeout(t *testing.T) {
	upstream, stopUpstream := startConcurrentEchoUpstream(t)
	defer stopUpstream()

	_, identity, config := testMulti(t)
	config.AllowLegacyHandshake = false
	config.AllowLegacyData = false
	config.Upstream = upstream
	config.IdleTimeout = 350 * time.Millisecond
	config.ProgressTimeout = 2 * time.Second
	server, addr, stop := startMulti(t, config, identity)
	defer stop()

	app, transport := tcpPair(t)
	defer app.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Second)
	defer cancel()
	statsCh := make(chan StreamStats, 1)
	errCh := make(chan error, 1)
	go func() {
		stats, err := ExchangeIndependentDuplex(ctx, addr, identity.Public, identity.PSK, transport,
			StreamConfig{ChunkSize: 4096, MaxBuffered: 16 << 10, MaxActive: 4},
			Options{Timeout: time.Second, KeepaliveInterval: 100 * time.Millisecond, KeepaliveTimeout: 600 * time.Millisecond})
		statsCh <- stats
		errCh <- err
	}()

	// Remain completely application-idle for several server idle lifetimes.
	time.Sleep(900 * time.Millisecond)

	payload := bytes.Repeat([]byte("alive-after-idle-"), 512)
	if _, err := app.Write(payload); err != nil {
		t.Fatal(err)
	}
	if err := app.CloseWrite(); err != nil {
		t.Fatal(err)
	}
	got, err := io.ReadAll(app)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, payload) {
		t.Fatalf("idle-survival echo mismatch: %d/%d", len(got), len(payload))
	}
	if err := <-errCh; err != nil {
		t.Fatal(err)
	}
	stats := <-statsCh
	if stats.PathPingsSent == 0 || stats.PathPongsReceived == 0 || stats.Capabilities&secureudp.CapabilityPathLiveness == 0 {
		t.Fatalf("liveness was not exercised: %+v", stats)
	}
	serverStats := server.Stats()
	if serverStats.PathPingsReceived == 0 || serverStats.PathPongsSent == 0 || serverStats.ExpiredSessions != 0 {
		t.Fatalf("server liveness stats %+v", serverStats)
	}
}

func TestStage24PathLivenessDetectsDeadServer(t *testing.T) {
	upstream, stopUpstream := startConcurrentEchoUpstream(t)
	defer stopUpstream()

	_, identity, config := testMulti(t)
	config.AllowLegacyHandshake = false
	config.AllowLegacyData = false
	config.Upstream = upstream
	config.IdleTimeout = 2 * time.Second
	_, addr, stop := startMulti(t, config, identity)

	app, transport := tcpPair(t)
	defer app.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	errCh := make(chan error, 1)
	go func() {
		_, err := ExchangeIndependentDuplex(ctx, addr, identity.Public, identity.PSK, transport,
			StreamConfig{ChunkSize: 4096, MaxBuffered: 16 << 10, MaxActive: 4},
			Options{Timeout: time.Second, KeepaliveInterval: 30 * time.Millisecond, KeepaliveTimeout: 150 * time.Millisecond})
		errCh <- err
	}()

	probe := []byte("probe-before-server-stop")
	if _, err := app.Write(probe); err != nil {
		stop()
		t.Fatal(err)
	}
	got := make([]byte, len(probe))
	_ = app.SetReadDeadline(time.Now().Add(time.Second))
	if _, err := io.ReadFull(app, got); err != nil {
		stop()
		t.Fatal(err)
	}
	if !bytes.Equal(got, probe) {
		stop()
		t.Fatal("probe mismatch")
	}

	stoppedAt := time.Now()
	stop()
	select {
	case err := <-errCh:
		if err == nil || !strings.Contains(err.Error(), "path-liveness timeout") {
			t.Fatalf("unexpected dead-path result: %v", err)
		}
		if time.Since(stoppedAt) > 800*time.Millisecond {
			t.Fatalf("dead path detected too slowly: %v", time.Since(stoppedAt))
		}
	case <-time.After(time.Second):
		t.Fatal("dead path did not terminate bridge")
	}
}

func TestStage24BridgeRequiresLivenessUnlessExplicitlyLegacy(t *testing.T) {
	_, identity, config := testMulti(t)
	config.Upstream = "127.0.0.1:1"
	config.AllowLegacyLiveness = true
	server, addr, stop := startMulti(t, config, identity)
	defer stop()
	_ = server

	// A modern client offers the new capability even when it does not require it.
	// This smoke test primarily guards the capability bit and explicit compatibility
	// flag from being accidentally removed from negotiation.
	app, transport := tcpPair(t)
	defer app.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 400*time.Millisecond)
	defer cancel()
	_, err := ExchangeIndependentDuplex(ctx, addr, identity.Public, identity.PSK, transport,
		StreamConfig{ChunkSize: 4096}, Options{Timeout: 200 * time.Millisecond, LegacyLiveness: true})
	if err == nil {
		t.Fatal("unreachable upstream unexpectedly succeeded")
	}
	if errors.Is(err, secureudp.ErrIncompatible) {
		t.Fatalf("explicit legacy-liveness negotiation was rejected: %v", err)
	}
}

func TestStage24AsymmetricFailureResetReleasesServer(t *testing.T) {
	upstream, stopUpstream := startConcurrentEchoUpstream(t)
	defer stopUpstream()

	_, identity, config := testMulti(t)
	config.AllowLegacyHandshake = false
	config.AllowLegacyData = false
	config.Upstream = upstream
	config.IdleTimeout = 2 * time.Second
	config.ProgressTimeout = 2 * time.Second
	server, addr, stop := startMulti(t, config, identity)
	defer stop()

	proxy, stopProxy := faultProxy(t, addr, faultSpec{downBlackoutAfter: 300 * time.Millisecond})
	defer stopProxy()

	app, transport := tcpPair(t)
	defer app.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	errCh := make(chan error, 1)
	go func() {
		_, err := ExchangeIndependentDuplex(ctx, proxy, identity.Public, identity.PSK, transport,
			StreamConfig{ChunkSize: 4096, MaxBuffered: 16 << 10, MaxActive: 4},
			Options{Timeout: time.Second, KeepaliveInterval: 30 * time.Millisecond, KeepaliveTimeout: 140 * time.Millisecond})
		errCh <- err
	}()

	probe := []byte("asymmetric-path-probe")
	if _, err := app.Write(probe); err != nil {
		t.Fatal(err)
	}
	got := make([]byte, len(probe))
	_ = app.SetReadDeadline(time.Now().Add(time.Second))
	if _, err := io.ReadFull(app, got); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, probe) {
		t.Fatal("asymmetric probe mismatch")
	}
	_ = app.SetReadDeadline(time.Time{})

	select {
	case err := <-errCh:
		if err == nil || !strings.Contains(err.Error(), "path-liveness timeout") {
			t.Fatalf("unexpected asymmetric failure result: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("asymmetric dead path was not detected")
	}

	deadline := time.Now().Add(500 * time.Millisecond)
	for time.Now().Before(deadline) {
		st := server.Stats()
		if st.ActiveSessions == 0 {
			if st.BridgeResets == 0 {
				t.Fatalf("session released without reset accounting: %+v", st)
			}
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("server retained asymmetric-dead bridge until idle timeout: %+v", server.Stats())
}

func TestStage24CompletedBridgeDrainsBeforeIdleTimeout(t *testing.T) {
	upstream, stopUpstream := startConcurrentEchoUpstream(t)
	defer stopUpstream()

	_, identity, config := testMulti(t)
	config.AllowLegacyHandshake = false
	config.AllowLegacyData = false
	config.Upstream = upstream
	config.IdleTimeout = 10 * time.Second
	config.BridgeDrainTimeout = 120 * time.Millisecond
	server, addr, stop := startMulti(t, config, identity)
	defer stop()

	app, transport := tcpPair(t)
	defer app.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	errCh := make(chan error, 1)
	go func() {
		_, err := ExchangeIndependentDuplex(ctx, addr, identity.Public, identity.PSK, transport,
			StreamConfig{ChunkSize: 4096, MaxBuffered: 16 << 10, MaxActive: 4}, Options{Timeout: time.Second})
		errCh <- err
	}()

	payload := bytes.Repeat([]byte("drain-"), 256)
	if _, err := app.Write(payload); err != nil {
		t.Fatal(err)
	}
	if err := app.CloseWrite(); err != nil {
		t.Fatal(err)
	}
	got, err := io.ReadAll(app)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, payload) {
		t.Fatal("drain echo mismatch")
	}
	if err := <-errCh; err != nil {
		t.Fatal(err)
	}

	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		st := server.Stats()
		if st.ActiveSessions == 0 {
			if st.CompletedBridgeSessions == 0 || st.ExpiredSessions != 0 {
				t.Fatalf("unexpected completion accounting: %+v", st)
			}
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("completed bridge retained until idle timeout: %+v", server.Stats())
}

func TestStage24ExplicitLegacyLivenessNegotiation(t *testing.T) {
	psk := bytes.Repeat([]byte{0x41}, 32)
	bridgeRequired := secureudp.CapabilityControlPadding |
		secureudp.CapabilityStreaming |
		secureudp.CapabilityIndependentStreams |
		secureudp.CapabilityWindowCredits
	oldSupported := secureudp.SupportedCapabilities &^ secureudp.CapabilityPathLiveness

	// Stage 23 client -> Stage 24 server with AllowLegacyLiveness semantics.
	server, err := secureudp.NewIdentity(psk)
	if err != nil {
		t.Fatal(err)
	}
	hello, clientSession, err := secureudp.ClientHelloV1(server.Public, psk, oldSupported, bridgeRequired)
	if err != nil {
		t.Fatal(err)
	}
	welcome, serverSession, err := secureudp.AcceptHelloV1(server, hello, secureudp.SupportedCapabilities, bridgeRequired)
	if err != nil {
		t.Fatal(err)
	}
	if err := secureudp.VerifyWelcome(welcome, clientSession); err != nil {
		t.Fatal(err)
	}
	if clientSession.HasCapability(secureudp.CapabilityPathLiveness) || serverSession.HasCapability(secureudp.CapabilityPathLiveness) {
		t.Fatal("legacy peer unexpectedly negotiated path liveness")
	}

	// Stage 24 client with explicit LegacyLiveness -> Stage 23 capability set.
	server2, err := secureudp.NewIdentity(psk)
	if err != nil {
		t.Fatal(err)
	}
	o := Options{LegacyLiveness: true}
	hello2, client2, err := secureudp.ClientHelloV1(server2.Public, psk, offeredCapabilities(o),
		requiredCapabilities(o, secureudp.CapabilityStreaming|secureudp.CapabilityIndependentStreams))
	if err != nil {
		t.Fatal(err)
	}
	welcome2, serverSession2, err := secureudp.AcceptHelloV1(server2, hello2, oldSupported, secureudp.CapabilityControlPadding)
	if err != nil {
		t.Fatal(err)
	}
	if err := secureudp.VerifyWelcome(welcome2, client2); err != nil {
		t.Fatal(err)
	}
	if client2.HasCapability(secureudp.CapabilityPathLiveness) || serverSession2.HasCapability(secureudp.CapabilityPathLiveness) {
		t.Fatal("old server unexpectedly selected path liveness")
	}
}

func TestStage24PathControlWireSizesRotate(t *testing.T) {
	psk := bytes.Repeat([]byte{0x73}, 32)
	identity, err := secureudp.NewIdentity(psk)
	if err != nil {
		t.Fatal(err)
	}
	hello, client, err := secureudp.ClientHelloV1(identity.Public, psk, secureudp.SupportedCapabilities,
		secureudp.CapabilityControlPadding|secureudp.CapabilityPathLiveness)
	if err != nil {
		t.Fatal(err)
	}
	welcome, server, err := secureudp.AcceptHelloV1(identity, hello, secureudp.SupportedCapabilities,
		secureudp.CapabilityControlPadding|secureudp.CapabilityPathLiveness)
	if err != nil {
		t.Fatal(err)
	}
	if err := secureudp.VerifyWelcome(welcome, client); err != nil {
		t.Fatal(err)
	}

	for _, tc := range []struct {
		name    string
		session *secureudp.Session
		typeID  byte
	}{
		{"ping", client, reliable.TypePathPing},
		{"pong", server, reliable.TypePathPong},
	} {
		t.Run(tc.name, func(t *testing.T) {
			lengths := make(map[int]struct{})
			for n := uint64(1); n <= 8; n++ {
				encoded, err := encodeFrame(nil, reliable.Frame{Type: tc.typeID, ID: n}, true, tc.session.NextControlPadding())
				if err != nil {
					t.Fatal(err)
				}
				packet, err := tc.session.Seal(encoded)
				if err != nil {
					t.Fatal(err)
				}
				lengths[len(packet)] = struct{}{}
			}
			if len(lengths) < 4 {
				t.Fatalf("path control retained too few wire sizes: %v", lengths)
			}
		})
	}
}
