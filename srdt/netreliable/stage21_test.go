package netreliable

import (
	"bytes"
	"context"
	"testing"
	"time"

	"example.com/srdt/emulator"
	"example.com/srdt/secureudp"
)

func TestStage21V1CapabilitySession(t *testing.T) {
	_, identity, config := testMulti(t)
	config.AllowLegacyHandshake = false
	server, addr, stop := startMulti(t, config, identity)
	defer stop()
	if err := Exchange(context.Background(), addr, identity.Public, identity.PSK, []byte("capability"), Options{Timeout: 5 * time.Second}); err != nil {
		t.Fatal(err)
	}
	stats := server.Stats()
	if stats.V1Sessions != 1 || stats.LegacyHandshakeSessions != 0 {
		t.Fatalf("unexpected handshake stats %+v", stats)
	}
}

func TestStage21LegacyHandshakeRequiresExplicitServerOptIn(t *testing.T) {
	psk := bytes.Repeat([]byte{0x67}, 32)
	identity, _ := secureudp.NewIdentity(psk)
	config := DefaultServerConfig()
	config.HelloRate, config.HelloBurst = 1000, 1000
	_, addr, stop := startMulti(t, config, identity)
	defer stop()
	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()
	if err := Exchange(ctx, addr, identity.Public, psk, []byte("legacy"), Options{LegacyHandshake: true, Timeout: time.Second}); err == nil {
		t.Fatal("legacy handshake unexpectedly admitted")
	}
}

func TestStage21StreamingEchoBoundedAndOrdered(t *testing.T) {
	_, identity, config := testMulti(t)
	config.AllowLegacyHandshake = false
	_, addr, stop := startMulti(t, config, identity)
	defer stop()
	input := make([]byte, 2<<20)
	for i := range input {
		input[i] = byte((i*131 + i/17) & 0xff)
	}
	var output bytes.Buffer
	stats, err := ExchangeStream(context.Background(), addr, identity.Public, identity.PSK, bytes.NewReader(input), &output,
		StreamConfig{ChunkSize: 16 << 10, MaxBuffered: 64 << 10, MaxActive: 4}, Options{Timeout: 20 * time.Second})
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(input, output.Bytes()) {
		t.Fatal("stream output mismatch")
	}
	if stats.SentBytes != uint64(len(input)) || stats.ReceivedBytes != uint64(len(input)) || stats.PeakTransactions > 4 || stats.PeakQueueBytes > 64<<10 || stats.Capabilities&secureudp.CapabilityStreaming == 0 {
		t.Fatalf("unexpected stream stats %+v", stats)
	}
}

func TestStage21StreamingEchoThroughFaultRelay(t *testing.T) {
	_, identity, config := testMulti(t)
	config.AllowLegacyHandshake = false
	_, addr, stop := startMulti(t, config, identity)
	defer stop()
	faults := emulator.Config{LossPercent: 2, DuplicatePercent: 2, DelayMinMs: 1, DelayMaxMs: 10, DropSizes: []emulator.SizeRange{{Min: 800, Max: 900}}}
	proxy, stopProxy := faultProxy(t, addr, faultSpec{up: faults, down: faults, inject: true})
	defer stopProxy()
	input := make([]byte, 512<<10)
	for i := range input {
		input[i] = byte(i*29 + 7)
	}
	var output bytes.Buffer
	stats, err := ExchangeStream(context.Background(), proxy, identity.Public, identity.PSK, bytes.NewReader(input), &output,
		StreamConfig{ChunkSize: 8 << 10, MaxBuffered: 32 << 10, MaxActive: 4}, Options{Timeout: 30 * time.Second})
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(input, output.Bytes()) || stats.ReceivedBytes != uint64(len(input)) {
		t.Fatalf("fault stream mismatch stats=%+v", stats)
	}
}

func TestStage21V1HelloRetryChangesObservedSize(t *testing.T) {
	_, identity, config := testMulti(t)
	config.AllowLegacyHandshake = false
	_, addr, stop := startMulti(t, config, identity)
	defer stop()
	proxy, stopProxy := faultProxy(t, addr, faultSpec{learnFirstUpSize: true})
	defer stopProxy()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := Exchange(ctx, proxy, identity.Public, identity.PSK, []byte("repad-hello"), Options{Timeout: 5 * time.Second}); err != nil {
		t.Fatal(err)
	}
}

func TestStage21V1WelcomeRetryChangesObservedSize(t *testing.T) {
	_, identity, config := testMulti(t)
	config.AllowLegacyHandshake = false
	_, addr, stop := startMulti(t, config, identity)
	defer stop()
	proxy, stopProxy := faultProxy(t, addr, faultSpec{learnFirstDownSize: true})
	defer stopProxy()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := Exchange(ctx, proxy, identity.Public, identity.PSK, []byte("repad-welcome"), Options{Timeout: 5 * time.Second}); err != nil {
		t.Fatal(err)
	}
}
