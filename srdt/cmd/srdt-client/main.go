package main

import (
	"bytes"
	"context"
	"errors"
	"example.com/srdt/netreliable"
	"example.com/srdt/secureudp"
	"flag"
	"fmt"
	"io"
	"log"
	"net"
	"time"
)

func main() {
	addr := flag.String("server", "127.0.0.1:19443", "UDP server")
	pskText := flag.String("psk", "", "32-byte per-client PSK in hex")
	publicText := flag.String("server-public", "", "pinned X25519 server public key in hex")
	message := flag.String("message", "SRDT test", "echo payload")
	legacyHandshake := flag.Bool("legacy-handshake", false, "explicit Stage 20 fixed-size HELLO compatibility; no automatic fallback")
	legacyPackets := flag.Bool("legacy-packets", false, "explicit Stage 18 range-only ACK compatibility; no automatic fallback")
	legacyPacketAcks := flag.Bool("legacy-packet-acks", false, "explicit Stage 19 one-packet ACK compatibility")
	legacyLiveness := flag.Bool("legacy-liveness", false, "explicit Stage 23 bridge compatibility without authenticated path keepalive")
	batchPacketAcks := flag.Bool("batch-packet-acks", false, "opt in to client-side exact ACK batching; may trade latency for fewer datagrams")
	legacyCredits := flag.Bool("legacy-credits", false, "explicit Stage 16 per-message admission; no automatic downgrade")
	reliableMode := flag.Bool("reliable", false, "run experimental reliable UDP echo")
	streamMode := flag.Bool("stream", false, "stream deterministic bytes through the bounded SRDT echo adapter")
	duplexListen := flag.String("duplex-listen", "", "listen for one local TCP connection and bridge it through the bounded SRDT echo transport")
	bridgeListen := flag.String("bridge-listen", "", "listen for one local TCP connection and carry independent bidirectional streams to --upstream on the SRDT server")
	streamChunk := flag.Int("stream-chunk", netreliable.DefaultStreamChunk, "logical chunk size for --stream/--duplex-listen")
	count := flag.Int("bytes", 4096, "test payload size in bytes")
	batch := flag.Int("batch", 0, "send N independent messages in one authenticated UDP session")
	timeout := flag.Duration("timeout", 30*time.Second, "test timeout")
	keepalive := flag.Duration("keepalive", 15*time.Second, "bridge keepalive interval after handshake/open")
	keepaliveTimeout := flag.Duration("keepalive-timeout", 45*time.Second, "bridge path-liveness timeout")
	dropMin := flag.Int("drop-min", 0, "drop encrypted datagrams from this size")
	dropMax := flag.Int("drop-max", 0, "drop encrypted datagrams through this size")
	noCongestion := flag.Bool("no-congestion", false, "LAB ONLY: disable DATA congestion window and pacing for A/B testing")
	flag.Parse()
	if *timeout <= 0 {
		log.Fatal("timeout must be positive")
	}
	psk, err := secureudp.ParsePSK(*pskText)
	if err != nil {
		log.Fatal(err)
	}
	public, err := secureudp.ParsePublic(*publicText)
	if err != nil {
		log.Fatal(err)
	}
	if *bridgeListen != "" {
		if *duplexListen != "" || *streamMode || *batch > 0 || *reliableMode {
			log.Fatal("--bridge-listen is exclusive with --duplex-listen/--stream/--batch/--reliable")
		}
		listenAddr, err := net.ResolveTCPAddr("tcp", *bridgeListen)
		if err != nil {
			log.Fatal(err)
		}
		listener, err := net.ListenTCP("tcp", listenAddr)
		if err != nil {
			log.Fatal(err)
		}
		defer listener.Close()
		fmt.Printf("BRIDGE_LISTEN=%s\n", listener.Addr())
		conn, err := listener.AcceptTCP()
		if err != nil {
			log.Fatal(err)
		}
		stats, err := netreliable.ExchangeIndependentDuplex(context.Background(), *addr, public, psk, conn,
			netreliable.StreamConfig{ChunkSize: *streamChunk},
			netreliable.Options{DisableCongestion: *noCongestion, LegacyPackets: *legacyPackets, LegacyPacketAcks: *legacyPacketAcks, LegacyLiveness: *legacyLiveness, BatchPacketAcks: *batchPacketAcks, DropMin: *dropMin, DropMax: *dropMax, KeepaliveInterval: *keepalive, KeepaliveTimeout: *keepaliveTimeout, Timeout: *timeout})
		if err != nil && !errors.Is(err, context.Canceled) {
			log.Fatal(err)
		}
		fmt.Printf("PASS: independent TCP bridge sent=%d received=%d chunks_out=%d chunks_in=%d peak_transactions=%d peak_queue=%d\n", stats.SentBytes, stats.ReceivedBytes, stats.SentChunks, stats.ReceivedChunks, stats.PeakTransactions, stats.PeakQueueBytes)
		return
	}
	if *duplexListen != "" {
		if *streamMode || *batch > 0 || *reliableMode {
			log.Fatal("--duplex-listen is exclusive with --stream/--batch/--reliable")
		}
		listener, err := net.Listen("tcp", *duplexListen)
		if err != nil {
			log.Fatal(err)
		}
		defer listener.Close()
		fmt.Printf("DUPLEX_LISTEN=%s\n", listener.Addr())
		conn, err := listener.Accept()
		if err != nil {
			log.Fatal(err)
		}
		ctx, cancel := context.WithTimeout(context.Background(), *timeout)
		defer cancel()
		stats, err := netreliable.ExchangeDuplex(ctx, *addr, public, psk, conn,
			netreliable.StreamConfig{ChunkSize: *streamChunk},
			netreliable.Options{LegacyHandshake: *legacyHandshake, DisableCongestion: *noCongestion, LegacyPackets: *legacyPackets, LegacyPacketAcks: *legacyPacketAcks, BatchPacketAcks: *batchPacketAcks, LegacyCredits: *legacyCredits, DropMin: *dropMin, DropMax: *dropMax, Timeout: *timeout})
		if err != nil && !errors.Is(err, context.Canceled) {
			log.Fatal(err)
		}
		fmt.Printf("PASS: duplex TCP bridge sent=%d received=%d chunks_out=%d chunks_in=%d peak_transactions=%d peak_queue=%d\n", stats.SentBytes, stats.ReceivedBytes, stats.SentChunks, stats.ReceivedChunks, stats.PeakTransactions, stats.PeakQueueBytes)
		return
	}
	if *streamMode {
		if *batch > 0 || *reliableMode || *count <= 0 || *count > 1<<30 {
			log.Fatal("invalid --stream combination or byte count")
		}
		ctx, cancel := context.WithTimeout(context.Background(), *timeout)
		defer cancel()
		source := &patternReader{remaining: int64(*count)}
		verify := &patternVerifier{}
		stats, err := netreliable.ExchangeStream(ctx, *addr, public, psk, source, verify,
			netreliable.StreamConfig{ChunkSize: *streamChunk},
			netreliable.Options{LegacyHandshake: *legacyHandshake, DisableCongestion: *noCongestion, LegacyPackets: *legacyPackets, LegacyPacketAcks: *legacyPacketAcks, BatchPacketAcks: *batchPacketAcks, LegacyCredits: *legacyCredits, DropMin: *dropMin, DropMax: *dropMax, Timeout: *timeout})
		if err != nil {
			log.Fatal(err)
		}
		if verify.offset != int64(*count) {
			log.Fatalf("stream verifier received %d/%d bytes", verify.offset, *count)
		}
		fmt.Printf("PASS: streamed %d bytes in %d chunks, peak_transactions=%d peak_queue=%d\n", stats.ReceivedBytes, stats.ReceivedChunks, stats.PeakTransactions, stats.PeakQueueBytes)
		return
	}
	if *batch > 0 {
		if *batch > 64 || *count <= 0 || *count > 1<<20 {
			log.Fatal("invalid batch or bytes")
		}
		messages := make([][]byte, *batch)
		for i := range messages {
			messages[i] = bytes.Repeat([]byte{byte(i), 0x5a, 0xff}, (*count+2)/3)[:*count]
		}
		ctx, cancel := context.WithTimeout(context.Background(), *timeout)
		defer cancel()
		err = netreliable.ExchangeBatch(ctx, *addr, public, psk, messages, netreliable.Options{LegacyHandshake: *legacyHandshake, DisableCongestion: *noCongestion, LegacyPackets: *legacyPackets, LegacyPacketAcks: *legacyPacketAcks, BatchPacketAcks: *batchPacketAcks, LegacyCredits: *legacyCredits, DropMin: *dropMin, DropMax: *dropMax, Timeout: *timeout})
		if err != nil {
			log.Fatal(err)
		}
		fmt.Printf("PASS: %d independent messages, %d bytes each, verified echo\n", *batch, *count)
		return
	}
	if *reliableMode {
		if *count <= 0 || *count > netreliable.MaxFramePayload*1000 {
			log.Fatal("invalid bytes count")
		}
		err = netreliable.Exchange(context.Background(), *addr, public, psk, bytes.Repeat([]byte{0x5a}, *count), netreliable.Options{LegacyHandshake: *legacyHandshake, DisableCongestion: *noCongestion, LegacyPackets: *legacyPackets, LegacyPacketAcks: *legacyPacketAcks, BatchPacketAcks: *batchPacketAcks, LegacyCredits: *legacyCredits, DropMin: *dropMin, DropMax: *dropMax, Timeout: *timeout})
		if err != nil {
			log.Fatal(err)
		}
		fmt.Println("PASS: reliable encrypted UDP round trip, exact payload match")
		return
	}
	if err := secureudp.RunEchoContext(context.Background(), *addr, public, psk, []byte(*message), *timeout); err != nil {
		log.Fatal(err)
	}
	fmt.Println("PASS: authenticated UDP round trip, exact payload match")
}

type patternReader struct {
	offset    int64
	remaining int64
}

func (r *patternReader) Read(p []byte) (int, error) {
	if r.remaining == 0 {
		return 0, io.EOF
	}
	if int64(len(p)) > r.remaining {
		p = p[:r.remaining]
	}
	for i := range p {
		at := r.offset + int64(i)
		p[i] = byte((at*131 + at/17) & 0xff)
	}
	r.offset += int64(len(p))
	r.remaining -= int64(len(p))
	return len(p), nil
}

type patternVerifier struct {
	offset int64
}

func (w *patternVerifier) Write(p []byte) (int, error) {
	for i, b := range p {
		at := w.offset + int64(i)
		if b != byte((at*131+at/17)&0xff) {
			return i, fmt.Errorf("stream mismatch at offset %d", at)
		}
	}
	w.offset += int64(len(p))
	return len(p), nil
}
