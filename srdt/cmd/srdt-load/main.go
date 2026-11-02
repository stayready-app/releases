// srdt-load is a laboratory load GENERATOR. Unlike the server, active mode
// intentionally uses a goroutine per simulated client. Run it separately when
// measuring server RSS/CPU; its allocations must not be attributed to SRDT.
package main

import (
	"context"
	"crypto/ecdh"
	cryptorand "crypto/rand"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"math/rand"
	"net"
	"os"
	"sync"
	"time"

	"example.com/srdt/netreliable"
	"example.com/srdt/reliable"
	"example.com/srdt/secureudp"
)

type report struct {
	Mode              string   `json:"mode"`
	Clients           int      `json:"clients"`
	Succeeded         int      `json:"succeeded"`
	MessagesPerClient int      `json:"messages_per_client"`
	MessageBytes      int      `json:"message_bytes"`
	VerifiedBytes     int64    `json:"verified_bytes_one_direction"`
	Seconds           float64  `json:"seconds"`
	GoodputMbps       float64  `json:"verified_one_direction_mbps"`
	Verification      string   `json:"verification"`
	Seed              int64    `json:"seed"`
	Errors            []string `json:"errors,omitempty"`
}

func main() {
	server := flag.String("server", "127.0.0.1:19443", "UDP multi-server")
	publicText := flag.String("server-public", "", "pinned public key")
	pskText := flag.String("psk", "", "laboratory PSK, 64 hex characters")
	legacyHandshake := flag.Bool("legacy-handshake", false, "explicit Stage 20 fixed-size HELLO compatibility")
	legacyPackets := flag.Bool("legacy-packets", false, "explicit Stage 18 range-only ACK compatibility; no automatic fallback")
	legacyPacketAcks := flag.Bool("legacy-packet-acks", false, "explicit Stage 19 one-packet ACK compatibility")
	batchPacketAcks := flag.Bool("batch-packet-acks", false, "opt in to client-side exact ACK batching; may trade latency for fewer datagrams")
	legacyCredits := flag.Bool("legacy-credits", false, "explicit Stage 16 admission compatibility")
	mode := flag.String("mode", "active", "active, idle, bridge, or bridge-idle")
	clients := flag.Int("clients", 32, "simulated clients")
	messages := flag.Int("messages", 12, "messages per active client (1..65536)")
	size := flag.Int("bytes", 4096, "bytes per active message")
	seed := flag.Int64("seed", 1, "deterministic payload seed, NEVER used for crypto")
	timeout := flag.Duration("timeout", 120*time.Second, "whole workload deadline")
	hold := flag.Duration("hold", 2*time.Second, "idle hold after READY, with sockets kept open")
	keepalive := flag.Duration("keepalive", 15*time.Second, "bridge keepalive interval")
	keepaliveTimeout := flag.Duration("keepalive-timeout", 45*time.Second, "bridge path-liveness timeout")
	dropMin := flag.Int("drop-min", 0, "drop client datagrams from this size")
	dropMax := flag.Int("drop-max", 0, "drop client datagrams through this size")
	noCongestion := flag.Bool("no-congestion", false, "LAB ONLY: disable DATA congestion window and pacing for A/B testing")
	flag.Parse()
	if *clients < 1 || *clients > 10000 || *messages < 1 || *messages > 65536 || *size < 1 || *size > 1<<20 || *timeout <= 0 || *hold < 0 || (*mode != "idle" && *mode != "active" && *mode != "bridge" && *mode != "bridge-idle") {
		fmt.Fprintln(os.Stderr, "invalid load arguments")
		os.Exit(2)
	}
	if (*mode == "active" || *mode == "bridge" || *mode == "bridge-idle") && (int64(*clients)*int64(*messages) > 1<<20 || int64(*clients)*int64(*messages)*(int64(*size)+64) > 256<<20) {
		fmt.Fprintln(os.Stderr, "load generator data/metadata estimate exceeds 256 MiB or 1M message objects; split the experiment")
		os.Exit(2)
	}
	psk, e := secureudp.ParsePSK(*pskText)
	if e != nil {
		fmt.Fprintln(os.Stderr, e)
		os.Exit(2)
	}
	public, e := secureudp.ParsePublic(*publicText)
	if e != nil {
		fmt.Fprintln(os.Stderr, e)
		os.Exit(2)
	}
	ctx, cancel := context.WithTimeout(context.Background(), *timeout)
	defer cancel()
	start := time.Now()
	r := report{Mode: *mode, Clients: *clients, MessagesPerClient: *messages, MessageBytes: *size,
		Verification: "byte-for-byte echo of every message", Seed: *seed}
	if *mode == "idle" {
		r.MessagesPerClient = 1
		r.MessageBytes = 1
		connections := make([]net.Conn, 0, *clients)
		for n := 0; n < *clients; n++ {
			conn, err := connectIdle(ctx, *server, public, psk, *legacyHandshake, *legacyCredits, *legacyPackets)
			if err != nil {
				r.Errors = append(r.Errors, fmt.Sprintf("client %d: %v", n, err))
				break
			}
			connections = append(connections, conn)
			r.Succeeded++
		}
		fmt.Printf("READY_IDLE=%d\n", len(connections))
		if len(connections) == *clients {
			select {
			case <-ctx.Done():
				r.Errors = append(r.Errors, ctx.Err().Error())
			case <-time.After(*hold):
			}
		}
		for _, conn := range connections {
			_ = conn.Close()
		}
		r.VerifiedBytes = int64(r.Succeeded)
	} else if *mode == "bridge" || *mode == "bridge-idle" {
		type result struct {
			index int
			err   error
		}
		results := make(chan result, *clients)
		var wg sync.WaitGroup
		for n := 0; n < *clients; n++ {
			wg.Add(1)
			go func(n int) {
				defer wg.Done()
				options := netreliable.Options{
					DisableCongestion: *noCongestion, LegacyPackets: *legacyPackets, LegacyPacketAcks: *legacyPacketAcks,
					BatchPacketAcks: *batchPacketAcks, Timeout: *timeout, DropMin: *dropMin, DropMax: *dropMax,
					KeepaliveInterval: *keepalive, KeepaliveTimeout: *keepaliveTimeout}
				var err error
				if *mode == "bridge-idle" {
					err = runBridgeIdleClient(ctx, *server, public, psk, *hold, options)
				} else {
					err = runBridgeClient(ctx, *server, public, psk, *messages*(*size), *seed+int64(n)*104729, options)
				}
				results <- result{n, err}
			}(n)
		}
		wg.Wait()
		close(results)
		for v := range results {
			if v.err != nil {
				r.Errors = append(r.Errors, fmt.Sprintf("client %d: %v", v.index, v.err))
			} else {
				r.Succeeded++
			}
		}
		if *mode == "bridge-idle" {
			r.MessagesPerClient = 1
			r.MessageBytes = 1
			r.Verification = "verified bridge open, idle keepalive hold, then exact one-byte echo and half-close"
			r.VerifiedBytes = int64(r.Succeeded)
		} else {
			r.Verification = "byte-for-byte independent TCP stream echo with half-close"
			r.VerifiedBytes = int64(r.Succeeded) * int64(*messages) * int64(*size)
		}
	} else {
		type result struct {
			index int
			err   error
		}
		results := make(chan result, *clients)
		var wg sync.WaitGroup
		for n := 0; n < *clients; n++ {
			wg.Add(1)
			go func(n int) {
				defer wg.Done()
				payloads := make([][]byte, *messages)
				prng := rand.New(rand.NewSource(*seed + int64(n)*104729))
				for j := range payloads {
					payloads[j] = make([]byte, *size)
					_, _ = prng.Read(payloads[j])
				}
				err := netreliable.ExchangeBatch(ctx, *server, public, psk, payloads, netreliable.Options{LegacyHandshake: *legacyHandshake, DisableCongestion: *noCongestion,
					LegacyPackets: *legacyPackets, LegacyPacketAcks: *legacyPacketAcks, BatchPacketAcks: *batchPacketAcks, LegacyCredits: *legacyCredits, Timeout: *timeout, DropMin: *dropMin, DropMax: *dropMax})
				results <- result{n, err}
			}(n)
		}
		wg.Wait()
		close(results)
		for v := range results {
			if v.err != nil {
				r.Errors = append(r.Errors, fmt.Sprintf("client %d: %v", v.index, v.err))
			} else {
				r.Succeeded++
			}
		}
		r.VerifiedBytes = int64(r.Succeeded) * int64(*messages) * int64(*size)
	}
	r.Seconds = time.Since(start).Seconds()
	r.GoodputMbps = float64(r.VerifiedBytes) * 8 / r.Seconds / 1e6
	b, _ := json.MarshalIndent(r, "", "  ")
	fmt.Println(string(b))
	if len(r.Errors) != 0 {
		os.Exit(1)
	}
}

// Complete one verified tiny exchange, then keep the connected UDP socket open.
func connectIdle(ctx context.Context, addr string, public *ecdh.PublicKey, psk []byte, legacyHandshake bool, legacy bool, legacyPackets bool) (net.Conn, error) {
	conn, e := net.DialTimeout("udp", addr, time.Second)
	if e != nil {
		return nil, e
	}
	ok := false
	defer func() {
		if !ok {
			conn.Close()
		}
	}()
	var h []byte
	var client *secureudp.Session
	if legacyHandshake {
		h, client, e = secureudp.ClientHello(public, psk)
	} else {
		offered := secureudp.SupportedCapabilities
		required := secureudp.CapabilityControlPadding
		if !legacyPackets {
			required |= secureudp.CapabilityPacketTracking
		}
		if !legacy {
			required |= secureudp.CapabilityWindowCredits
		}
		h, client, e = secureudp.ClientHelloV1(public, psk, offered, required)
	}
	if e != nil {
		return nil, e
	}
	buf := make([]byte, 1201)
	deadline := time.Now().Add(5 * time.Second)
	for {
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		if time.Now().After(deadline) {
			return nil, fmt.Errorf("hello timeout")
		}
		_ = conn.SetDeadline(time.Now().Add(150 * time.Millisecond))
		if !legacyHandshake {
			if repacked, err := secureudp.RepadHelloV1(h); err == nil {
				h = repacked
			}
		}
		if _, e = conn.Write(h); e != nil {
			return nil, e
		}
		n, e := conn.Read(buf)
		if e == nil && secureudp.VerifyWelcome(buf[:n], client) == nil {
			break
		}
		if e != nil {
			if ne, yes := e.(net.Error); !yes || !ne.Timeout() {
				return nil, e
			}
		}
	}
	f := reliable.Frame{Type: reliable.TypeData, ID: 1, Total: 1, Payload: []byte{0x5a}}
	body, _ := reliable.Encode(f)
	openType, grantType := reliable.TypeWindowOpen, reliable.TypeWindowGrant
	if legacy {
		openType, grantType = reliable.TypeOpen, reliable.TypeGrant
	}
	granted := false
	gotAck, gotEcho := false, false
	var lastSend time.Time
	for !gotAck || !gotEcho {
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		if time.Now().After(deadline) {
			return nil, fmt.Errorf("idle proof timeout")
		}
		if time.Since(lastSend) > 100*time.Millisecond {
			toSend := body
			if granted && !legacy && !legacyPackets {
				f.Type = reliable.TypePacketData
				f.PacketNumber++
				toSend, _ = reliable.Encode(f)
			}
			if !granted {
				var err error
				toSend, err = encodeIdleControl(reliable.Frame{Type: openType, ID: 1, Total: 1})
				if err != nil {
					return nil, err
				}
			}
			packet, _ := client.Seal(toSend)
			_ = conn.SetWriteDeadline(time.Now().Add(time.Second))
			_, e = conn.Write(packet)
			if e != nil {
				return nil, e
			}
			lastSend = time.Now()
		}
		_ = conn.SetReadDeadline(time.Now().Add(50 * time.Millisecond))
		n, e := conn.Read(buf)
		if e != nil {
			if ne, yes := e.(net.Error); yes && ne.Timeout() {
				continue
			}
			return nil, e
		}
		plain, e := client.Open(buf[:n])
		if e != nil {
			continue
		}
		frame, e := reliable.Decode(plain)
		if e != nil {
			continue
		}
		if frame.Type == grantType && frame.ID == 1 && frame.Total == 1 {
			granted = true
			lastSend = time.Time{}
		}
		if frame.Type == openType && frame.ID == 2 && frame.Total == 1 {
			grant, err := encodeIdleControl(reliable.Frame{Type: grantType, ID: 2, Total: 1})
			if err != nil {
				return nil, err
			}
			packet, _ := client.Seal(grant)
			if _, e = conn.Write(packet); e != nil {
				return nil, e
			}
		}
		if frame.Type == reliable.TypeReject && frame.ID == 1 {
			return nil, fmt.Errorf("idle request rejected")
		}
		if ((frame.Type == reliable.TypeAck || frame.Type == reliable.TypePacketAck) && frame.ID == 1) || frame.Type == reliable.TypePacketAckBatch {
			gotAck = true
		}
		if (frame.Type == reliable.TypeData || frame.Type == reliable.TypePacketData) && frame.ID == 2 && frame.Total == 1 && len(frame.Payload) == 1 && frame.Payload[0] == 0x5a {
			gotEcho = true
			ackType := reliable.TypeAck
			if frame.Type == reliable.TypePacketData {
				ackType = reliable.TypePacketAck
			}
			ack, _ := reliable.Encode(reliable.Frame{Type: ackType, ID: 2, Total: 1, Payload: []byte{0, 0, 0, 1}, PacketNumber: frame.PacketNumber})
			packet, _ := client.Seal(ack)
			if _, e = conn.Write(packet); e != nil {
				return nil, e
			}
		}
	}
	_ = conn.SetDeadline(time.Time{})
	ok = true
	return conn, nil
}

func encodeIdleControl(f reliable.Frame) ([]byte, error) {
	if f.Type == reliable.TypeWindowOpen || f.Type == reliable.TypeWindowGrant {
		var random [1]byte
		if _, err := cryptorand.Read(random[:]); err != nil {
			return nil, err
		}
		return reliable.EncodeEnvelopeInto(nil, f, 16+int(random[0])%240)
	}
	return reliable.Encode(f)
}

func runBridgeIdleClient(ctx context.Context, server string, public *ecdh.PublicKey, psk []byte, hold time.Duration, o netreliable.Options) error {
	app, transport, err := bridgeTCPPair()
	if err != nil {
		return err
	}
	defer app.Close()
	result := make(chan error, 1)
	go func() {
		_, err := netreliable.ExchangeIndependentDuplex(ctx, server, public, psk, transport,
			netreliable.StreamConfig{ChunkSize: netreliable.DefaultStreamChunk}, o)
		result <- err
	}()

	// One byte proves STREAM_OPEN and the upstream bridge before the idle hold.
	probe := []byte{0x5a}
	if _, err := app.Write(probe); err != nil {
		return err
	}
	got := make([]byte, 1)
	_ = app.SetReadDeadline(time.Now().Add(min(5*time.Second, o.Timeout)))
	if _, err := io.ReadFull(app, got); err != nil {
		return err
	}
	if got[0] != probe[0] {
		return fmt.Errorf("bridge-idle probe mismatch")
	}
	_ = app.SetReadDeadline(time.Time{})

	select {
	case <-ctx.Done():
		return ctx.Err()
	case err := <-result:
		if err == nil {
			return fmt.Errorf("bridge closed during idle hold")
		}
		return err
	case <-time.After(hold):
	}

	if err := app.CloseWrite(); err != nil {
		return err
	}
	if _, err := io.Copy(io.Discard, app); err != nil {
		return err
	}
	return <-result
}

func runBridgeClient(ctx context.Context, server string, public *ecdh.PublicKey, psk []byte, total int, seed int64, o netreliable.Options) error {
	app, transport, err := bridgeTCPPair()
	if err != nil {
		return err
	}
	defer app.Close()
	result := make(chan error, 1)
	go func() {
		_, err := netreliable.ExchangeIndependentDuplex(ctx, server, public, psk, transport,
			netreliable.StreamConfig{ChunkSize: netreliable.DefaultStreamChunk}, o)
		result <- err
	}()
	writeDone := make(chan error, 1)
	go func() {
		buf := make([]byte, 32<<10)
		offset := 0
		var err error
		for offset < total {
			n := min(len(buf), total-offset)
			fillBridgePattern(buf[:n], int64(offset), seed)
			_, err = app.Write(buf[:n])
			if err != nil {
				break
			}
			offset += n
		}
		if err == nil {
			err = app.CloseWrite()
		}
		writeDone <- err
	}()
	buf := make([]byte, 32<<10)
	offset := 0
	for {
		n, err := app.Read(buf)
		if n > 0 {
			if offset+n > total {
				return fmt.Errorf("bridge received too many bytes")
			}
			for i, b := range buf[:n] {
				at := int64(offset + i)
				if b != bridgePattern(at, seed) {
					return fmt.Errorf("bridge mismatch at %d", at)
				}
			}
			offset += n
		}
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}
	}
	if err := <-writeDone; err != nil {
		return err
	}
	if offset != total {
		return fmt.Errorf("bridge received %d/%d bytes", offset, total)
	}
	select {
	case err := <-result:
		return err
	case <-ctx.Done():
		return ctx.Err()
	}
}

func bridgePattern(offset, seed int64) byte { return byte((offset*131 + offset/17 + seed*29) & 0xff) }
func fillBridgePattern(b []byte, offset, seed int64) {
	for i := range b {
		b[i] = bridgePattern(offset+int64(i), seed)
	}
}

func bridgeTCPPair() (*net.TCPConn, *net.TCPConn, error) {
	ln, err := net.ListenTCP("tcp", &net.TCPAddr{IP: net.ParseIP("127.0.0.1")})
	if err != nil {
		return nil, nil, err
	}
	accepted := make(chan *net.TCPConn, 1)
	errs := make(chan error, 1)
	go func() {
		c, e := ln.AcceptTCP()
		if e != nil {
			errs <- e
		} else {
			accepted <- c
		}
	}()
	client, err := net.DialTCP("tcp", nil, ln.Addr().(*net.TCPAddr))
	if err != nil {
		_ = ln.Close()
		return nil, nil, err
	}
	select {
	case server := <-accepted:
		_ = ln.Close()
		return client, server, nil
	case err := <-errs:
		_ = ln.Close()
		_ = client.Close()
		return nil, nil, err
	case <-time.After(2 * time.Second):
		_ = ln.Close()
		_ = client.Close()
		return nil, nil, fmt.Errorf("bridge TCP pair timeout")
	}
}
