package main

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"net"
	"os"
	"os/signal"
	"syscall"
	"time"

	"example.com/srdt/netreliable"
	"example.com/srdt/secureudp"
)

func main() {
	addr := flag.String("listen", "127.0.0.1:19443", "UDP listen address")
	pskText := flag.String("psk", "", "32-byte laboratory PSK in hex; do not use production credentials")
	batch := flag.Int("batch", 0, "echo N messages within one authenticated UDP session")
	timeout := flag.Duration("timeout", 30*time.Second, "legacy session timeout")
	reliableMode := flag.Bool("reliable", false, "run experimental reliable UDP echo (single client)")
	dropMin := flag.Int("drop-min", 0, "drop encrypted datagrams from this size")
	dropMax := flag.Int("drop-max", 0, "drop encrypted datagrams through this size")
	multi := flag.Bool("multi", false, "run bounded persistent multi-client UDP echo service")
	metricsFile := flag.String("metrics-file", "", "write multi-server JSON counters on shutdown")
	c := netreliable.DefaultServerConfig()
	flag.StringVar(&c.Upstream, "upstream", "", "fixed TCP upstream for V1 independent-stream bridge mode (for example 127.0.0.1:18443)")
	flag.IntVar(&c.MaxUpstreamDials, "max-upstream-dials", c.MaxUpstreamDials, "maximum concurrent TCP upstream dial operations")
	flag.DurationVar(&c.UpstreamDialTimeout, "upstream-dial-timeout", c.UpstreamDialTimeout, "TCP upstream dial timeout")
	flag.DurationVar(&c.BridgeDrainTimeout, "bridge-drain-timeout", c.BridgeDrainTimeout, "retain a normally closed SRDT bridge briefly for duplicate FIN/ACK recovery")
	flag.IntVar(&c.UpstreamSocketBuffer, "upstream-socket-buffer", c.UpstreamSocketBuffer, "requested TCP read/write socket buffer per upstream connection")
	flag.IntVar(&c.UDPReadBuffer, "udp-read-buffer", c.UDPReadBuffer, "requested shared UDP receive socket buffer")
	flag.IntVar(&c.UDPWriteBuffer, "udp-write-buffer", c.UDPWriteBuffer, "requested shared UDP send socket buffer")
	flag.BoolVar(&c.AllowLegacyHandshake, "allow-legacy-handshake", false, "accept explicit Stage 20 fixed-size HELLO clients; no automatic downgrade")
	flag.BoolVar(&c.AllowLegacyLiveness, "allow-legacy-liveness", false, "allow Stage 23 bridge clients without authenticated path keepalive")
	flag.BoolVar(&c.AllowLegacyData, "allow-legacy-data", false, "multi-server only: accept Stage 15 clients without receive credits (laboratory compatibility)")
	flag.IntVar(&c.MaxSessions, "max-sessions", c.MaxSessions, "maximum concurrent sessions")
	flag.IntVar(&c.MaxSessionsPerIP, "max-per-ip", c.MaxSessionsPerIP, "maximum concurrent sessions per source IP; consider CGNAT")
	flag.IntVar(&c.MaxAdmissions, "max-admissions", c.MaxAdmissions, "lifetime hello-ID registry cap; refuses NEW sessions when full")
	flag.Int64Var(&c.PacketStateBudget, "packet-state-budget", c.PacketStateBudget, "separate physical DATA metadata cap; must cover max-sessions * 8192 bytes")
	flag.Int64Var(&c.BufferBudget, "buffer-budget", c.BufferBudget, "shared payload, bitmap and reserved range-metadata budget in bytes; NOT RSS")
	flag.IntVar(&c.SessionBufferBytes, "session-buffer", c.SessionBufferBytes, "combined RX+TX payload limit per session in bytes")
	flag.IntVar(&c.MaxMessageBytes, "max-message", c.MaxMessageBytes, "largest message accepted by multi-server in bytes")
	flag.IntVar(&c.MaxMessages, "max-messages", c.MaxMessages, "combined RX+TX active messages per session")
	flag.IntVar(&c.WindowBytes, "window", c.WindowBytes, "unacknowledged outgoing payload bytes per session")
	flag.DurationVar(&c.IdleTimeout, "idle-timeout", c.IdleTimeout, "idle authenticated session lifetime")
	flag.DurationVar(&c.HandshakeTimeout, "handshake-timeout", c.HandshakeTimeout, "deadline for authenticated client data after hello")
	flag.DurationVar(&c.ProgressTimeout, "progress-timeout", c.ProgressTimeout, "maximum time without new data/ACK progress with pending buffers")
	flag.IntVar(&c.HelloRate, "hello-rate", c.HelloRate, "global hello processing rate per second")
	flag.IntVar(&c.HelloBurst, "hello-burst", c.HelloBurst, "global hello processing burst")
	legacyPacketAcks := flag.Bool("legacy-packet-acks", false, "emit Stage 19 one-packet ACKs for compatibility")
	noCongestion := flag.Bool("no-congestion", false, "LAB ONLY: disable DATA congestion window and pacing for A/B testing")
	flag.Parse()
	psk, err := secureudp.ParsePSK(*pskText)
	if err != nil {
		log.Fatal(err)
	}
	identity, err := secureudp.NewIdentity(psk)
	if err != nil {
		log.Fatal(err)
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	o := netreliable.Options{LegacyPacketAcks: *legacyPacketAcks, DisableCongestion: *noCongestion, DropMin: *dropMin, DropMax: *dropMax, Timeout: *timeout}
	if *multi {
		if *batch != 0 || *reliableMode {
			log.Fatal("--multi cannot be combined with --batch or --reliable")
		}
		c.Options = o
		s, err := netreliable.NewServer(identity, c)
		if err != nil {
			log.Fatal(err)
		}
		a, err := net.ResolveUDPAddr("udp", *addr)
		if err != nil {
			log.Fatal(err)
		}
		conn, err := net.ListenUDP("udp", a)
		if err != nil {
			log.Fatal(err)
		}
		fmt.Println("SERVER_PUBLIC=" + hex.EncodeToString(identity.Public.Bytes()))
		fmt.Println("LISTEN_ADDR=" + conn.LocalAddr().String())
		err = s.Run(ctx, conn)
		report, _ := json.MarshalIndent(s.Stats(), "", "  ")
		fmt.Println("SERVER_STATS=" + string(report))
		if *metricsFile != "" {
			if e := os.WriteFile(*metricsFile, append(report, '\n'), 0600); e != nil {
				log.Fatal(e)
			}
		}
		if err != nil {
			log.Fatal(err)
		}
		return
	}
	fmt.Fprintln(os.Stdout, "SERVER_PUBLIC="+hex.EncodeToString(identity.Public.Bytes()))
	if *batch > 0 {
		if *batch > 64 {
			log.Fatal("batch exceeds 64 messages")
		}
		if err := netreliable.ServeBatch(ctx, *addr, identity, *batch, o); err != nil {
			log.Fatal(err)
		}
		fmt.Println("PASS: batch completed")
		return
	}
	if *reliableMode {
		if err := netreliable.Serve(ctx, *addr, identity, o); err != nil {
			log.Fatal(err)
		}
		return
	}
	if err := secureudp.ServeEcho(ctx, *addr, identity); err != nil {
		log.Fatal(err)
	}
}
