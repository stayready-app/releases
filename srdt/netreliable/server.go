package netreliable

import (
	"container/heap"
	"context"
	"errors"
	"fmt"
	"net"
	"net/netip"
	"sync"
	"time"

	"example.com/srdt/reliable"
	"example.com/srdt/secureudp"
)

// ServerConfig describes bounded resources, not preallocated buffers. The echo
// service is an integration harness, not an Internet-ready proxy or VPN server.
type ServerConfig struct {
	Options              Options
	AllowLegacyData      bool // Explicit opt-in for pre-credit clients; default is strict admission.
	AllowLegacyHandshake bool // Explicit Stage 20 fixed-size HELLO compatibility; default is V1 only.
	AllowLegacyLiveness  bool // Explicit Stage 23 bridge compatibility without authenticated keepalive.
	MaxSessions          int
	MaxSessionsPerIP     int
	MaxAdmissions        int   // Never evict used hello IDs under the same server identity.
	PacketStateBudget    int64 // Separate bounded physical-transmission metadata, not payload credit.
	BufferBudget         int64
	SessionBufferBytes   int // Incoming + outgoing payloads, not two separate caps.
	MaxMessages          int // Incoming + outgoing messages per session.
	MaxMessageBytes      int
	WindowBytes          int
	IdleTimeout          time.Duration
	HandshakeTimeout     time.Duration
	ProgressTimeout      time.Duration
	Tick                 time.Duration
	FramesPerTurn        int
	HelloRate            int    // Global admission work cap, including invalid hellos.
	CompletionMessages   uint64 // Zero is persistent; legacy finite echo harnesses only.
	Upstream             string // Optional fixed TCP upstream for independent-stream bridge mode.
	MaxUpstreamDials     int
	UpstreamDialTimeout  time.Duration
	BridgeDrainTimeout   time.Duration // Retain a normally closed SRDT session briefly for duplicate FIN/ACK recovery.
	UpstreamSocketBuffer int           // Requested TCP read/write socket buffer per upstream connection; kernel accounting is outside RSS.
	UDPReadBuffer        int           // One shared UDP receive socket buffer; kernel accounting is outside RSS.
	UDPWriteBuffer       int           // One shared UDP send socket buffer; kernel accounting is outside RSS.
	HelloBurst           int
}

func DefaultServerConfig() ServerConfig {
	return ServerConfig{
		MaxSessions: 1024, MaxSessionsPerIP: 64, MaxAdmissions: 65536,
		PacketStateBudget: 16 << 20, BufferBudget: 64 << 20, SessionBufferBytes: 64 << 10,
		MaxMessages: 8, MaxMessageBytes: 32 << 10, WindowBytes: 16 << 10,
		IdleTimeout: 60 * time.Second, HandshakeTimeout: 5 * time.Second,
		ProgressTimeout: 15 * time.Second, Tick: 5 * time.Millisecond,
		FramesPerTurn: 8, HelloRate: 128, HelloBurst: 128, MaxUpstreamDials: 16, UpstreamDialTimeout: 2 * time.Second, BridgeDrainTimeout: time.Second, UpstreamSocketBuffer: 16 << 10,
		UDPReadBuffer: 2 << 20, UDPWriteBuffer: 1 << 20,
	}
}

func (c ServerConfig) validate() error {
	if (c.CompletionMessages != 0 && c.MaxSessions != 1) || c.MaxSessions < 1 || c.MaxSessions > 100000 || c.MaxSessionsPerIP < 1 ||
		c.MaxAdmissions < c.MaxSessions || c.MaxAdmissions > 1000000 ||
		c.BufferBudget < reliable.BufferCharge(c.MaxMessageBytes) ||
		c.PacketStateBudget < int64(c.MaxSessions)*reliable.PacketStateCharge ||
		c.SessionBufferBytes < c.MaxMessageBytes || c.SessionBufferBytes > reliable.MaxActiveBytes ||
		c.MaxMessageBytes < 1 || c.MaxMessageBytes > reliable.MaxMessageSize ||
		c.MaxMessages < 1 || c.MaxMessages > reliable.MaxActiveMessages ||
		c.WindowBytes < 1 || c.WindowBytes > c.SessionBufferBytes ||
		c.IdleTimeout <= 0 || c.HandshakeTimeout <= 0 || c.ProgressTimeout <= 0 ||
		c.Tick < time.Millisecond || c.FramesPerTurn < 1 || c.FramesPerTurn > 64 ||
		c.HelloRate < 1 || c.HelloBurst < 1 ||
		c.UDPReadBuffer < 64<<10 || c.UDPReadBuffer > 16<<20 || c.UDPWriteBuffer < 64<<10 || c.UDPWriteBuffer > 16<<20 ||
		(c.Upstream != "" && (c.MaxUpstreamDials < 1 || c.MaxUpstreamDials > 128 || c.UpstreamDialTimeout <= 0 || c.UpstreamDialTimeout > 10*time.Second || c.BridgeDrainTimeout < 50*time.Millisecond || c.BridgeDrainTimeout > 5*time.Second || c.UpstreamSocketBuffer < 4<<10 || c.UpstreamSocketBuffer > 1<<20)) {
		return errors.New("invalid multi-server resource configuration")
	}
	return nil
}

type ServerStats struct {
	ActiveBridges              int    `json:"active_bridges"`
	BridgeSessions             uint64 `json:"bridge_sessions"`
	BridgeDialFailures         uint64 `json:"bridge_dial_failures"`
	BridgeUpstreamReadBytes    uint64 `json:"bridge_upstream_read_bytes"`
	BridgeUpstreamWrittenBytes uint64 `json:"bridge_upstream_written_bytes"`
	BridgeHalfCloses           uint64 `json:"bridge_half_closes"`
	BridgeResets               uint64 `json:"bridge_resets"`
	CompletedBridgeSessions    uint64 `json:"completed_bridge_sessions"`
	PathPingsReceived          uint64 `json:"path_pings_received"`
	PathPongsSent              uint64 `json:"path_pongs_sent"`
	PacketStateBytes           int64  `json:"packet_state_bytes"`
	PeakPacketStateBytes       int64  `json:"peak_packet_state_bytes"`
	PacketTrackedSends         uint64 `json:"packet_tracked_sends"`
	PacketReceipts             uint64 `json:"packet_receipts"`
	PacketReceiptFrames        uint64 `json:"packet_receipt_frames"`
	PacketBatchReceiptFrames   uint64 `json:"packet_batch_receipt_frames"`
	PacketDuplicateReceipts    uint64 `json:"packet_duplicate_receipts"`
	PacketLateReceipts         uint64 `json:"packet_late_receipts"`
	PacketTimeoutLosses        uint64 `json:"packet_timeout_losses"`
	PacketThresholdLosses      uint64 `json:"packet_threshold_losses"`
	PacketRingWaits            uint64 `json:"packet_ring_waits"`
	PacketWireInFlight         int    `json:"packet_wire_in_flight"`
	PeakSessionWireInFlight    int    `json:"peak_session_wire_in_flight"`
	PeakSessionTrackedPackets  int    `json:"peak_session_tracked_packets"`
	ActiveSessions             int    `json:"active_sessions"`
	V1Sessions                 uint64 `json:"v1_sessions"`
	LegacyHandshakeSessions    uint64 `json:"legacy_handshake_sessions"`
	PeakSessions               int    `json:"peak_sessions"`
	UsedHelloIDs               int    `json:"used_hello_ids"`
	BufferBytes                int64  `json:"buffer_bytes"`
	PeakBufferBytes            int64  `json:"peak_buffer_bytes"`
	ReceivedDatagrams          uint64 `json:"received_datagrams"`
	SentDatagrams              uint64 `json:"sent_datagrams"`
	DeliveredMessages          uint64 `json:"delivered_messages"`
	DeliveredBytes             uint64 `json:"delivered_bytes"`
	InvalidPackets             uint64 `json:"invalid_packets"`
	AdmissionRejects           uint64 `json:"admission_rejects"`
	RateLimited                uint64 `json:"rate_limited"`
	ResourceDrops              uint64 `json:"resource_drops"`
	WindowRequests             uint64 `json:"window_requests"`
	WindowGrantUpdates         uint64 `json:"window_grant_updates"`
	CreditRequests             uint64 `json:"credit_requests"`
	CreditsGranted             uint64 `json:"credits_granted"`
	CreditBlocked              uint64 `json:"credit_blocked"`
	CreditViolations           uint64 `json:"credit_violations"`
	CreditRejected             uint64 `json:"credit_rejected"`
	LegacySessions             uint64 `json:"legacy_sessions"`
	WaitingSessions            int    `json:"waiting_sessions"`
	PeakWaitingSessions        int    `json:"peak_waiting_sessions"`
	StaleMessages              uint64 `json:"stale_messages"`
	RepeatedHellos             uint64 `json:"repeated_hellos"`
	ExpiredSessions            uint64 `json:"expired_sessions"`
	WriteErrors                uint64 `json:"write_errors"`
	DataFreshPackets           uint64 `json:"data_fresh_packets"`
	DataRetryPackets           uint64 `json:"data_retry_packets"`
	DataRetryBytes             uint64 `json:"data_retry_payload_bytes"`
	CongestionEvents           uint64 `json:"congestion_timeout_events"`
	PacingWaits                uint64 `json:"pacing_waits"`
	PeakCongestionWindow       int    `json:"peak_congestion_window"`
	FilteredDatagrams          uint64 `json:"filtered_datagrams"`
}

type peerSession struct {
	id              [16]byte
	peer            netip.AddrPort
	crypto          *secureudp.Session
	helloHash       [32]byte
	welcome         []byte
	lastWelcome     time.Time
	created         time.Time
	lastValid       time.Time
	lastProgress    time.Time
	firstData       time.Time
	confirmed       bool
	receiver        *reliable.Receiver // Lazy: no payload/map allocations for idle hello.
	queue           *reliable.Queue
	nextRequest     uint64
	completedAhead  map[uint64]struct{}
	due             time.Time
	heapIndex       int
	packetMode      byte // Pinned by first DATA: 1 legacy, 2 exact packet receipts.
	flowMode        byte // 0 unknown, 1 exact admission, 2 legacy, 3 rolling capacity window.
	windowCapacity  uint32
	windowWanted    uint64
	windowGranted   uint64
	lastWindowReply time.Time
	pendingOpens    []openRequest
	waitPrev        *peerSession
	waitNext        *peerSession
	waiting         bool
	lastBlocked     time.Time
	bridge          *bridgePeer
}

// Server owns one UDP socket and one event loop. No per-session/per-packet
// goroutines or timers. A heap keeps one scheduled event per live session.
// Identity also retains a process-local non-evicting replay registry across Server
// instances. A fresh key/Identity is needed for a new cryptographic lifetime.
type Server struct {
	mu            sync.Mutex // Only for concurrent Stats snapshots; session state has one owner.
	identity      *secureudp.Identity
	config        ServerConfig
	budget        *reliable.Budget
	packetBudget  *reliable.Budget
	sessions      map[[16]byte]*peerSession
	perIP         map[netip.Addr]int
	used          map[[16]byte]struct{}
	events        sessionHeap
	stats         ServerStats
	tokens        float64
	tokenAt       time.Time
	started       bool
	waitHead      *peerSession
	waitTail      *peerSession
	waitCount     int
	creditScan    int
	rxScratch     [secureudp.MaxDatagramSize]byte
	frameScratch  [secureudp.MaxDatagramSize]byte
	packetScratch [secureudp.MaxDatagramSize]byte
	runCtx        context.Context
	wake          func()
	bridgeEvents  chan bridgeEvent
	bridgeDialSem chan struct{}
}

func NewServer(identity *secureudp.Identity, config ServerConfig) (*Server, error) {
	if identity == nil || identity.Private == nil || len(identity.PSK) != 32 {
		return nil, errors.New("missing server identity")
	}
	if err := config.validate(); err != nil {
		return nil, err
	}
	budget, err := reliable.NewBudget(config.BufferBudget)
	if err != nil {
		return nil, err
	}
	packetBudget, _ := reliable.NewBudget(config.PacketStateBudget)
	s := &Server{
		packetBudget: packetBudget, identity: identity, config: config, budget: budget,
		sessions: make(map[[16]byte]*peerSession), perIP: make(map[netip.Addr]int),
		used: make(map[[16]byte]struct{}), tokens: float64(config.HelloBurst),
	}
	if config.Upstream != "" {
		capacity := min(config.MaxSessions*3, 4096)
		if capacity < 16 {
			capacity = 16
		}
		s.bridgeEvents = make(chan bridgeEvent, capacity)
		s.bridgeDialSem = make(chan struct{}, config.MaxUpstreamDials)
	}
	return s, nil
}

func (s *Server) Stats() ServerStats {
	s.mu.Lock()
	defer s.mu.Unlock()
	r := s.stats
	for _, p := range s.sessions {
		if p.bridge != nil && p.bridge.open && !p.bridge.closed {
			r.ActiveBridges++
		}
		if p.queue != nil {
			r.addControl(p.queue.CongestionStats())
			r.addPacket(p.queue.PacketStats())
			r.PacketWireInFlight += p.queue.PacketStats().WireInFlight
		}
	}
	r.ActiveSessions, r.UsedHelloIDs = len(s.sessions), len(s.used)
	r.BufferBytes = s.budget.Used()
	r.PacketStateBytes = s.packetBudget.Used()
	r.WaitingSessions = s.waitCount
	return r
}

// Run takes ownership of conn, including Close. It can be called only once.
// Cancellation is observed within the socket's bounded (<=100ms) read deadline.
func (s *Server) Run(ctx context.Context, conn *net.UDPConn) error {
	if conn == nil {
		return errors.New("nil UDP connection")
	}
	s.mu.Lock()
	if s.started {
		s.mu.Unlock()
		return errors.New("server already run")
	}
	s.started = true
	s.runCtx = ctx
	s.wake = func() { _ = conn.SetReadDeadline(time.Now()) }
	s.mu.Unlock()
	if err := conn.SetReadBuffer(s.config.UDPReadBuffer); err != nil {
		return fmt.Errorf("set UDP read buffer: %w", err)
	}
	if err := conn.SetWriteBuffer(s.config.UDPWriteBuffer); err != nil {
		return fmt.Errorf("set UDP write buffer: %w", err)
	}
	defer conn.Close()
	defer func() {
		s.mu.Lock()
		defer s.mu.Unlock()
		for _, p := range s.sessions {
			s.remove(p, false)
		}
	}()
	// An extra byte distinguishes oversize/truncated input from valid envelopes.
	buf := make([]byte, secureudp.MaxDatagramSize+1)
	send := func(packet []byte, peer netip.AddrPort) error {
		return writeDatagram(conn, packet, peer)
	}
	for {
		if ctx.Err() != nil {
			return nil
		}
		now := time.Now()
		s.mu.Lock()
		s.service(now, send)
		done := s.config.CompletionMessages > 0 && s.stats.DeliveredMessages == s.config.CompletionMessages
		if done {
			for _, p := range s.sessions {
				if p.queue != nil && p.queue.Pending() > 0 {
					done = false
					break
				}
			}
		}
		deadline := s.readDeadline(time.Now())
		s.mu.Unlock()
		if done {
			return nil
		}
		if err := conn.SetReadDeadline(deadline); err != nil {
			return err
		}
		n, peer, err := conn.ReadFromUDPAddrPort(buf)
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			if secureudp.IsDatagramReadError(err) {
				s.mu.Lock()
				s.stats.InvalidPackets++
				s.mu.Unlock()
				continue
			}
			if ne, ok := err.(net.Error); ok && ne.Timeout() {
				continue
			}
			return err
		}
		peer = netip.AddrPortFrom(peer.Addr().Unmap(), peer.Port())
		s.mu.Lock()
		s.handle(buf[:n], peer, time.Now(), send)
		s.mu.Unlock()
	}
}

func writeDatagram(conn *net.UDPConn, b []byte, peer netip.AddrPort) error {
	if err := conn.SetWriteDeadline(time.Now().Add(2 * time.Millisecond)); err != nil {
		return err
	}
	_, err := conn.WriteToUDPAddrPort(b, peer)
	return err
}

type sendDatagram func([]byte, netip.AddrPort) error

func (s *Server) send(packet []byte, peer netip.AddrPort, fn sendDatagram) {
	if err := fn(packet, peer); err != nil {
		s.stats.WriteErrors++
	} else {
		s.stats.SentDatagrams++
	}
}

func (s *Server) allowHello(now time.Time) bool {
	if s.tokenAt.IsZero() {
		s.tokenAt = now
	}
	s.tokens += now.Sub(s.tokenAt).Seconds() * float64(s.config.HelloRate)
	s.tokenAt = now
	if s.tokens > float64(s.config.HelloBurst) {
		s.tokens = float64(s.config.HelloBurst)
	}
	if s.tokens < 1 {
		s.stats.RateLimited++
		return false
	}
	s.tokens--
	return true
}

func (s *Server) handle(packet []byte, peer netip.AddrPort, now time.Time, send sendDatagram) {
	s.stats.ReceivedDatagrams++
	if len(packet) > secureudp.MaxDatagramSize {
		s.stats.InvalidPackets++
		return
	}
	id, ok := secureudp.SessionID(packet)
	if !ok {
		s.stats.InvalidPackets++
		return
	}
	if secureudp.IsHello(packet) {
		if !s.allowHello(now) {
			return
		}
		if p := s.sessions[id]; p != nil {
			fingerprint, valid := secureudp.HelloFingerprint(packet)
			if peer == p.peer && valid && fingerprint == p.helloHash {
				s.stats.RepeatedHellos++
				if now.Sub(p.lastWelcome) >= 50*time.Millisecond {
					// V1 may safely vary WELCOME length on retransmission while
					// retaining the same session and monotonically increasing nonce.
					if p.crypto.ProtocolVersion() == secureudp.ProtocolV1 {
						if fresh, err := secureudp.RefreshWelcome(p.crypto); err == nil {
							p.welcome = fresh
						}
					}
					s.send(p.welcome, peer, send)
					p.lastWelcome = now
				}
			} else {
				s.stats.InvalidPackets++
			}
			return // HELLO replay cannot extend the session lifetime.
		}
		_, seen := s.used[id]
		if seen || len(s.used) >= s.config.MaxAdmissions || len(s.sessions) >= s.config.MaxSessions || s.perIP[peer.Addr()] >= s.config.MaxSessionsPerIP {
			s.stats.AdmissionRejects++
			return
		}
		var welcome []byte
		var crypto *secureudp.Session
		var err error
		if secureudp.IsHelloV1(packet) {
			required := serverRequiredCapabilities()
			supported := serverSupportedCapabilities()
			if s.config.Upstream != "" {
				required |= secureudp.CapabilityStreaming | secureudp.CapabilityIndependentStreams | secureudp.CapabilityWindowCredits
				if !s.config.AllowLegacyLiveness {
					required |= secureudp.CapabilityPathLiveness
				}
			} else {
				supported &^= secureudp.CapabilityIndependentStreams
			}
			welcome, crypto, err = secureudp.AcceptHelloV1(s.identity, packet, supported, required)
		} else if s.config.AllowLegacyHandshake && secureudp.IsLegacyHello(packet) {
			welcome, crypto, err = secureudp.AcceptHello(s.identity, packet)
		} else {
			s.stats.AdmissionRejects++
			return
		}
		if err != nil {
			s.stats.InvalidPackets++
			return
		}
		if crypto.ProtocolVersion() == secureudp.ProtocolV1 {
			s.stats.V1Sessions++
		} else {
			s.stats.LegacyHandshakeSessions++
		}
		fingerprint, _ := secureudp.HelloFingerprint(packet)
		p := &peerSession{id: id, peer: peer, crypto: crypto, helloHash: fingerprint,
			welcome: welcome, created: now, lastValid: now, lastProgress: now,
			lastWelcome: now, nextRequest: 1, heapIndex: -1}
		s.sessions[id], s.used[id] = p, struct{}{}
		s.perIP[peer.Addr()]++
		if len(s.sessions) > s.stats.PeakSessions {
			s.stats.PeakSessions = len(s.sessions)
		}
		s.schedule(p, now, false)
		s.send(welcome, peer, send)
		return
	}
	p := s.sessions[id]
	if p == nil || p.peer != peer {
		s.stats.InvalidPackets++
		return
	}
	plain, err := p.crypto.OpenInto(s.rxScratch[:0], packet)
	if err != nil {
		s.stats.InvalidPackets++
		return
	}
	f, err := reliable.Decode(plain)
	if err != nil {
		s.stats.InvalidPackets++
		return
	}
	switch f.Type {
	case reliable.TypePathPing:
		if p.crypto.ProtocolVersion() != secureudp.ProtocolV1 || !p.crypto.HasCapability(secureudp.CapabilityPathLiveness) {
			s.stats.InvalidPackets++
			return
		}
		p.confirmed, p.lastValid = true, now
		s.stats.PathPingsReceived++
		s.sendFrame(p, reliable.Frame{Type: reliable.TypePathPong, ID: f.ID}, send)
		s.stats.PathPongsSent++
		if s.sessions[p.id] != nil {
			s.schedule(p, now, true)
		}
		return
	case reliable.TypePathPong:
		// Stage 24 uses client-originated probes. A server-side PONG has no
		// matching outstanding server probe and is therefore invalid.
		s.stats.InvalidPackets++
		return
	case reliable.TypeStreamReset:
		if p.bridge == nil || p.crypto.ProtocolVersion() != secureudp.ProtocolV1 || !p.crypto.HasCapability(secureudp.CapabilityIndependentStreams) {
			s.stats.InvalidPackets++
			return
		}
		p.confirmed, p.lastValid = true, now
		s.stats.BridgeResets++
		s.remove(p, false)
		return
	case reliable.TypeStreamOpen:
		s.handleStreamOpen(p, f, now, send)
		return
	case reliable.TypeStreamFin:
		s.handleStreamFin(p, f, now, send)
		return
	case reliable.TypeStreamFinAck:
		s.handleStreamFinAck(p, f, now)
		return
	case reliable.TypeStreamOpenAck:
		s.stats.InvalidPackets++
		return
	}
	if f.Type == reliable.TypeWindowOpen {
		if p.crypto.ProtocolVersion() == secureudp.ProtocolV1 && !p.crypto.HasCapability(secureudp.CapabilityWindowCredits) {
			s.stats.InvalidPackets++
			return
		}
		s.handleWindowOpen(p, f, now, send)
		return
	}
	if f.Type == reliable.TypeWindowGrant {
		if (p.crypto.ProtocolVersion() == secureudp.ProtocolV1 && !p.crypto.HasCapability(secureudp.CapabilityWindowCredits)) || p.flowMode != 3 || p.queue == nil {
			s.stats.InvalidPackets++
			return
		}
		blocked := p.queue.CreditBlockedMessages()
		changed, err := p.queue.GrantWindow(f)
		if err != nil {
			s.stats.InvalidPackets++
			return
		}
		p.confirmed, p.lastValid = true, now
		if changed && p.queue.CreditBlockedMessages() < blocked {
			p.lastProgress = now
		}
		s.schedule(p, now, true)
		return
	}
	if f.Type == reliable.TypeOpen {
		s.handleOpen(p, f, now, send)
		return
	}
	if f.Type == reliable.TypeGrant || f.Type == reliable.TypeBlocked || f.Type == reliable.TypeReject {
		s.handleCreditReply(p, f, now)
		return
	}
	if f.Type == reliable.TypeAck || f.Type == reliable.TypePacketAck || f.Type == reliable.TypePacketAckBatch {
		if p.crypto.ProtocolVersion() == secureudp.ProtocolV1 {
			if (f.Type == reliable.TypePacketAck || f.Type == reliable.TypePacketAckBatch) && !p.crypto.HasCapability(secureudp.CapabilityPacketTracking) {
				s.stats.InvalidPackets++
				return
			}
			if f.Type == reliable.TypePacketAckBatch && !p.crypto.HasCapability(secureudp.CapabilityPacketAckBatch) {
				s.stats.InvalidPackets++
				return
			}
		}
		if p.queue != nil {
			before := p.queue.InFlight()
			queued := p.queue.QueuedBytes()
			if p.queue.AckAt(f, now) == nil {
				p.confirmed, p.lastValid = true, now
				if p.queue.QueuedBytes() < queued {
					s.creditScan = s.waitCount
				}
				if p.queue.InFlight() < before {
					p.lastProgress = now
				}
			} else {
				s.stats.InvalidPackets++
			}
		}
		s.tryBridgeRead(p, now, send)
		s.maybeCloseBridgeIO(p)
		s.schedule(p, now, true)
		return
	}
	if (f.Type != reliable.TypeData && f.Type != reliable.TypePacketData) || f.ID == 0 || f.ID%2 == 0 || f.ID == ^uint64(0) || int(f.Total) > s.config.MaxMessageBytes {
		s.stats.InvalidPackets++
		return
	}
	if p.flowMode == 0 {
		if !s.config.AllowLegacyData {
			s.stats.CreditViolations++
			return
		}
		p.flowMode = 2
		s.stats.LegacySessions++
	}
	s.initDelivery(p)
	if p.bridge != nil && p.bridge.remoteFinSeen && f.ID > p.bridge.remoteFinID {
		s.stats.InvalidPackets++
		return
	}
	if p.bridge != nil && f.ID > p.bridge.highestInboundSeen {
		p.bridge.highestInboundSeen = f.ID
	}
	known := p.receiver.Known(f.ID)
	if (p.flowMode == 1 || p.flowMode == 3) && !known {
		s.stats.CreditViolations++ // DATA cannot create its own receiver reservation.
		return
	}
	if !known && s.config.CompletionMessages > 0 && s.stats.DeliveredMessages+uint64(p.receiver.ActiveMessages()) >= s.config.CompletionMessages {
		return
	}
	// IDs below a contiguous retirement watermark can never allocate again.
	// A bounded look-ahead prevents an arbitrarily large sparse completion map.
	if (f.ID < p.nextRequest && !known) || (f.ID >= p.nextRequest && (f.ID-p.nextRequest)/2 >= 128) {
		s.stats.StaleMessages++
		return
	}
	heldMessages, heldBytes := 0, 0
	if p.bridge != nil {
		heldMessages = len(p.bridge.received) + boolInt(p.bridge.writerBusy)
		heldBytes = p.bridge.heldBytes
	}
	if !known && (p.receiver.ActiveMessages()+p.queue.Pending()+heldMessages >= s.config.MaxMessages ||
		int(f.Total) > s.config.SessionBufferBytes-p.receiver.ActiveBytes()-p.queue.QueuedBytes()-heldBytes) {
		s.stats.ResourceDrops++ // No ACK: keep ownership at sender and retry later.
		return
	}
	mode := byte(1)
	if f.Type == reliable.TypePacketData {
		if p.crypto.ProtocolVersion() == secureudp.ProtocolV1 && !p.crypto.HasCapability(secureudp.CapabilityPacketTracking) {
			s.stats.InvalidPackets++
			return
		}
		mode = 2
	}
	if p.packetMode != 0 && p.packetMode != mode {
		s.stats.InvalidPackets++
		return
	}
	if p.packetMode == 0 {
		if mode == 2 {
			// Configuration preflights the maximum possible metadata; payload credit
			// cannot consume it and deadlock this already admitted DATA transaction.
			if err := p.queue.EnablePacketTracking(s.packetBudget, s.config.Options.fragment(), packetWireOverhead); err != nil {
				s.remove(p, false)
				return
			}
			s.stats.PeakPacketStateBytes = max(s.stats.PeakPacketStateBytes, s.packetBudget.Used())
		}
		p.packetMode = mode
	}
	before := p.receiver.AcceptedBytes()
	beforeBudget := s.budget.Used()
	acks, received, err := p.receiver.AcceptRetainedAt(f, now)
	if err != nil {
		if errors.Is(err, reliable.ErrFrame) || errors.Is(err, reliable.ErrStale) {
			s.stats.InvalidPackets++
		} else {
			s.stats.ResourceDrops++
		}
		return
	}
	if s.budget.Used() < beforeBudget {
		s.creditScan = s.waitCount
	}
	p.confirmed, p.lastValid = true, now
	if p.receiver.AcceptedBytes() > before {
		p.lastProgress = now
	}
	if received != nil {
		if p.completedAhead == nil {
			p.completedAhead = make(map[uint64]struct{})
		}
		p.completedAhead[f.ID] = struct{}{}
		for {
			if _, ok := p.completedAhead[p.nextRequest]; !ok {
				break
			}
			delete(p.completedAhead, p.nextRequest)
			p.nextRequest += 2
		}
		if p.bridge != nil && p.bridge.open {
			if _, exists := p.bridge.received[f.ID]; exists {
				received.Release()
			} else {
				p.bridge.received[f.ID] = received
				p.bridge.heldBytes += len(received.Data)
				s.tryBridgeWrite(p, now, send)
			}
		} else {
			// Echo harness retains the same budget charge while moving RX to TX.
			if err := p.queue.EnqueueReceived(f.ID+1, received, 80*time.Millisecond); err != nil {
				received.Release()
				s.remove(p, false)
				return
			}
		}
		s.stats.DeliveredMessages++
		s.stats.DeliveredBytes += uint64(f.Total)
		if p.firstData.IsZero() {
			p.firstData = now
		}
	}
	for _, ack := range acks {
		s.sendFrame(p, ack, send)
		if s.sessions[p.id] == nil {
			return
		}
	}
	if n := s.budget.Used(); n > s.stats.PeakBufferBytes {
		s.stats.PeakBufferBytes = n
	}
	s.schedule(p, now, true)
}

func (s *Server) sendFrame(p *peerSession, f reliable.Frame, send sendDatagram) {
	padding := 0
	if (p.crypto.HasCapability(secureudp.CapabilityControlPadding) && controlFrame(f.Type)) || f.Type == reliable.TypeWindowOpen || f.Type == reliable.TypeWindowGrant {
		padding = p.crypto.NextControlPadding()
	}
	encoded, err := encodeFrame(s.frameScratch[:0], f, p.crypto.HasCapability(secureudp.CapabilityControlPadding), padding)
	if err != nil {
		return
	}
	packet, err := p.crypto.SealInto(s.packetScratch[:0], encoded)
	if err != nil {
		s.remove(p, false)
		return
	}
	o := s.config.Options
	if o.DropMin > 0 && len(packet) >= o.DropMin && len(packet) <= o.DropMax {
		s.stats.FilteredDatagrams++
		return
	}
	s.send(packet, p.peer, send)
}

func (s *Server) expires(p *peerSession) time.Time {
	if p.bridge != nil && p.bridge.draining && !p.bridge.closedAt.IsZero() {
		return p.bridge.closedAt.Add(s.config.BridgeDrainTimeout)
	}
	if !p.confirmed {
		return p.created.Add(s.config.HandshakeTimeout)
	}
	expires := p.lastValid.Add(s.config.IdleTimeout)
	if s.hasWaitDemand(p) || (p.receiver != nil && (p.receiver.ActiveMessages() > 0 || p.queue.Pending() > 0)) {
		progress := p.lastProgress.Add(s.config.ProgressTimeout)
		if progress.Before(expires) {
			expires = progress
		}
	}
	return expires
}

func (s *Server) schedule(p *peerSession, now time.Time, immediate bool) {
	p.due = s.expires(p)
	if p.receiver != nil {
		if ackAt, ok := p.receiver.PacketAckWake(); ok && ackAt.Before(p.due) {
			p.due = ackAt
		}
	}
	if at, ok := s.bridgeWake(p); ok && at.Before(p.due) {
		p.due = at
	}
	if p.queue != nil {
		if sendAt, ok := p.queue.Wake(now); ok {
			if !immediate && s.config.Options.DisableCongestion && sendAt.Before(now.Add(s.config.Tick)) {
				sendAt = now.Add(s.config.Tick)
			}
			if sendAt.Before(p.due) {
				p.due = sendAt
			}
		}
	}
	if p.heapIndex < 0 {
		heap.Push(&s.events, p)
	} else {
		heap.Fix(&s.events, p.heapIndex)
	}
}

// readDeadline always permits socket reads, even with overdue timer work.
// A deadline in the past can prevent queued datagrams from ever being drained.
func (s *Server) readDeadline(now time.Time) time.Time {
	d := now.Add(100 * time.Millisecond)
	if s.creditScan > 0 {
		d = now.Add(time.Millisecond)
	}
	if len(s.events) > 0 && s.events[0].due.Before(d) {
		d = s.events[0].due
	}
	floor := now.Add(eventLoopMinimumSleep)
	if d.Before(floor) {
		d = floor
	}
	return d
}

func (s *Server) service(now time.Time, send sendDatagram) {
	turnStart := time.Now()
	frames := s.serviceBridgeEvents(now, send, turnStart)
	frames += s.serviceCredits(now, send, turnStart)
	// Bound timer work per loop so newly received traffic is not starved.
	for work := 0; work < 64 && frames < 32 && time.Since(turnStart) < 2*time.Millisecond && len(s.events) > 0 && !s.events[0].due.After(now); work++ {
		p := heap.Pop(&s.events).(*peerSession)
		if !now.Before(s.expires(p)) {
			if p.bridge != nil && p.bridge.draining {
				s.stats.CompletedBridgeSessions++
				s.remove(p, false)
			} else {
				s.remove(p, true)
			}
			continue
		}
		if p.receiver != nil {
			for _, ack := range p.receiver.FlushPacketAcks(now) {
				s.sendFrame(p, ack, send)
				frames++
			}
		}
		frames += s.serviceBridgeControl(p, now, send)
		if p.queue != nil {
			for n := 0; n < s.config.FramesPerTurn && frames < 32 && time.Since(turnStart) < 2*time.Millisecond; n++ {
				f, ok := p.queue.NextSized(now, s.config.Options.fragment(), s.config.Options.retryFragment())
				if !ok {
					break
				}
				// Packet ledgers are allocated lazily and may be released while a
				// session remains alive. Record the real process-wide high-water mark
				// when a DATA send can actually acquire that storage.
				s.stats.PeakPacketStateBytes = max(s.stats.PeakPacketStateBytes, s.packetBudget.Used())
				s.sendFrame(p, f, send)
				frames++
				if s.sessions[p.id] == nil {
					break
				}
			}
		}
		if s.sessions[p.id] != nil {
			s.tryBridgeRead(p, now, send)
			s.tryBridgeHalfClose(p)
			s.maybeCloseBridgeIO(p)
			s.schedule(p, now, false)
		}
	}
}

func (s *Server) remove(p *peerSession, expired bool) {
	if s.sessions[p.id] != p {
		return
	}
	if p.heapIndex >= 0 {
		heap.Remove(&s.events, p.heapIndex)
	}
	s.unlinkWaiter(p)
	p.pendingOpens = nil
	if p.bridge != nil {
		s.closeBridge(p)
	}
	if p.receiver != nil {
		p.receiver.Close()
	}
	if p.queue != nil {
		s.stats.addControl(p.queue.CongestionStats())
		s.stats.addPacket(p.queue.PacketStats())
		p.queue.Close()
	}
	s.creditScan = s.waitCount
	delete(s.sessions, p.id)
	ip := p.peer.Addr()
	s.perIP[ip]--
	if s.perIP[ip] == 0 {
		delete(s.perIP, ip)
	}
	// s.used intentionally retains the ID. At MaxAdmissions fail closed; never
	// reopen an old transcript with reset AES-GCM counters under the same key.
	if expired {
		s.stats.ExpiredSessions++
	}
}

type sessionHeap []*peerSession

func (h sessionHeap) Len() int           { return len(h) }
func (h sessionHeap) Less(i, j int) bool { return h[i].due.Before(h[j].due) }
func (h sessionHeap) Swap(i, j int)      { h[i], h[j] = h[j], h[i]; h[i].heapIndex = i; h[j].heapIndex = j }
func (h *sessionHeap) Push(x any)        { p := x.(*peerSession); p.heapIndex = len(*h); *h = append(*h, p) }
func (h *sessionHeap) Pop() any {
	a := *h
	n := len(a) - 1
	p := a[n]
	a[n] = nil
	*h = a[:n]
	p.heapIndex = -1
	return p
}

// ServeMulti is the CLI entry point; callers needing stats should use NewServer.
func ServeMulti(ctx context.Context, listen string, identity *secureudp.Identity, config ServerConfig) (ServerStats, error) {
	s, err := NewServer(identity, config)
	if err != nil {
		return ServerStats{}, err
	}
	addr, err := net.ResolveUDPAddr("udp", listen)
	if err != nil {
		return ServerStats{}, err
	}
	conn, err := net.ListenUDP("udp", addr)
	if err != nil {
		return ServerStats{}, fmt.Errorf("listen: %w", err)
	}
	err = s.Run(ctx, conn)
	return s.Stats(), err
}

func (s *ServerStats) addControl(c reliable.CongestionStats) {
	s.DataFreshPackets += c.FreshPackets
	s.DataRetryPackets += c.RetryPackets
	s.DataRetryBytes += c.RetryBytes
	s.CongestionEvents += c.LossEvents
	s.PacingWaits += c.PacingWaits
	s.PeakCongestionWindow = max(s.PeakCongestionWindow, c.PeakWindowBytes)
}

func (s *ServerStats) addPacket(p reliable.PacketStats) {
	s.PacketTrackedSends += p.Sent
	s.PacketReceipts += p.Receipts
	s.PacketReceiptFrames += p.ReceiptFrames
	s.PacketBatchReceiptFrames += p.BatchReceiptFrames
	s.PacketDuplicateReceipts += p.DuplicateReceipts
	s.PacketLateReceipts += p.LateReceipts
	s.PacketTimeoutLosses += p.Lost
	s.PacketThresholdLosses += p.ThresholdLost
	s.PacketRingWaits += p.RingWaits
	s.PeakSessionWireInFlight = max(s.PeakSessionWireInFlight, p.PeakWireInFlight)
	s.PeakSessionTrackedPackets = max(s.PeakSessionTrackedPackets, p.PeakOutstanding)
}
