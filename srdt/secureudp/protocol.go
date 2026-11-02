package secureudp

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/ecdh"
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"sync"
)

const (
	helloType   byte = 1
	welcomeType byte = 2
	dataType    byte = 3
	helloV1Type byte = 4
	headerSize       = 1 + 16 + 8
	maxDatagram      = 1200

	ProtocolLegacy uint16 = 0
	ProtocolV1     uint16 = 1

	v1HelloCoreSize    = 1 + 16 + 32 + 2 + 8 + 8
	v1HelloMACSize     = 32
	v1HelloFixedSize   = v1HelloCoreSize + v1HelloMACSize
	v1HelloMinPadding  = 16
	v1HelloMaxPadding  = 143
	v1WelcomeFixedSize = 16 + 2 + 8 + 2
	v1WelcomeMinPad    = 16
	v1WelcomeMaxPad    = 143
)

type Capabilities uint64

const (
	CapabilityPacketTracking Capabilities = 1 << iota
	CapabilityPacketAckBatch
	CapabilityWindowCredits
	CapabilityControlPadding
	CapabilityStreaming
	CapabilityAdaptiveRTO
	CapabilityCongestionControl
	CapabilityIndependentStreams
	CapabilityPathLiveness
)

const SupportedCapabilities = CapabilityPacketTracking |
	CapabilityPacketAckBatch |
	CapabilityWindowCredits |
	CapabilityControlPadding |
	CapabilityStreaming |
	CapabilityAdaptiveRTO |
	CapabilityCongestionControl |
	CapabilityIndependentStreams |
	CapabilityPathLiveness

var ErrInvalid = errors.New("invalid or unauthenticated packet")
var ErrIncompatible = errors.New("incompatible protocol capabilities")

type Identity struct {
	Private  *ecdh.PrivateKey
	Public   *ecdh.PublicKey
	PSK      []byte
	mu       sync.Mutex
	accepted map[[16]byte]struct{}
}

func NewIdentity(psk []byte) (*Identity, error) {
	if len(psk) != 32 {
		return nil, fmt.Errorf("PSK must be 32 bytes")
	}
	key, err := ecdh.X25519().GenerateKey(rand.Reader)
	if err != nil {
		return nil, err
	}
	return &Identity{Private: key, Public: key.PublicKey(), PSK: append([]byte(nil), psk...)}, nil
}

func mac(key []byte, payload []byte) []byte {
	h := hmac.New(sha256.New, key)
	h.Write(payload)
	return h.Sum(nil)
}

func derive(secret, psk, transcript []byte, label string) ([]byte, error) {
	// HKDF extract and expand (RFC 5869), SHA-256, one output block.
	salt := mac(psk, transcript)
	prk := mac(salt, secret)
	info := append([]byte("SRDT-v0/"+label), 1)
	return mac(prk, info), nil
}

type Session struct {
	id        [16]byte
	tx        cipher.AEAD
	rx        cipher.AEAD
	txCounter uint64
	rxHighest uint64
	rxBitmap  uint64

	protocol     uint16
	offered      Capabilities
	required     Capabilities
	capabilities Capabilities
	padCounter   uint64
	padState     uint64
}

func newSession(id [16]byte, txKey, rxKey []byte) (*Session, error) {
	a, err := aes.NewCipher(txKey)
	if err != nil {
		return nil, err
	}
	tx, err := cipher.NewGCM(a)
	if err != nil {
		return nil, err
	}
	b, err := aes.NewCipher(rxKey)
	if err != nil {
		return nil, err
	}
	rx, err := cipher.NewGCM(b)
	if err != nil {
		return nil, err
	}
	seedInput := make([]byte, 0, len(txKey)+len(rxKey))
	seedInput = append(seedInput, txKey...)
	seedInput = append(seedInput, rxKey...)
	seed := sha256.Sum256(seedInput)
	state := binary.BigEndian.Uint64(seed[:8])
	if state == 0 {
		state = 0x9e3779b97f4a7c15
	}
	return &Session{id: id, tx: tx, rx: rx, padState: state}, nil
}

func (s *Session) ProtocolVersion() uint16 {
	if s == nil {
		return ProtocolLegacy
	}
	return s.protocol
}

func (s *Session) Capabilities() Capabilities {
	if s == nil {
		return 0
	}
	return s.capabilities
}

func (s *Session) HasCapability(c Capabilities) bool {
	return s != nil && s.capabilities&c == c
}

// NextControlPadding returns 16..255 bytes while rotating through four broad
// bands. Retries therefore do not depend on repeatedly drawing a lucky random
// length when a network selectively blocks one contiguous size range. Jitter is
// generated from per-session secret-derived state; it carries no cryptographic
// authority and is not used as a nonce or key.
func (s *Session) NextControlPadding() int {
	if s == nil {
		return 16
	}
	x := s.padState
	x ^= x << 13
	x ^= x >> 7
	x ^= x << 17
	s.padState = x
	band := int(s.padCounter & 3)
	s.padCounter++
	return 16 + band*60 + int(x%60)
}

func nonce(counter uint64) []byte {
	n := make([]byte, 12)
	binary.BigEndian.PutUint64(n[4:], counter)
	return n
}

// Seal returns a fresh packet. Session is single-owner, not concurrent-safe.
func (s *Session) Seal(plain []byte) ([]byte, error) { return s.SealInto(nil, plain) }

// SealInto reuses dst capacity. dst must not alias plain; the result remains
// valid only until the owner reuses dst. Each NEW encryption consumes a nonce.
func (s *Session) SealInto(dst, plain []byte) ([]byte, error) {
	if len(plain) > maxDatagram-headerSize-s.tx.Overhead() {
		return nil, fmt.Errorf("message too large")
	}
	if s.txCounter == ^uint64(0) {
		return nil, fmt.Errorf("counter exhausted")
	}
	s.txCounter++
	size := headerSize + len(plain) + s.tx.Overhead()
	var out []byte
	if cap(dst) < size {
		out = make([]byte, headerSize, size)
	} else {
		out = dst[:headerSize]
	}
	out[0] = dataType
	copy(out[1:17], s.id[:])
	binary.BigEndian.PutUint64(out[17:25], s.txCounter)
	return s.tx.Seal(out, nonce(s.txCounter), plain, out), nil
}

func (s *Session) Open(packet []byte) ([]byte, error) { return s.OpenInto(nil, packet) }

// OpenInto reuses dst; it must not overlap packet. Authentication failure cannot
// advance replay state. Unknown or oversized input is rejected before decryption.
func (s *Session) OpenInto(dst, packet []byte) ([]byte, error) {
	if len(packet) > maxDatagram || len(packet) < headerSize+s.rx.Overhead() || packet[0] != dataType || !hmac.Equal(packet[1:17], s.id[:]) {
		return nil, ErrInvalid
	}
	counter := binary.BigEndian.Uint64(packet[17:25])
	if counter == 0 {
		return nil, ErrInvalid
	}
	if counter <= s.rxHighest {
		delta := s.rxHighest - counter
		if delta >= 64 || s.rxBitmap&(uint64(1)<<delta) != 0 {
			return nil, ErrInvalid
		}
	}
	plain, err := s.rx.Open(dst[:0], nonce(counter), packet[headerSize:], packet[:headerSize])
	if err != nil {
		return nil, ErrInvalid
	}
	if counter > s.rxHighest {
		delta := counter - s.rxHighest
		if delta >= 64 {
			s.rxBitmap = 0
		} else {
			s.rxBitmap <<= delta
		}
		s.rxBitmap |= 1
		s.rxHighest = counter
	} else {
		s.rxBitmap |= uint64(1) << (s.rxHighest - counter)
	}
	return plain, nil
}

func acceptIdentityID(server *Identity, id [16]byte) error {
	server.mu.Lock()
	defer server.mu.Unlock()
	if _, seen := server.accepted[id]; seen || len(server.accepted) >= MaxIdentitySessions {
		return ErrInvalid
	}
	if server.accepted == nil {
		server.accepted = make(map[[16]byte]struct{})
	}
	server.accepted[id] = struct{}{}
	return nil
}

// ClientHello is the Stage 20 legacy handshake. It is retained only for explicit
// compatibility paths. New SRDT reliable sessions should use ClientHelloV1.
func ClientHello(serverPublic *ecdh.PublicKey, psk []byte) ([]byte, *Session, error) {
	if serverPublic == nil || serverPublic.Curve() != ecdh.X25519() || len(psk) != 32 {
		return nil, nil, ErrInvalid
	}
	ephemeral, err := ecdh.X25519().GenerateKey(rand.Reader)
	if err != nil {
		return nil, nil, err
	}
	secret, err := ephemeral.ECDH(serverPublic)
	if err != nil {
		return nil, nil, err
	}
	var id [16]byte
	if _, err = rand.Read(id[:]); err != nil {
		return nil, nil, err
	}
	hello := make([]byte, 1+16+32+32)
	hello[0] = helloType
	copy(hello[1:17], id[:])
	copy(hello[17:49], ephemeral.PublicKey().Bytes())
	copy(hello[49:], mac(psk, hello[:49]))
	tx, _ := derive(secret, psk, hello[:49], "client-to-server")
	rx, _ := derive(secret, psk, hello[:49], "server-to-client")
	session, err := newSession(id, tx, rx)
	if session != nil {
		session.protocol = ProtocolLegacy
	}
	return hello, session, err
}

// ClientHelloV1 authenticates protocol version and capabilities in the PSK MAC.
// Random authenticated padding intentionally makes the first UDP datagram variable
// length. Padding is not a cryptographic primitive and does not claim DPI evasion.
func ClientHelloV1(serverPublic *ecdh.PublicKey, psk []byte, offered, required Capabilities) ([]byte, *Session, error) {
	if serverPublic == nil || serverPublic.Curve() != ecdh.X25519() || len(psk) != 32 || required&^offered != 0 || offered&^SupportedCapabilities != 0 {
		return nil, nil, ErrInvalid
	}
	ephemeral, err := ecdh.X25519().GenerateKey(rand.Reader)
	if err != nil {
		return nil, nil, err
	}
	secret, err := ephemeral.ECDH(serverPublic)
	if err != nil {
		return nil, nil, err
	}
	var id [16]byte
	if _, err = rand.Read(id[:]); err != nil {
		return nil, nil, err
	}
	core := make([]byte, v1HelloFixedSize)
	core[0] = helloV1Type
	copy(core[1:17], id[:])
	copy(core[17:49], ephemeral.PublicKey().Bytes())
	binary.BigEndian.PutUint16(core[49:51], ProtocolV1)
	binary.BigEndian.PutUint64(core[51:59], uint64(offered))
	binary.BigEndian.PutUint64(core[59:67], uint64(required))
	copy(core[v1HelloCoreSize:], mac(psk, core[:v1HelloCoreSize]))
	hello, err := RepadHelloV1(core)
	if err != nil {
		return nil, nil, err
	}
	tx, _ := derive(secret, psk, core[:v1HelloCoreSize], "client-to-server-v1")
	rx, _ := derive(secret, psk, core[:v1HelloCoreSize], "server-to-client-v1")
	session, err := newSession(id, tx, rx)
	if session != nil {
		session.protocol = ProtocolV1
		session.offered = offered
		session.required = required
	}
	return hello, session, err
}

// RepadHelloV1 changes only an unauthenticated, semantically ignored size tail.
// The authenticated core, session ID, ECDH key and derived traffic keys remain
// unchanged, so a retransmission can choose a different UDP length without
// creating another server-side session. The tail carries no authority.
func RepadHelloV1(hello []byte) ([]byte, error) {
	if len(hello) < v1HelloFixedSize || hello[0] != helloV1Type || binary.BigEndian.Uint16(hello[49:51]) != ProtocolV1 {
		return nil, ErrInvalid
	}
	var choice [1]byte
	if _, err := rand.Read(choice[:]); err != nil {
		return nil, err
	}
	padding := v1HelloMinPadding + int(choice[0])%(v1HelloMaxPadding-v1HelloMinPadding+1)
	out := make([]byte, v1HelloFixedSize+padding)
	copy(out, hello[:v1HelloFixedSize])
	if _, err := rand.Read(out[v1HelloFixedSize:]); err != nil {
		return nil, err
	}
	return out, nil
}

func AcceptHello(server *Identity, hello []byte) ([]byte, *Session, error) {
	if server == nil || server.Private == nil || server.Private.Curve() != ecdh.X25519() || len(server.PSK) != 32 || len(hello) != 81 || hello[0] != helloType || !hmac.Equal(mac(server.PSK, hello[:49]), hello[49:]) {
		return nil, nil, ErrInvalid
	}
	public, err := ecdh.X25519().NewPublicKey(hello[17:49])
	if err != nil {
		return nil, nil, ErrInvalid
	}
	secret, err := server.Private.ECDH(public)
	if err != nil {
		return nil, nil, ErrInvalid
	}
	var id [16]byte
	copy(id[:], hello[1:17])
	if err := acceptIdentityID(server, id); err != nil {
		return nil, nil, err
	}
	tx, _ := derive(secret, server.PSK, hello[:49], "server-to-client")
	rx, _ := derive(secret, server.PSK, hello[:49], "client-to-server")
	session, err := newSession(id, tx, rx)
	if err != nil {
		return nil, nil, err
	}
	session.protocol = ProtocolLegacy
	welcome, err := session.Seal([]byte("SRDT-WELCOME-v0"))
	return welcome, session, err
}

func parseHelloV1(hello []byte) (id [16]byte, publicBytes []byte, offered, required Capabilities, transcript []byte, ok bool) {
	if len(hello) < v1HelloFixedSize+v1HelloMinPadding || len(hello) > v1HelloFixedSize+v1HelloMaxPadding || hello[0] != helloV1Type {
		return id, nil, 0, 0, nil, false
	}
	if binary.BigEndian.Uint16(hello[49:51]) != ProtocolV1 {
		return id, nil, 0, 0, nil, false
	}
	copy(id[:], hello[1:17])
	offered = Capabilities(binary.BigEndian.Uint64(hello[51:59]))
	required = Capabilities(binary.BigEndian.Uint64(hello[59:67]))
	if required&^offered != 0 {
		return id, nil, 0, 0, nil, false
	}
	return id, hello[17:49], offered, required, hello[:v1HelloCoreSize], true
}

// AcceptHelloV1 selects only mutually supported authenticated capabilities.
// requiredByServer is a fail-closed policy: no implicit downgrade is performed.
func AcceptHelloV1(server *Identity, hello []byte, supported, requiredByServer Capabilities) ([]byte, *Session, error) {
	if server == nil || server.Private == nil || server.Private.Curve() != ecdh.X25519() || len(server.PSK) != 32 || supported&^SupportedCapabilities != 0 || requiredByServer&^supported != 0 {
		return nil, nil, ErrInvalid
	}
	id, publicBytes, offered, requiredByClient, transcript, ok := parseHelloV1(hello)
	if !ok || !hmac.Equal(mac(server.PSK, transcript), hello[v1HelloCoreSize:v1HelloFixedSize]) {
		return nil, nil, ErrInvalid
	}
	selected := offered & supported
	if requiredByClient&^selected != 0 || requiredByServer&^selected != 0 {
		return nil, nil, ErrIncompatible
	}
	public, err := ecdh.X25519().NewPublicKey(publicBytes)
	if err != nil {
		return nil, nil, ErrInvalid
	}
	secret, err := server.Private.ECDH(public)
	if err != nil {
		return nil, nil, ErrInvalid
	}
	if err := acceptIdentityID(server, id); err != nil {
		return nil, nil, err
	}
	tx, _ := derive(secret, server.PSK, transcript, "server-to-client-v1")
	rx, _ := derive(secret, server.PSK, transcript, "client-to-server-v1")
	session, err := newSession(id, tx, rx)
	if err != nil {
		return nil, nil, err
	}
	session.protocol = ProtocolV1
	session.offered = offered
	session.required = requiredByClient | requiredByServer
	session.capabilities = selected
	welcome, err := RefreshWelcome(session)
	return welcome, session, err
}

// RefreshWelcome safely re-encrypts the same V1 negotiation result with a
// fresh AEAD counter and fresh encrypted padding. It is used only after a
// repeated authenticated HELLO for an already live session.
func RefreshWelcome(session *Session) ([]byte, error) {
	if session == nil || session.protocol != ProtocolV1 {
		return nil, ErrInvalid
	}
	plain, err := encodeWelcomeV1(session.capabilities)
	if err != nil {
		return nil, err
	}
	return session.Seal(plain)
}

func encodeWelcomeV1(selected Capabilities) ([]byte, error) {
	var choice [1]byte
	if _, err := rand.Read(choice[:]); err != nil {
		return nil, err
	}
	padding := v1WelcomeMinPad + int(choice[0])%(v1WelcomeMaxPad-v1WelcomeMinPad+1)
	plain := make([]byte, v1WelcomeFixedSize+padding)
	copy(plain[:16], []byte("SRDT-WELCOME-v1!"))
	binary.BigEndian.PutUint16(plain[16:18], ProtocolV1)
	binary.BigEndian.PutUint64(plain[18:26], uint64(selected))
	binary.BigEndian.PutUint16(plain[26:28], uint16(padding))
	if _, err := rand.Read(plain[28:]); err != nil {
		return nil, err
	}
	return plain, nil
}

func verifyWelcomeV1(plain []byte, session *Session) error {
	if len(plain) < v1WelcomeFixedSize+v1WelcomeMinPad || len(plain) > v1WelcomeFixedSize+v1WelcomeMaxPad || string(plain[:16]) != "SRDT-WELCOME-v1!" || binary.BigEndian.Uint16(plain[16:18]) != ProtocolV1 {
		return ErrInvalid
	}
	padding := int(binary.BigEndian.Uint16(plain[26:28]))
	if padding < v1WelcomeMinPad || padding > v1WelcomeMaxPad || len(plain) != v1WelcomeFixedSize+padding {
		return ErrInvalid
	}
	selected := Capabilities(binary.BigEndian.Uint64(plain[18:26]))
	if selected&^session.offered != 0 || session.required&^selected != 0 || selected&^SupportedCapabilities != 0 {
		return ErrIncompatible
	}
	session.capabilities = selected
	return nil
}

func VerifyWelcome(welcome []byte, session *Session) error {
	if session == nil {
		return ErrInvalid
	}
	plain, err := session.Open(welcome)
	if err != nil {
		return ErrInvalid
	}
	if session.protocol == ProtocolV1 {
		return verifyWelcomeV1(plain, session)
	}
	if string(plain) != "SRDT-WELCOME-v0" {
		return ErrInvalid
	}
	return nil
}

func SessionID(packet []byte) ([16]byte, bool) {
	var id [16]byte
	if len(packet) < 17 {
		return id, false
	}
	copy(id[:], packet[1:17])
	return id, true
}

// HelloFingerprint ignores the V1 size-only tail. This lets a client repack
// a retransmission without creating a distinct cryptographic transcript.
func HelloFingerprint(packet []byte) ([32]byte, bool) {
	if IsLegacyHello(packet) {
		return sha256.Sum256(packet), true
	}
	if IsHelloV1(packet) {
		return sha256.Sum256(packet[:v1HelloFixedSize]), true
	}
	return [32]byte{}, false
}

// MaxDatagramSize is the current prototype's complete UDP payload limit.
// A larger experimental carrier requires a separate wire-format change.
const MaxDatagramSize = maxDatagram

// IsHello identifies only the envelope. It does not authenticate a peer.
func IsHello(packet []byte) bool       { return IsLegacyHello(packet) || IsHelloV1(packet) }
func IsLegacyHello(packet []byte) bool { return len(packet) == 81 && packet[0] == helloType }
func IsHelloV1(packet []byte) bool {
	_, _, _, _, _, ok := parseHelloV1(packet)
	return ok
}

// MaxIdentitySessions bounds the shared handshake replay registry. Entries are
// NEVER evicted under a live private key. Identity must not be copied, mutated,
// reconstructed with an old private key, or used after its registry is lost.
const MaxIdentitySessions = 1_000_000
