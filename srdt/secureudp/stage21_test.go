package secureudp

import (
	"bytes"
	"testing"
)

func TestV1CapabilityNegotiationAndRepadding(t *testing.T) {
	psk := bytes.Repeat([]byte{0x31}, 32)
	identity, err := NewIdentity(psk)
	if err != nil {
		t.Fatal(err)
	}
	offered := CapabilityPacketTracking | CapabilityPacketAckBatch | CapabilityWindowCredits | CapabilityControlPadding | CapabilityStreaming
	required := CapabilityPacketTracking | CapabilityWindowCredits | CapabilityControlPadding
	hello, client, err := ClientHelloV1(identity.Public, psk, offered, required)
	if err != nil {
		t.Fatal(err)
	}
	if !IsHelloV1(hello) || len(hello) == 81 {
		t.Fatalf("unexpected V1 hello length %d", len(hello))
	}
	fingerprint, ok := HelloFingerprint(hello)
	if !ok {
		t.Fatal("missing fingerprint")
	}
	for i := 0; i < 8; i++ {
		repacked, err := RepadHelloV1(hello)
		if err != nil {
			t.Fatal(err)
		}
		other, ok := HelloFingerprint(repacked)
		if !ok || other != fingerprint {
			t.Fatal("padding changed authenticated hello identity")
		}
		hello = repacked
	}
	welcome, server, err := AcceptHelloV1(identity, hello, SupportedCapabilities, CapabilityControlPadding)
	if err != nil {
		t.Fatal(err)
	}
	if err := VerifyWelcome(welcome, client); err != nil {
		t.Fatal(err)
	}
	if client.ProtocolVersion() != ProtocolV1 || server.ProtocolVersion() != ProtocolV1 {
		t.Fatal("wrong negotiated protocol version")
	}
	if client.Capabilities() != offered || server.Capabilities() != offered {
		t.Fatalf("capabilities mismatch client=%x server=%x", client.Capabilities(), server.Capabilities())
	}
	fresh, err := RefreshWelcome(server)
	if err != nil {
		t.Fatal(err)
	}
	if err := VerifyWelcome(fresh, client); err != nil {
		t.Fatal(err)
	}
}

func TestV1RequiredCapabilityFailsClosed(t *testing.T) {
	psk := bytes.Repeat([]byte{0x42}, 32)
	identity, _ := NewIdentity(psk)
	hello, _, err := ClientHelloV1(identity.Public, psk, CapabilityStreaming|CapabilityControlPadding, CapabilityStreaming)
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := AcceptHelloV1(identity, hello, CapabilityControlPadding, CapabilityControlPadding); err != ErrIncompatible {
		t.Fatalf("expected incompatible, got %v", err)
	}
}

func TestV1TailCannotChangeAuthenticatedCapabilities(t *testing.T) {
	psk := bytes.Repeat([]byte{0x53}, 32)
	identity, _ := NewIdentity(psk)
	hello, _, _ := ClientHelloV1(identity.Public, psk, SupportedCapabilities, CapabilityControlPadding)
	// Tail is deliberately unauthenticated and semantically ignored.
	hello[len(hello)-1] ^= 0xff
	if _, _, err := AcceptHelloV1(identity, hello, SupportedCapabilities, CapabilityControlPadding); err != nil {
		t.Fatal(err)
	}
}
