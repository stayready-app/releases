package secureudp

import (
	"bytes"
	"crypto/rand"
	"testing"
)

func TestHandshakeBidirectionalAndTamper(t *testing.T) {
	psk := make([]byte, 32)
	rand.Read(psk)
	server, err := NewIdentity(psk)
	if err != nil {
		t.Fatal(err)
	}
	hello, clientSession, err := ClientHello(server.Public, psk)
	if err != nil {
		t.Fatal(err)
	}
	welcome, serverSession, err := AcceptHello(server, hello)
	if err != nil {
		t.Fatal(err)
	}
	if err := VerifyWelcome(welcome, clientSession); err != nil {
		t.Fatal(err)
	}
	msg := []byte("hello world")
	packet, _ := clientSession.Seal(msg)
	tampered := bytes.Clone(packet)
	tampered[len(tampered)-1] ^= 1
	if _, err := serverSession.Open(tampered); err == nil {
		t.Fatal("tamper accepted")
	}
	received, err := serverSession.Open(packet)
	if err != nil || !bytes.Equal(received, msg) {
		t.Fatal("invalid delivery", err)
	}
	if _, err := serverSession.Open(packet); err == nil {
		t.Fatal("replay accepted")
	}
	response, _ := serverSession.Seal(msg)
	received, err = clientSession.Open(response)
	if err != nil || !bytes.Equal(received, msg) {
		t.Fatal("invalid reverse delivery", err)
	}
	forged := bytes.Clone(hello)
	forged[20] ^= 1
	if _, _, err := AcceptHello(server, forged); err == nil {
		t.Fatal("forged hello accepted")
	}
}

func TestOutOfOrderAndSize(t *testing.T) {
	psk := make([]byte, 32)
	rand.Read(psk)
	server, _ := NewIdentity(psk)
	hello, client, _ := ClientHello(server.Public, psk)
	_, peer, _ := AcceptHello(server, hello)
	a, _ := client.Seal([]byte("a"))
	b, _ := client.Seal([]byte("b"))
	if _, err := peer.Open(b); err != nil {
		t.Fatal(err)
	}
	if _, err := peer.Open(a); err != nil {
		t.Fatal(err)
	}
	if _, err := client.Seal(make([]byte, 1200)); err == nil {
		t.Fatal("oversize accepted")
	}
}
