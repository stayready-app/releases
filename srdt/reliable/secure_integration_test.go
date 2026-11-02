package reliable

import (
	"bytes"
	"crypto/rand"
	"testing"
	"time"

	"example.com/srdt/secureudp"
)

// The exact same reliable frames are encrypted using the stage-2 session.
// A deterministic drop policy discards all full-size transmissions initially.
func TestEncryptedReliableDelivery(t *testing.T) {
	psk := make([]byte, 32)
	if _, err := rand.Read(psk); err != nil {
		t.Fatal(err)
	}
	server, err := secureudp.NewIdentity(psk)
	if err != nil {
		t.Fatal(err)
	}
	hello, client, err := secureudp.ClientHello(server.Public, psk)
	if err != nil {
		t.Fatal(err)
	}
	welcome, peer, err := secureudp.AcceptHello(server, hello)
	if err != nil {
		t.Fatal(err)
	}
	if err := secureudp.VerifyWelcome(welcome, client); err != nil {
		t.Fatal(err)
	}
	data := make([]byte, 8192)
	rand.Read(data)
	sender, _ := NewSender(91, data, 5*time.Millisecond)
	receiver := NewReceiver()
	now := time.Unix(1, 0)
	delivered := 0
	for i := 0; i < 500 && !sender.Done(); i++ {
		size := 900
		if i < 12 {
			size = 1100
		}
		f, ok := sender.Next(now, size)
		now = now.Add(6 * time.Millisecond)
		if !ok {
			continue
		}
		raw, _ := Encode(f)
		encrypted, err := client.Seal(raw)
		if err != nil {
			t.Fatal(err)
		}
		if len(f.Payload) == 1100 {
			continue
		}
		plain, err := peer.Open(encrypted)
		if err != nil {
			t.Fatal(err)
		}
		decoded, err := Decode(plain)
		if err != nil {
			t.Fatal(err)
		}
		acks, result, err := receiver.Accept(decoded)
		if err != nil {
			t.Fatal(err)
		}
		if result != nil {
			delivered++
			if !bytes.Equal(result, data) {
				t.Fatal("corrupt delivery")
			}
		}
		for _, ack := range acks {
			ackBytes, _ := Encode(ack)
			sealed, err := peer.Seal(ackBytes)
			if err != nil {
				t.Fatal(err)
			}
			opened, err := client.Open(sealed)
			if err != nil {
				t.Fatal(err)
			}
			parsed, err := Decode(opened)
			if err != nil || sender.Ack(parsed) != nil {
				t.Fatal("bad ack")
			}
		}
	}
	if !sender.Done() || delivered != 1 {
		t.Fatalf("done=%v deliveries=%d", sender.Done(), delivered)
	}
}
