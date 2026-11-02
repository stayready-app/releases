package testkit

import (
	"bytes"
	"testing"
)

func TestGeneratorAndVerifier(t *testing.T) {
	a := NewGenerator(9)
	b := NewGenerator(9)
	var msgs []Message
	for i := 0; i < 200; i++ {
		x, _ := a.Next(0, 8192)
		y, _ := b.Next(0, 8192)
		if x.ID != y.ID || !bytes.Equal(x.Data, y.Data) {
			t.Fatal("seed mismatch")
		}
		msgs = append(msgs, x)
	}
	v, _ := NewVerifier(msgs)
	for _, x := range msgs {
		m, err := Decode(Encode(x))
		if err != nil || v.Accept(m) != nil {
			t.Fatal("round trip")
		}
	}
	if err := v.Complete(); err != nil {
		t.Fatal(err)
	}
	if v.Accept(msgs[0]) == nil {
		t.Fatal("duplicate accepted")
	}
}
func TestCorruption(t *testing.T) {
	m := Message{1, []byte{1, 2, 3}}
	v, _ := NewVerifier([]Message{m})
	b := Encode(m)
	b[len(b)-1] ^= 1
	x, _ := Decode(b)
	if v.Accept(x) == nil {
		t.Fatal("corruption accepted")
	}
	if _, err := Decode(b[:4]); err == nil {
		t.Fatal("short frame")
	}
}
