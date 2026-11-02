package testkit

import (
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"math/rand"
)

// Bound fixture allocation and inclusive size arithmetic.
const MaxGeneratedMessage = 8 << 20

type Message struct {
	ID   uint64
	Data []byte
}
type Generator struct {
	rng *rand.Rand
	id  uint64
}

func NewGenerator(seed int64) *Generator { return &Generator{rng: rand.New(rand.NewSource(seed))} }
func (g *Generator) Next(min, max int) (Message, error) {
	if min < 0 || max < min || max > MaxGeneratedMessage || g.id == ^uint64(0) {
		return Message{}, errors.New("invalid sizes")
	}
	n := min
	if max > min {
		n += g.rng.Intn(max - min + 1)
	}
	b := make([]byte, n)
	for i := range b {
		b[i] = byte(g.rng.Intn(256))
	}
	g.id++
	return Message{g.id, b}, nil
}

// Encode is only a test fixture format, NOT the SRDT wire format.
func Encode(m Message) []byte {
	b := make([]byte, 12+len(m.Data))
	binary.BigEndian.PutUint64(b[:8], m.ID)
	binary.BigEndian.PutUint32(b[8:12], uint32(len(m.Data)))
	copy(b[12:], m.Data)
	return b
}
func Decode(b []byte) (Message, error) {
	if len(b) < 12 {
		return Message{}, errors.New("short frame")
	}
	n := binary.BigEndian.Uint32(b[8:12])
	if uint64(n) != uint64(len(b)-12) {
		return Message{}, errors.New("length mismatch")
	}
	return Message{binary.BigEndian.Uint64(b[:8]), append([]byte(nil), b[12:]...)}, nil
}

type Verifier struct {
	expected map[uint64][32]byte
	received map[uint64]bool
}

func NewVerifier(messages []Message) (*Verifier, error) {
	v := &Verifier{expected: map[uint64][32]byte{}, received: map[uint64]bool{}}
	for _, m := range messages {
		if _, ok := v.expected[m.ID]; ok {
			return nil, errors.New("duplicate expected id")
		}
		v.expected[m.ID] = sha256.Sum256(m.Data)
	}
	return v, nil
}
func (v *Verifier) Accept(m Message) error {
	hash, ok := v.expected[m.ID]
	if !ok {
		return fmt.Errorf("unexpected id %d", m.ID)
	}
	if hash != sha256.Sum256(m.Data) {
		return fmt.Errorf("corrupt id %d", m.ID)
	}
	if v.received[m.ID] {
		return fmt.Errorf("duplicate id %d", m.ID)
	}
	v.received[m.ID] = true
	return nil
}
func (v *Verifier) Complete() error {
	if len(v.received) != len(v.expected) {
		return fmt.Errorf("missing %d messages", len(v.expected)-len(v.received))
	}
	return nil
}
func (v *Verifier) Received() int { return len(v.received) }
