package reliable

import "encoding/binary"

const envelopeType byte = 0x80
const MaxEnvelopePadding = 256

func isControlType(t byte) bool {
	switch t {
	case TypeAck, TypeOpen, TypeGrant, TypeBlocked, TypeReject, TypeWindowOpen,
		TypeWindowGrant, TypePacketAck, TypePacketAckBatch, TypeStreamOpen, TypeStreamOpenAck,
		TypeStreamFin, TypeStreamFinAck, TypeStreamReset, TypePathPing, TypePathPong:
		return true
	default:
		return false
	}
}

// EncodeEnvelopeInto authenticates padding INSIDE the existing AEAD payload.
// It changes framing only; padding is zero-filled because AEAD already hides it.
// DATA frames are intentionally excluded: their size is controlled by the DATA
// scheduler rather than by the control-padding policy.
func EncodeEnvelopeInto(dst []byte, f Frame, padding int) ([]byte, error) {
	if padding < 1 || padding > MaxEnvelopePadding || validateFrame(f) != nil || !isControlType(f.Type) {
		return nil, ErrFrame
	}
	inner := HeaderSize + len(f.Payload)
	if f.PacketNumber != 0 {
		inner += 8
	}
	size := 3 + inner + padding
	if cap(dst) < size {
		dst = make([]byte, size)
	} else {
		dst = dst[:size]
	}
	dst[0] = envelopeType
	binary.BigEndian.PutUint16(dst[1:3], uint16(inner))
	if _, err := EncodeInto(dst[3:3], f); err != nil {
		return nil, err
	}
	clear(dst[size-padding:])
	return dst, nil
}

func unwrap(b []byte) ([]byte, error) {
	if len(b) == 0 || b[0] != envelopeType {
		return b, nil
	}
	if len(b) < 3 {
		return nil, ErrFrame
	}
	n := int(binary.BigEndian.Uint16(b[1:3]))
	padding := len(b) - 3 - n
	if n < HeaderSize || n > len(b)-3 || padding < 1 || padding > MaxEnvelopePadding || !isControlType(b[3]) {
		return nil, ErrFrame
	}
	return b[3 : 3+n], nil
}
