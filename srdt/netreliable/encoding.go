package netreliable

import "example.com/srdt/reliable"

func controlFrame(t byte) bool {
	switch t {
	case reliable.TypeAck, reliable.TypeOpen, reliable.TypeGrant, reliable.TypeBlocked,
		reliable.TypeReject, reliable.TypeWindowOpen, reliable.TypeWindowGrant,
		reliable.TypePacketAck, reliable.TypePacketAckBatch, reliable.TypeStreamOpen,
		reliable.TypeStreamOpenAck, reliable.TypeStreamFin, reliable.TypeStreamFinAck, reliable.TypeStreamReset,
		reliable.TypePathPing, reliable.TypePathPong:
		return true
	default:
		return false
	}
}

func encodeFrame(dst []byte, f reliable.Frame, padControls bool, padding int) ([]byte, error) {
	// Stage 17 already envelopes rolling-window controls. Preserve that wire
	// behavior for explicit legacy handshake compatibility. V1 capability
	// negotiation extends the same authenticated envelope to every control frame.
	if (padControls && controlFrame(f.Type)) || f.Type == reliable.TypeWindowOpen || f.Type == reliable.TypeWindowGrant {
		if padding < 1 || padding > reliable.MaxEnvelopePadding {
			padding = 16
		}
		return reliable.EncodeEnvelopeInto(dst, f, padding)
	}
	return reliable.EncodeInto(dst, f)
}
