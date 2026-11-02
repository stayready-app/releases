package reliable

import (
	"encoding/binary"
	"testing"
)

func TestStage21ControlEnvelopeRoundTrip(t *testing.T) {
	ackLen := make([]byte, 4)
	binary.BigEndian.PutUint32(ackLen, 7)
	batch := make([]byte, 16)
	binary.BigEndian.PutUint64(batch[0:8], 11)
	binary.BigEndian.PutUint64(batch[8:16], 12)
	frames := []Frame{
		{Type: TypeAck, ID: 1, Offset: 0, Total: 7, Payload: ackLen},
		{Type: TypeOpen, ID: 1, Total: 7},
		{Type: TypeGrant, ID: 1, Total: 7},
		{Type: TypeBlocked, ID: 1, Total: 7, Offset: 250},
		{Type: TypeReject, ID: 1, Total: 7, Offset: 2},
		{Type: TypeWindowOpen, ID: 1, Total: 4096},
		{Type: TypeWindowGrant, ID: 1, Total: 4096},
		{Type: TypePacketAck, ID: 1, Offset: 0, Total: 7, Payload: ackLen, PacketNumber: 9},
		{Type: TypePacketAckBatch, Total: 2, Payload: batch},
	}
	for _, frame := range frames {
		encoded, err := EncodeEnvelopeInto(nil, frame, 37)
		if err != nil {
			t.Fatalf("type %d: %v", frame.Type, err)
		}
		decoded, err := Decode(encoded)
		if err != nil {
			t.Fatalf("type %d: %v", frame.Type, err)
		}
		if decoded.Type != frame.Type || decoded.ID != frame.ID || decoded.Offset != frame.Offset || decoded.Total != frame.Total || decoded.PacketNumber != frame.PacketNumber {
			t.Fatalf("type %d round trip mismatch: %#v", frame.Type, decoded)
		}
	}
	if _, err := EncodeEnvelopeInto(nil, Frame{Type: TypeData, ID: 1, Total: 1, Payload: []byte{1}}, 10); err == nil {
		t.Fatal("DATA unexpectedly accepted by control envelope")
	}
}
