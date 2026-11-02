package netreliable

import (
	"bytes"
	"context"
	"testing"
	"time"

	"example.com/srdt/reliable"
)

func TestPacketModernAndExplicitLegacy(t *testing.T) {
	for _, legacy := range []bool{false, true} {
		t.Run(map[bool]string{false: "packet-receipts", true: "range-only-compatibility"}[legacy], func(t *testing.T) {
			_, i, c := testMulti(t)
			c.AllowLegacyData = false
			s, addr, stop := startMulti(t, c, i)
			data := [][]byte{bytes.Repeat([]byte{5}, 4096), bytes.Repeat([]byte{7}, 12000)}
			e := ExchangeBatch(context.Background(), addr, i.Public, i.PSK, data, Options{LegacyPackets: legacy, Timeout: 10 * time.Second})
			st := s.Stats()
			stop()
			if e != nil {
				t.Fatal(e)
			}
			if (st.PacketTrackedSends > 0) == legacy || (st.PacketReceipts > 0) == legacy {
				t.Fatal(st)
			}
			if st.PeakSessionTrackedPackets > reliable.PacketSlots {
				t.Fatal("unbounded tracking", st)
			}
			if s.Stats().PacketStateBytes != 0 || s.Stats().BufferBytes != 0 {
				t.Fatal("shutdown leak", s.Stats())
			}
		})
	}
}
func TestPacketBudgetPreflightAndMixedMode(t *testing.T) {
	_, identity, c := testMulti(t)
	c.PacketStateBudget = int64(c.MaxSessions)*reliable.PacketStateCharge - 1
	if _, e := NewServer(identity, c); e == nil {
		t.Fatal("accepted potential metadata overcommit")
	}
	c.PacketStateBudget++
	s, e := NewServer(identity, c)
	if e != nil {
		t.Fatal(e)
	}
	now := time.Now()
	_, client, p := admit(t, s, identity, 12345, now)
	if s.packetBudget.Used() != 0 {
		t.Fatal("idle allocated packet ledger")
	}
	// A complete inbound packet negotiates packet mode and queues an echo, but
	// must not allocate outbound packet state until the scheduler sends DATA.
	sendClient(t, s, client, p, reliable.Frame{Type: reliable.TypePacketData, ID: 1, Total: 1, Payload: []byte{1}, PacketNumber: 1}, now)
	if s.packetBudget.Used() != 0 || p.packetMode != 2 {
		t.Fatal("packet mode reserved idle ring storage", s.Stats())
	}
	s.service(now.Add(time.Millisecond), discardPacket)
	if s.packetBudget.Used() != reliable.PacketStateCharge || s.stats.PeakPacketStateBytes != reliable.PacketStateCharge {
		t.Fatal("first outbound DATA did not charge/record packet ring", s.Stats())
	}
	before := p.receiver.AcceptedBytes()
	sendClient(t, s, client, p, reliable.Frame{Type: reliable.TypeData, ID: 3, Total: 1, Payload: []byte{2}}, now.Add(2*time.Millisecond))
	if p.receiver.AcceptedBytes() != before {
		t.Fatal("mid-session downgrade accepted")
	}
	s.remove(p, false)
	if s.packetBudget.Used() != 0 || s.budget.Used() != 0 {
		t.Fatal("expiration leak")
	}
}
func TestPacketThousandMetadataBound(t *testing.T) {
	s, i, c := testMulti(t)
	now := time.Now()
	for n := 0; n < 1000; n++ {
		_, client, p := admit(t, s, i, uint16(2000+n), now)
		sendClient(t, s, client, p, reliable.Frame{Type: reliable.TypePacketData, ID: 1, Total: 32, Payload: []byte{1}, PacketNumber: 1}, now)
	}
	st := s.Stats()
	if st.PacketStateBytes != 0 || st.PacketStateBytes > c.PacketStateBudget {
		t.Fatal("idle packet ledgers retained ring storage", st)
	}
	s.service(now.Add(2*time.Minute), discardPacket)
	for len(s.sessions) > 0 {
		s.service(now.Add(2*time.Minute), discardPacket)
	}
	if s.packetBudget.Used() != 0 || s.budget.Used() != 0 {
		t.Fatal(s.Stats())
	}
	t.Logf("1000 packet-capable sessions: idle ring charge=%d; per-active-direction cap=%d", st.PacketStateBytes, reliable.PacketStateCharge)
}

func TestPacketAckBatchAndExplicitStage19Compatibility(t *testing.T) {
	for _, legacyAck := range []bool{false, true} {
		name := "batched-acks"
		if legacyAck {
			name = "stage19-one-packet-acks"
		}
		t.Run(name, func(t *testing.T) {
			_, i, c := testMulti(t)
			c.Options = Options{LegacyPacketAcks: legacyAck, Timeout: 10 * time.Second}
			s, addr, stop := startMulti(t, c, i)
			data := [][]byte{bytes.Repeat([]byte{1}, 4096), bytes.Repeat([]byte{2}, 12000)}
			err := ExchangeBatch(context.Background(), addr, i.Public, i.PSK, data, Options{
				LegacyPacketAcks: legacyAck, BatchPacketAcks: !legacyAck, Timeout: 10 * time.Second})
			stats := s.Stats()
			stop()
			if err != nil {
				t.Fatal(err)
			}
			if stats.PacketReceipts == 0 || stats.PacketReceiptFrames == 0 {
				t.Fatal("missing exact packet receipts", stats)
			}
			if legacyAck {
				if stats.PacketBatchReceiptFrames != 0 || stats.PacketReceiptFrames != stats.PacketReceipts {
					t.Fatal("legacy ACK mode emitted batches", stats)
				}
			} else {
				if stats.PacketBatchReceiptFrames == 0 || stats.PacketReceiptFrames >= stats.PacketReceipts {
					t.Fatal("ACK batching did not reduce receipt frames", stats)
				}
			}
		})
	}
}
