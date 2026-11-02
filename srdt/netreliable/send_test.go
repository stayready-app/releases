package netreliable

import (
	"errors"
	"example.com/srdt/reliable"
	"example.com/srdt/secureudp"
	"net"
	"testing"
	"time"
)

type sendTimeout struct{}

func (sendTimeout) Error() string   { return "simulated send deadline" }
func (sendTimeout) Timeout() bool   { return true }
func (sendTimeout) Temporary() bool { return true }

type failingConn struct{ err error }

func (c failingConn) WriteTo(b []byte, a net.Addr) (int, error) { return 0, c.err }
func (c failingConn) ReadFrom([]byte) (int, net.Addr, error)    { return 0, nil, errors.New("unused") }
func (c failingConn) LocalAddr() net.Addr                       { return &net.UDPAddr{} }
func (c failingConn) Close() error                              { return nil }
func (c failingConn) SetDeadline(time.Time) error               { return nil }
func (c failingConn) SetReadDeadline(time.Time) error           { return nil }
func (c failingConn) SetWriteDeadline(time.Time) error          { return nil }

func TestWindowTransientSendErrorRetainsUnacknowledgedData(t *testing.T) {
	i, _ := secureudp.NewIdentity(make([]byte, 32))
	_, session, _ := secureudp.ClientHello(i.Public, i.PSK)
	q, _ := reliable.NewQueue(4096, 4096)
	_ = q.Enqueue(1, make([]byte, 1000), time.Millisecond)
	now := time.Now()
	f, _ := q.Next(now, 900)
	if err := (Options{}).send(failingConn{sendTimeout{}}, &net.UDPAddr{}, session, f); err != nil {
		t.Fatal(err)
	}
	if q.InFlight() != 900 || q.Pending() != 1 {
		t.Fatal("write timeout confirmed/lost payload ownership")
	}
	retry, ok := q.NextSized(now.Add(time.Millisecond), 900, 350)
	if !ok || retry.Offset != 0 || len(retry.Payload) != 350 {
		t.Fatal("cannot repack timed-out write")
	}
	permanent := errors.New("permanent local error")
	if err := (Options{}).send(failingConn{permanent}, &net.UDPAddr{}, session, retry); !errors.Is(err, permanent) {
		t.Fatal("unexpected permanent error suppression", err)
	}
	q.Close()
}

func TestWindowFiniteCompletionNeedsSingleSession(t *testing.T) {
	_, i, c := testMulti(t)
	c.CompletionMessages = 1
	if _, err := NewServer(i, c); err == nil {
		t.Fatal("ambiguous global finite completion with multiple sessions")
	}
	c.MaxSessions = 1
	if _, err := NewServer(i, c); err != nil {
		t.Fatal(err)
	}
}
