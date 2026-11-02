package testkit

import "testing"

func TestAuditGeneratorBounds(t *testing.T) {
	g := NewGenerator(1)
	if _, err := g.Next(0, int(^uint(0)>>1)); err == nil {
		t.Fatal("unbounded allocation/overflow")
	}
	g.id = ^uint64(0)
	if _, err := g.Next(1, 1); err == nil {
		t.Fatal("ID reuse")
	}
}
