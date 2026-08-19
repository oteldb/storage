package faultbackend

import (
	"testing"
	"time"
)

// gateTimeout bounds how long [Gate.Await] waits for an operation to arrive. It only ever fires on
// a broken test — a correct one is released promptly — so it is generous enough for a loaded CI
// machine.
const gateTimeout = 30 * time.Second

// Gate suspends the backend operations matching it until the test releases them, so a test can
// state an interleaving instead of racing for it: arrange for the operation to arrive, drive the
// other goroutine to the point of interest, then release.
//
// A gated operation blocks the goroutine that issued it inside the backend, so the code under test
// needs no hooks of its own.
type Gate struct {
	arrived chan Op
	release chan struct{}
}

// NewGate returns a gate with no operation suspended.
func NewGate() *Gate {
	return &Gate{arrived: make(chan Op, 1), release: make(chan struct{})}
}

// Rule returns a [Rule] that suspends the first operation of kind that match accepts. A nil match
// takes the first operation of that kind.
func (g *Gate) Rule(kind Kind, match func(Op) bool) Rule {
	return Rule{Kind: kind, Match: match, Before: g.hold, Times: 1}
}

// Await blocks until an operation is suspended at the gate and returns it, failing the test if
// none arrives.
func (g *Gate) Await(tb testing.TB) Op {
	tb.Helper()

	select {
	case op := <-g.arrived:
		return op
	case <-time.After(gateTimeout):
		tb.Fatal("no operation reached the gate")

		return Op{}
	}
}

// Release lets every operation held at the gate, and every later one matching it, proceed. It is
// safe to call once; a second call panics.
func (g *Gate) Release() { close(g.release) }

func (g *Gate) hold(op Op) {
	select {
	case g.arrived <- op:
	default:
	}
	<-g.release
}
