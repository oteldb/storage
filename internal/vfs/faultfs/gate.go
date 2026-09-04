package faultfs

import (
	"testing"
	"time"
)

// gateTimeout bounds how long [Gate.Await] waits for an operation to arrive. It only ever fires on
// a broken test — the operation the gate names never happened — so it is generous.
const gateTimeout = 30 * time.Second

// Gate suspends the filesystem operations matching it until the test releases them, so a test can
// state an interleaving — a crash *during* a publish, an append landing between a rename and its
// directory sync — instead of racing for it. The counterpart to faultbackend's gate, one layer down.
type Gate struct {
	arrived chan Call
	release chan struct{}
}

// NewGate returns a gate with no operation suspended.
func NewGate() *Gate {
	return &Gate{arrived: make(chan Call, 1), release: make(chan struct{})}
}

// Rule returns the rule that suspends op's matching operations at this gate.
func (g *Gate) Rule(op Op, match func(Call) bool) Rule {
	return Rule{Op: op, Match: match, Before: g.hold}
}

// Await blocks until a matching operation reaches the gate, returning it. The operation stays
// suspended until [Gate.Release].
func (g *Gate) Await(tb testing.TB) Call {
	tb.Helper()

	select {
	case c := <-g.arrived:
		return c
	case <-time.After(gateTimeout):
		tb.Fatal("faultfs: no operation reached the gate")

		return Call{}
	}
}

// Release resumes every operation held at the gate, and any that arrive later.
func (g *Gate) Release() { close(g.release) }

func (g *Gate) hold(c Call) {
	select {
	case g.arrived <- c:
	default: // the test is already awake; later arrivals just wait for the release
	}

	<-g.release
}
