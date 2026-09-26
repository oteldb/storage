package block

import "sync/atomic"

// DictGen identifies one decoded dictionary table, so a [Binding] can tell the table it was bound to
// from any other. Tokens are comparable and never reused within a process; the zero value is invalid.
type DictGen struct{ n uint64 }

var dictGens atomic.Uint64

// NewDictGen mints a fresh token, for a caller that builds its own table.
func NewDictGen() DictGen { return DictGen{n: dictGens.Add(1)} }

// IsZero reports whether g is the zero token.
func (g DictGen) IsZero() bool { return g.n == 0 }
