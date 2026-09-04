package faultfs

import (
	"hash/fnv"
	"math/rand/v2"
)

// crashRand draws the survival decisions of a [FS.CrashWith] from one root seed.
//
// Every stream is keyed by (seed, domain, path) rather than taken from a single sequential
// generator, because the clone walks Go maps: with one stream the run-to-run map order would decide
// which draw a file got, and a seed would not reproduce anything. Keying by path also keeps a file's
// outcome fixed when an unrelated file is added to the test, so a reproducer stays a reproducer.
//
// Pebble's errorfs keys its per-file generators the same way for a different reason — concurrent
// access to other files. That reason does not apply here: the clone runs once under the
// filesystem's lock. Determinism alone is what makes the keying worth it.
type crashRand struct {
	seed uint64
	pct  int
}

// crash domains keep the entry decision for a path independent of its byte decisions.
const (
	domainEntry = "entry"
	domainData  = "data"
)

func (c crashRand) stream(domain, name string) *rand.Rand {
	h := fnv.New64a()
	_, _ = h.Write([]byte(domain))
	_, _ = h.Write([]byte{0})
	_, _ = h.Write([]byte(name))

	return rand.New(rand.NewPCG(c.seed, h.Sum64())) //nolint:gosec // reproducibility is the point
}

// survives reports whether one unsynced unit — a directory entry, a data block — reached the platter.
func (c crashRand) survives(r *rand.Rand) bool {
	return r.IntN(100) < c.pct
}

// entrySurvives reports whether the unsynced name change to name landed.
func (c crashRand) entrySurvives(name string) bool {
	return c.survives(c.stream(domainEntry, name))
}
