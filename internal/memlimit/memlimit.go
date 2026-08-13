// Package memlimit reports the memory budget the process actually has, so a size derived from it
// (the merge cap) tracks the container it runs in rather than a constant that is right at exactly
// one deployment size.
//
// It is deliberately dependency-free: the answer comes from GOMEMLIMIT, the cgroup, or the host's
// total memory, in that order of authority.
package memlimit

import (
	"math"
	"runtime/debug"
	"sync"
)

// Bytes returns the memory the process may use, or 0 when nothing reports one.
//
// GOMEMLIMIT wins when set: it is what the embedder promised the Go heap, and an embedder that
// applies a cgroup limit to it (automemlimit and friends) has already reserved its own headroom.
// Otherwise the cgroup limit is used, since a container's limit — not the host's RAM — is what the
// kernel kills against, and the host total is the last resort.
func Bytes() int64 {
	if limit := debug.SetMemoryLimit(-1); limit > 0 && limit != math.MaxInt64 {
		return limit
	}

	return detected()
}

// detected is resolved once: GOMEMLIMIT can be changed at runtime and is re-read on every call, but
// the cgroup and host figures are fixed for the process's life and cost several /proc reads, which
// a per-merge caller should not repeat.
var detected = sync.OnceValue(func() int64 {
	if limit := cgroupBytes(); limit > 0 {
		return limit
	}

	return hostBytes()
})
