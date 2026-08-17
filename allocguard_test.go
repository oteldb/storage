package storage

import (
	"os"
	"testing"
)

// allocGuardEnv gates the tests that measure an operation's allocation volume as a
// runtime.MemStats delta around a single call.
const allocGuardEnv = "OTELDB_ALLOC_GUARDS"

// requireCleanProcess skips unless this process was started to measure allocation.
//
// runtime.MemStats.TotalAlloc is process-global, so a delta taken around one operation also counts
// whatever every other live goroutine allocated in that window — and `go test ./...` leaves plenty
// running: the cluster tests each own an embedded etcd, which allocates heavily through startup and
// teardown. Marking such a test non-parallel does not help; that only stops *new* parallel tests
// from starting, not goroutines already in flight.
//
// Measured on TestRecordMergeBoundedWorkingSet: run alone the per-round series is 35–40 MiB and
// tight; run with the rest of the package it spreads to 31–170 MiB, varying 5x between rounds at an
// identical part count. The test's own bound (a 1.5x half-over-half median ratio) then lands
// wherever the neighbours' allocations happen to fall — 1.30 in one local run, 1.61 in the CI run
// that failed on main. Widening the bound to cover that spread would give up the sensitivity it
// exists for, so the measurement gets its own process instead: the `alloc` job in
// .github/workflows/isolated.yml.
func requireCleanProcess(t *testing.T) {
	t.Helper()

	if os.Getenv(allocGuardEnv) == "" {
		t.Skipf("allocation measurement needs a process to itself; set %s=1 to run", allocGuardEnv)
	}
}
