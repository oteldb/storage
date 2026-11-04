package retention

import (
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestStateRequestScope(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name                    string
		cutoff                  int64
		bound                   Bound
		storedTerm, currentTerm uint64
		want                    Request
	}{
		{"empty", 0, BoundAge, 0, 0, Request{}},
		{"negative disabled", -1, BoundAge, 7, 7, Request{Term: 7}},
		{"standalone", 100, BoundAge, 0, 0, Request{100, BoundAge, 0}},
		{"current tenure", 100, BoundSignalBytes, 7, 7, Request{100, BoundSignalBytes, 7}},
		{"different tenure", 100, BoundPooledBytes, 7, 8, Request{}},
		{"claim absent", 100, BoundPooledBytes, 7, 0, Request{}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			var s State
			s.Record(tc.cutoff, tc.bound, tc.storedTerm)
			require.Equal(t, tc.want, s.Load(tc.currentTerm))
			s.Record(tc.cutoff, tc.bound, tc.storedTerm)
			require.Equal(t, tc.want, s.Load(tc.currentTerm))
		})
	}
}

func TestStateSnapshotIsCoherent(t *testing.T) {
	t.Parallel()
	var s State
	var wg sync.WaitGroup
	for worker := range 4 {
		wg.Go(func() {
			for range 1000 {
				cutoff := int64(worker + 1)
				s.Record(cutoff, Bound(cutoff), 7)
				r := s.Load(7)
				if r.Cutoff != int64(r.Bound) {
					t.Error("mixed cutoff and bound")
				}
			}
		})
	}
	wg.Wait()
}

func TestBoundString(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		bound Bound
		name  string
	}{{BoundNone, "none"}, {BoundAge, "age"}, {BoundSignalBytes, "signal_bytes"}, {BoundPooledBytes, "pooled_bytes"}, {Bound(255), "none"}} {
		t.Run(tc.name, func(t *testing.T) { t.Parallel(); require.Equal(t, tc.name, tc.bound.String()) })
	}
}
