package retention

import (
	"sync"
	"sync/atomic"
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

func TestStateStaleTenureCannotReplaceNewerRequest(t *testing.T) {
	t.Parallel()
	var s State
	var current atomic.Uint64
	current.Store(7)
	captured := make(chan struct{})
	resume := make(chan struct{})
	done := make(chan struct{})
	go func() {
		defer close(done)
		term := current.Load()
		close(captured)
		<-resume
		s.Record(100, BoundAge, term)
	}()
	<-captured
	current.Store(8)
	s.Record(200, BoundSignalBytes, current.Load())
	close(resume)
	<-done
	require.Equal(t, Request{Cutoff: 200, Bound: BoundSignalBytes, Term: 8}, s.Load(8))
	s.Record(50, BoundAge, 8)
	require.Equal(t, Request{Cutoff: 50, Bound: BoundAge, Term: 8}, s.Load(8), "the same owner may lengthen retention")
}
