package engine

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/backend/bucketindex"
)

func TestHasWantsWhileIngestionOwnsState(t *testing.T) {
	t.Parallel()
	for _, tt := range []struct {
		name         string
		wants        bool
		pending      bool
		adopted      bool
		holes        bool
		pendingHoles bool
		want         bool
	}{
		{name: "empty"},
		{name: "committed", wants: true, want: true},
		{name: "pending", pending: true, want: true},
		{name: "adopted", adopted: true, want: true},
		{name: "all sources", wants: true, pending: true, adopted: true, want: true},
		{name: "acknowledged hole", holes: true},
		{name: "pending hole does not discharge want", pending: true, pendingHoles: true, want: true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			e := &Engine{}
			want := bucketindex.Want{Prefix: "missing"}
			if tt.wants {
				e.wants = []bucketindex.Want{want}
			}
			if tt.pending {
				e.pendingWants = []bucketindex.Want{want}
			}
			if tt.adopted {
				e.adoptedWants = []bucketindex.Want{want}
			}
			if tt.holes {
				e.holes = []bucketindex.Entry{{Prefix: "acknowledged"}}
			}
			if tt.pendingHoles {
				e.pendingHoles = []bucketindex.Want{want}
			}

			e.mu.LockWriter()
			defer e.mu.UnlockWriter()
			require.Equal(t, tt.want, e.HasWants(), "repair presence must not join the ingestion state lock")
		})
	}
}

func TestHasWantsBusyCatalogUsesStateRead(t *testing.T) {
	t.Parallel()
	for _, wanted := range []bool{false, true} {
		t.Run(map[bool]string{false: "empty", true: "adopted"}[wanted], func(t *testing.T) {
			t.Parallel()
			e := &Engine{}
			if wanted {
				e.adoptedWants = []bucketindex.Want{{Prefix: "missing"}}
			}
			require.True(t, e.mu.TryLockCatalog())
			defer e.mu.UnlockCatalog()

			require.Equal(t, wanted, e.HasWants(), "busy catalog must fall back instead of waiting for catalog ownership")
			require.False(t, e.mu.TryLockCatalog(), "the failed probe must not release another owner's catalog lock")
			require.True(t, e.mu.TryLockWriter(), "the fallback must release its state read lock")
			e.mu.UnlockWriter()
		})
	}
}

func TestHasWantsAfterAdoptAcknowledgement(t *testing.T) {
	t.Parallel()
	e := &Engine{}
	adopted := make(chan struct{})
	go func() {
		e.AdoptWants([]bucketindex.Want{{Prefix: "peer/missing"}})
		close(adopted)
	}()
	<-adopted

	e.mu.LockWriter()
	defer e.mu.UnlockWriter()
	for range 8 {
		require.True(t, e.HasWants(), "acknowledged adoption cannot be hidden by a stale negative")
	}
}

func TestHasWantsDuringCatalogIO(t *testing.T) {
	t.Parallel()
	for _, tt := range []struct {
		name string
		io   func(*Engine, func() error) error
	}{
		{name: "readers", io: func(e *Engine, fn func() error) error { return e.mu.WithReaders(fn) }},
		{name: "writers", io: func(e *Engine, fn func() error) error { return e.mu.WithWriters(fn) }},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			e := &Engine{adoptedWants: []bucketindex.Want{{Prefix: "peer/missing"}}}
			e.mu.Lock()
			defer e.mu.Unlock()
			require.NoError(t, tt.io(e, func() error {
				result := make(chan bool, 1)
				go func() { result <- e.HasWants() }()
				require.True(t, <-result, "catalog I/O must still admit a reader through the state lock")
				return nil
			}))
		})
	}
}
