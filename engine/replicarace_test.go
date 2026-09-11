package engine_test

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/go-faster/errors"
	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/backend"
	"github.com/oteldb/storage/engine"
	"github.com/oteldb/storage/query/fetch"
)

// TestRefreshReplicaLeavesPublishedPartsAlone: a refresh reuses the handles readers already hold, and
// MergeShape, Count and PartsDetailed read those handles after releasing the engine lock, so a refresh
// that wrote to one would race them. Only -race can fail it.
func TestRefreshReplicaLeavesPublishedPartsAlone(t *testing.T) {
	t.Parallel()

	const refreshes = 200

	ctx := context.Background()
	be := backend.Memory()
	seedBulkyParts(t, be)

	replica := engine.New(engine.Config{Backend: be, Prefix: replicaPrefix})
	require.NoError(t, replica.RefreshReplica(ctx))
	require.Equal(t, 2, replica.PartCount())

	r := fetch.Request{Start: 0, End: 1 << 60, Matchers: []fetch.Matcher{eqMatcher("job", "j0")}}

	readers := []func() error{
		func() error {
			if n := replica.MergeShape().Parts; n != 2 {
				return errors.Errorf("merge shape saw %d parts", n)
			}

			return nil
		},
		func() error {
			n, err := replica.Count(ctx, r)
			if err != nil {
				return err
			}

			if n != 1 {
				return errors.Errorf("counted %d series", n)
			}

			return nil
		},
		func() error {
			parts, err := replica.PartsDetailed(ctx)
			if err != nil {
				return err
			}

			if len(parts) != 2 {
				return errors.Errorf("detailed %d parts", len(parts))
			}

			return nil
		},
	}

	runAgainstRefreshes(t, readers, refreshes, func() error { return replica.RefreshReplica(ctx) })
}

// runAgainstRefreshes runs every reader in a loop of its own, from before the first refresh until
// after the last, and fails on the first error any of them returned.
func runAgainstRefreshes(t *testing.T, readers []func() error, refreshes int, refresh func() error) {
	t.Helper()

	var (
		stop       atomic.Bool
		ready, all sync.WaitGroup
	)

	errs := make(chan error, len(readers))

	ready.Add(len(readers))

	for _, read := range readers {
		all.Go(func() {
			err := read()
			ready.Done()

			for err == nil && !stop.Load() {
				err = read()
			}

			errs <- err
		})
	}

	defer func() {
		stop.Store(true)
		all.Wait()
	}()

	ready.Wait()

	for range refreshes {
		require.NoError(t, refresh())
	}

	stop.Store(true)
	all.Wait()
	close(errs)

	for err := range errs {
		require.NoError(t, err)
	}
}
