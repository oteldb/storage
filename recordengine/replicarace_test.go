package recordengine_test

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/go-faster/errors"
	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/backend"
	"github.com/oteldb/storage/query/fetch"
)

// TestRefreshReplicaLeavesPublishedPartsAlone: a refresh reuses the handles readers already hold, and
// MergeShape, a top-N fetch and PartsDetailed read those handles after releasing the engine lock, so a
// refresh that wrote to one would race them. Only -race can fail it.
func TestRefreshReplicaLeavesPublishedPartsAlone(t *testing.T) {
	t.Parallel()

	const refreshes = 200

	ctx := context.Background()
	be := backend.Memory()
	seedBulkyParts(t, be)

	replica := newEngine(t, be)
	require.NoError(t, replica.RefreshReplica(ctx))
	require.Equal(t, 2, replica.PartCount())

	// A limit with no conditions takes the top-N scan, which orders the parts by their time bounds
	// off the lock.
	r := req("svc0")
	r.Limit = 1

	readers := []func() error{
		func() error {
			if n := replica.MergeShape().Parts; n != 2 {
				return errors.Errorf("merge shape saw %d parts", n)
			}

			return nil
		},
		func() error {
			it, err := replica.Fetch(ctx, r)
			if err != nil {
				return err
			}

			got, err := fetch.Drain(ctx, it)
			if err != nil {
				return err
			}

			if len(got) == 0 {
				return errors.New("top-N fetch returned nothing")
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
