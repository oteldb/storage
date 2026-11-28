package backend

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"

	"github.com/stretchr/testify/require"
)

func TestPostingBitmapUsesObjectGeneration(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	b := CachedWithPostings(Memory(), 64<<20, 4<<20).(*cachedBackend)
	loads := uint64(0)
	chunks := 4
	read := func(name string) []uint64 {
		t.Helper()
		out, err := b.ReadPostingBitmap(ctx, "root", []byte(name), func() ([]uint64, int, error) {
			loads++
			return []uint64{loads}, chunks, nil
		})
		require.NoError(t, err)
		return out
	}
	require.Equal(t, []uint64{1}, read("exact"))
	require.Equal(t, []uint64{1}, read("exact"))
	require.NoError(t, b.Write(ctx, "root", []byte("replacement")))
	require.Equal(t, []uint64{2}, read("exact"))
	chunks = 1
	require.Equal(t, []uint64{3}, read("sparse"))
	require.Equal(t, []uint64{4}, read("sparse"))
	stats := b.PostingStats()
	require.EqualValues(t, 4, stats.Builds)
	require.EqualValues(t, 1, stats.Hits)
	require.EqualValues(t, 2, stats.Skipped)
	require.EqualValues(t, 10, stats.ChunksVisited)
	require.EqualValues(t, 4, stats.ChunksAvoided)
}

func TestPostingBitmapSharesConcurrentBuild(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		const readers = 8
		b := CachedWithPostings(Memory(), 64<<20, 4<<20).(*cachedBackend)
		var loads atomic.Int64
		var wg sync.WaitGroup
		done := make(chan struct{})
		results := make(chan []uint64, readers)
		for range readers {
			wg.Go(func() {
				v, err := b.ReadPostingBitmap(t.Context(), "root", []byte("exact"), func() ([]uint64, int, error) {
					loads.Add(1)
					<-done
					return []uint64{1}, 4, nil
				})
				if err != nil {
					t.Error(err)
				}
				results <- v
			})
		}
		synctest.Wait()
		require.EqualValues(t, 1, loads.Load())
		close(done)
		wg.Wait()
		for range readers {
			require.Equal(t, []uint64{1}, <-results)
		}
		require.EqualValues(t, readers-1, b.PostingStats().Joins)
	})
}

func TestPostingBitmapRetriesCanceledLeader(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		b := CachedWithPostings(Memory(), 64<<20, 4<<20).(*cachedBackend)
		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()
		leader := make(chan error, 1)
		go func() {
			_, err := b.ReadPostingBitmap(ctx, "root", []byte("exact"), func() ([]uint64, int, error) {
				<-ctx.Done()
				return nil, 0, ctx.Err()
			})
			leader <- err
		}()
		synctest.Wait()
		result := make(chan []uint64, 1)
		go func() {
			v, err := b.ReadPostingBitmap(t.Context(), "root", []byte("exact"), func() ([]uint64, int, error) {
				return []uint64{2}, 4, nil
			})
			if err != nil {
				t.Error(err)
			}
			result <- v
		}()
		synctest.Wait()
		cancel()
		require.ErrorIs(t, <-leader, context.Canceled)
		require.Equal(t, []uint64{2}, <-result)
	})
}
