package storage

import (
	"context"
	"fmt"
	"math"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/recordengine"
	"github.com/oteldb/storage/signal/profile"
)

// fullStoreResolver is the whole-store resolver: the head, an in-flight flush and every part's
// sidecars unioned into one set of tables and decoded.
func fullStoreResolver(t *testing.T, s *Storage) *profile.Resolver {
	t.Helper()

	eng := mustEngine(s.profileEngineFor("default"))

	var tables []map[string][]byte

	rd := eng.ReadSide(0, 0, func(live recordengine.SideStore) { tables = append(tables, live.Encode()) })
	defer rd.Release()

	if rd.Flushing != nil {
		tables = append(tables, rd.Flushing)
	}

	for _, p := range rd.Parts {
		m, err := p.Load(context.Background())
		require.NoError(t, err)

		tables = append(tables, m)
	}

	union, err := profile.NewSymbolStore().Union(tables)
	require.NoError(t, err)

	r, err := profile.NewResolver(union)
	require.NoError(t, err)

	return r
}

// TestProfileResolverWindow checks a windowed resolver resolves every stack the samples in its
// window reference, exactly as the whole-store resolver does, and that a window covering the store
// answers every id as the whole-store resolver does.
func TestProfileResolverWindow(t *testing.T) {
	t.Parallel()

	const day = int64(24 * time.Hour)

	for _, cache := range []int64{0, -1} {
		t.Run(fmt.Sprintf("cache=%d", cache), func(t *testing.T) {
			t.Parallel()
			ctx := context.Background()

			s, err := InMemory(WithProfileSymbolCache(cache))
			require.NoError(t, err)
			t.Cleanup(func() { _ = s.Close(ctx) })

			w := symbolWorkload{functions: 30, locations: 80, stacks: 40, depth: 6}
			eng := mustEngine(s.profileEngineFor("default"))

			// Two scrapes a day for three days, each flushed to its own part; a merge then folds
			// each day's parts together. The last scrape stays in the head.
			for d := range int64(3) {
				for k := range int64(2) {
					_, err := s.WriteProfiles(ctx, w.batch(fmt.Sprintf("svc-%d", k), (d+1)*day+k, uint64(2*d+k)))
					require.NoError(t, err)
					require.NoError(t, eng.Flush(ctx))
				}
			}

			require.NoError(t, eng.Merge(ctx, 0))

			_, err = s.WriteProfiles(ctx, w.batch("svc-head", 4*day, 99))
			require.NoError(t, err)

			full := fullStoreResolver(t, s)

			for _, win := range [][2]int64{
				{day, day}, {day + 1, 2 * day}, {2 * day, 3*day + 1}, {3*day + 1, 3*day + 1},
				{4 * day, 4 * day}, {5 * day, 6 * day},
			} {
				stacks := profileStackIDs(t, s, win[0], win[1])

				r, err := s.ProfileResolver(ctx, "default", win[0], win[1])
				require.NoError(t, err)

				for _, id := range stacks {
					frames := r.Resolve(id)
					require.NotEmpty(t, frames, "window %v stack %x", win, id)
					require.Equal(t, full.Resolve(id), frames, "window %v stack %x", win, id)
				}
			}

			dayOne, err := s.ProfileResolver(ctx, "default", day, day+1)
			require.NoError(t, err)

			unresolved := 0

			for _, id := range profileStackIDs(t, s, 3*day, 3*day+1) {
				if len(dayOne.Resolve(id)) == 0 {
					unresolved++
				}
			}

			assert.Positive(t, unresolved, "a window reads only the parts it overlaps")

			all := append(profileStackIDs(t, s, math.MinInt64, math.MaxInt64), nil, []byte("short"), make([]byte, 16))
			require.Greater(t, len(all), 3*w.stacks)

			for _, win := range [][2]int64{{0, 0}, {day, 4 * day}} {
				r, err := s.ProfileResolver(ctx, "default", win[0], win[1])
				require.NoError(t, err)

				for _, id := range all {
					require.Equal(t, full.Resolve(id), r.Resolve(id), "window %v stack %x", win, id)
				}
			}

			caches := s.Inspect().Caches.ProfileSymbols
			if cache < 0 {
				assert.Zero(t, caches)
			} else {
				assert.Positive(t, caches.Hits, "repeated windows reuse decoded parts")
				assert.Equal(t, eng.PartCount(), caches.Items)
			}
		})
	}
}

func TestProfileResolverUnknownTenant(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	s, err := InMemory()
	require.NoError(t, err)
	t.Cleanup(func() { _ = s.Close(ctx) })

	r, err := s.ProfileResolver(ctx, "nobody", 0, 0)
	require.NoError(t, err)
	assert.Empty(t, r.Resolve(make([]byte, 16)))

	require.NoError(t, s.Close(ctx))

	_, err = s.ProfileResolver(ctx, "default", 0, 0)
	require.ErrorIs(t, err, ErrClosed)
}
