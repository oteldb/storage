package enginetest

import (
	"context"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/backend"
	"github.com/oteldb/storage/backend/bucketindex"
	"github.com/oteldb/storage/backend/faultbackend"
	"github.com/oteldb/storage/block"
	"github.com/oteldb/storage/internal/obs"
	"github.com/oteldb/storage/internal/obs/obstest"
)

// corruptLoads is how many consecutive loads must fail on one corrupt part before the next records it
// as a want.
const corruptLoads = 3

func (s staleLoad) manifest(t *testing.T, k Kind, prefix string) string {
	t.Helper()

	return k.manifestOf(t, s.be, strings.TrimPrefix(prefix, k.Prefix+"/"))
}

func readObject(ctx context.Context, t *testing.T, be backend.Backend, key string) []byte {
	t.Helper()

	data, err := backend.ReadView(ctx, be, key)
	require.NoError(t, err)

	return append([]byte(nil), data...)
}

// corruptPartBecomesWant: a part that stays corrupt over [corruptLoads] loads stops fencing the
// engine. The next load records it as a want, the flush the fence held back commits that want, and
// repair takes it from there: a peer's copy, or a hole once no owner has one.
func corruptPartBecomesWant(t *testing.T, k Kind) {
	t.Helper()

	for _, tc := range []struct {
		name   string
		answer func(t *testing.T, peer, be backend.Backend) Answer
		merges int
		check  func(t *testing.T, s staleLoad)
	}{
		{
			name: "repaired from a peer",
			answer: func(t *testing.T, peer, be backend.Backend) Answer {
				t.Helper()

				return func(w bucketindex.Want) (bucketindex.Entry, bucketindex.WantOutcome, error) {
					CopyObjects(t, peer, be, w.Prefix)

					return w.Entry(), bucketindex.WantSatisfied, nil
				}
			},
			merges: 1,
			check: func(t *testing.T, s staleLoad) {
				t.Helper()

				assert.Empty(t, s.e.WantPrefixes())
				assert.Empty(t, s.e.Holes())
				assert.Equal(t, []Row{api(100, 1), api(200, 2), api(400, 4)}, rows(t, s.e, apiStream))
			},
		},
		{
			name: "acknowledged as a hole",
			answer: func(*testing.T, backend.Backend, backend.Backend) Answer {
				return func(bucketindex.Want) (bucketindex.Entry, bucketindex.WantOutcome, error) {
					return bucketindex.Entry{}, bucketindex.WantAbsent, nil
				}
			},
			merges: 3,
			check: func(t *testing.T, s staleLoad) {
				t.Helper()

				assert.Empty(t, s.e.WantPrefixes())
				assert.Equal(t, []string{s.b}, prefixes(s.e.Holes()))
				assert.EqualValues(t, 1, s.e.LostParts())
				assert.Equal(t, []Row{api(100, 1), api(400, 4)}, rows(t, s.e, apiStream))
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			ctx := context.Background()
			o, m := obstest.New(t)
			peer := backend.Memory()
			f := NewFetcher(nil)
			s := k.newStaleLoad(t, Config{Obs: o, Repair: f}, func(ctx context.Context, be *faultbackend.Backend, manifest string) {
				copyObjects(ctx, t, be, peer, strings.TrimSuffix(manifest, "/manifest"))
				corruptObject(t)(ctx, be, manifest)
			})
			f.SetAnswer(tc.answer(t, peer, s.be))

			require.Error(t, s.e.LoadParts(ctx))
			s.e.Append(t, api(400, 4))

			for range corruptLoads - 1 {
				require.ErrorIs(t, s.e.ReloadFenced(ctx), block.ErrCorrupt)
			}

			require.True(t, s.e.Stats().IndexFenced)
			require.Zero(t, s.e.Stats().WantedParts, "short of the bar the part is not yet a want")
			assert.EqualValues(t, corruptLoads, m.Counter("storage.index.fenced_loads", "reason", obs.FenceCorrupt))

			require.NoError(t, s.e.ReloadFenced(ctx))

			st := s.e.Stats()
			assert.False(t, st.IndexFenced)
			assert.Equal(t, 1, st.WantedParts)
			assert.EqualValues(t, 1, m.Counter("storage.corruption.detected",
				"component", "part", "disposition", obs.CorruptWanted))

			require.NoError(t, s.e.Flush(ctx), "the fence no longer holds the flush back")

			ix := k.loadIndex(t, s.be)
			assert.NotContains(t, prefixes(ix.Entries), s.b)
			assert.Contains(t, prefixes(ix.Entries), s.c)
			assert.Equal(t, []string{s.b}, wantPrefixes(ix.Wanted), "the loss is committed as a want, not a removal")
			assert.Equal(t, []string{s.b}, s.e.WantPrefixes())

			mergeTimes(t, s.e, tc.merges)
			tc.check(t, s)
		})
	}
}

// otherFailuresNeverExit: only damage earns the exit. A backend that fails to answer says nothing
// about the part, and a newer format is an intact part this binary cannot read, so both stay fenced
// however long they last.
func otherFailuresNeverExit(t *testing.T, k Kind) {
	t.Helper()

	for _, tc := range []struct {
		name   string
		breakB breakPart
		reason string
	}{
		{"backend error", func(_ context.Context, be *faultbackend.Backend, key string) {
			be.Add(faultbackend.Rule{
				Kind: faultbackend.Read, Match: func(op faultbackend.Op) bool { return op.Key == key },
				Err: errReadRejected,
			})
		}, obs.FenceUnavailable},
		{"unsupported version", func(ctx context.Context, be *faultbackend.Backend, key string) {
			require.NoError(t, be.Write(ctx, key, block.Manifest{Version: 1 << 20}.Encode(nil)))
		}, obs.FenceUnsupportedVersion},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			ctx := context.Background()
			o, m := obstest.New(t)
			s := k.newStaleLoad(t, Config{Obs: o}, tc.breakB)

			require.Error(t, s.e.LoadParts(ctx))

			const loads = 3 * corruptLoads
			for range loads {
				require.Error(t, s.e.ReloadFenced(ctx))
			}

			st := s.e.Stats()
			assert.True(t, st.IndexFenced)
			assert.Zero(t, st.WantedParts)

			ix := k.loadIndex(t, s.be)
			assert.Contains(t, prefixes(ix.Entries), s.b)
			assert.Empty(t, ix.Wanted)
			assert.EqualValues(t, 1+loads, m.Counter("storage.index.fenced_loads", "reason", tc.reason))
			assert.Zero(t, m.Counter("storage.corruption.detected", "disposition", obs.CorruptWanted))
		})
	}
}

// corruptRunResets: the loads must fail on the part consecutively. A load that fails for another
// reason, or on another part, starts the part's run over.
func corruptRunResets(t *testing.T, k Kind) {
	t.Helper()

	for _, tc := range []struct {
		name string
		// interrupt makes the next load fail without finding B corrupt, and undoes itself after.
		interrupt func(t *testing.T, s staleLoad, good []byte) (undo func())
	}{
		{"backend error", func(t *testing.T, s staleLoad, _ []byte) func() {
			t.Helper()
			failNextRead(context.Background(), s.be, s.manifest(t, k, s.b))

			return func() {}
		}},
		{"another part", func(t *testing.T, s staleLoad, good []byte) func() {
			t.Helper()

			ctx := context.Background()
			b, c := s.manifest(t, k, s.b), s.manifest(t, k, s.c)
			goodC := readObject(ctx, t, s.be, c)
			corrupt := readObject(ctx, t, s.be, b)

			require.NoError(t, s.be.Write(ctx, b, good))
			corruptObject(t)(ctx, s.be, c)

			return func() {
				require.NoError(t, s.be.Write(ctx, c, goodC))
				require.NoError(t, s.be.Write(ctx, b, corrupt))
			}
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			ctx := context.Background()

			var good []byte

			s := k.newStaleLoad(t, Config{}, func(ctx context.Context, be *faultbackend.Backend, manifest string) {
				good = readObject(ctx, t, be, manifest)
				corruptObject(t)(ctx, be, manifest)
			})

			require.Error(t, s.e.LoadParts(ctx))

			for range corruptLoads - 2 {
				require.Error(t, s.e.ReloadFenced(ctx))
			}

			undo := tc.interrupt(t, s, good)
			require.Error(t, s.e.ReloadFenced(ctx))
			undo()

			for i := range corruptLoads {
				require.Error(t, s.e.ReloadFenced(ctx), "load %d of the new run", i+1)
				require.Zero(t, s.e.Stats().WantedParts)
			}

			require.NoError(t, s.e.ReloadFenced(ctx))
			assert.Equal(t, 1, s.e.Stats().WantedParts)
		})
	}
}

// corruptPartsExitTogether: several corrupt parts each keep their own run, so neither resets the
// other's and all of them become wants on the same load.
func corruptPartsExitTogether(t *testing.T, k Kind) {
	t.Helper()

	ctx := context.Background()
	s := k.newStaleLoad(t, Config{}, corruptObject(t))
	corruptObject(t)(ctx, s.be, s.manifest(t, k, s.c))

	require.Error(t, s.e.LoadParts(ctx))

	for range corruptLoads - 1 {
		require.Error(t, s.e.ReloadFenced(ctx))
	}

	require.NoError(t, s.e.ReloadFenced(ctx))
	assert.Equal(t, 2, s.e.Stats().WantedParts)
}
