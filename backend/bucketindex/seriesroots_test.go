package bucketindex_test

import (
	"encoding/binary"
	"slices"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/backend"
	"github.com/oteldb/storage/backend/bucketindex"
)

func seriesRoot(prefix string, digest byte) bucketindex.SeriesRoot {
	return bucketindex.SeriesRoot{Prefix: prefix, ManifestDigest: [32]byte{digest}}
}

func TestSeriesRootsRoundTrip(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name  string
		roots []bucketindex.SeriesRoot
	}{
		{name: "empty"},
		{name: "one", roots: []bucketindex.SeriesRoot{seriesRoot("tenant/metrics/series/a", 1)}},
		{name: "several", roots: []bucketindex.SeriesRoot{seriesRoot("a", 1), seriesRoot("b", 2)}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			in := fullIndex()
			legacy := in.AppendBinary(nil)
			in.SeriesRoots = tc.roots
			raw := in.AppendBinary(nil)
			wantVersion := byte(8)
			if len(tc.roots) > 0 {
				wantVersion = 9
			}
			require.Equal(t, wantVersion, raw[2])
			out, err := bucketindex.Decode(raw)
			require.NoError(t, err)
			require.Equal(t, in, out)
			require.Equal(t, raw, out.AppendBinary(nil))
			legacy[2] = wantVersion
			require.Equal(t, legacy, raw[:len(legacy)], "v9 preserves every sample-catalog byte")
		})
	}
}

func TestSeriesRootsV9Encoding(t *testing.T) {
	t.Parallel()
	ix := &bucketindex.Index{SeriesRoots: []bucketindex.SeriesRoot{seriesRoot("a", 1)}}
	want := [49]byte{'B', 'I', 9, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 1, 'a', 1}
	require.Equal(t, want[:], ix.AppendBinary(nil))
}

func TestMergeSeriesRoots(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name   string
		groups [][]bucketindex.SeriesRoot
		want   []bucketindex.SeriesRoot
		bad    bool
	}{
		{name: "empty"},
		{
			name:   "union",
			groups: [][]bucketindex.SeriesRoot{{seriesRoot("c", 3), seriesRoot("a", 1)}, {seriesRoot("b", 2), seriesRoot("a", 1)}},
			want:   []bucketindex.SeriesRoot{seriesRoot("a", 1), seriesRoot("b", 2), seriesRoot("c", 3)},
		},
		{name: "conflict", groups: [][]bucketindex.SeriesRoot{{seriesRoot("a", 1)}, {seriesRoot("a", 2)}}, bad: true},
		{name: "missing digest", groups: [][]bucketindex.SeriesRoot{{seriesRoot("a", 0)}}, bad: true},
		{name: "empty prefix", groups: [][]bucketindex.SeriesRoot{{seriesRoot("", 1)}}, bad: true},
		{name: "traversal", groups: [][]bucketindex.SeriesRoot{{seriesRoot("a/../b", 1)}}, bad: true},
		{name: "absolute", groups: [][]bucketindex.SeriesRoot{{seriesRoot("/a", 1)}}, bad: true},
		{name: "empty segment", groups: [][]bucketindex.SeriesRoot{{seriesRoot("a//b", 1)}}, bad: true},
		{name: "backslash", groups: [][]bucketindex.SeriesRoot{{seriesRoot("a\\b", 1)}}, bad: true},
		{name: "nul", groups: [][]bucketindex.SeriesRoot{{seriesRoot("a\x00b", 1)}}, bad: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			before := make([][]bucketindex.SeriesRoot, len(tc.groups))
			for i := range tc.groups {
				before[i] = slices.Clone(tc.groups[i])
			}
			out, err := bucketindex.MergeSeriesRoots(tc.groups...)
			if tc.bad {
				require.ErrorIs(t, err, bucketindex.ErrCorrupt)
				require.Nil(t, out)
			} else {
				require.NoError(t, err)
				require.Equal(t, tc.want, out)
				if len(out) > 0 {
					out[0].Prefix = "changed"
				}
			}
			for i := range tc.groups {
				require.Equal(t, before[i], tc.groups[i])
			}
		})
	}
}

func TestSeriesRootsRejectMalformedEncoding(t *testing.T) {
	t.Parallel()
	base := (&bucketindex.Index{}).AppendBinary(nil)
	base[2] = 9
	encode := func(roots ...bucketindex.SeriesRoot) []byte {
		ix := &bucketindex.Index{SeriesRoots: roots}
		return ix.AppendBinary(nil)
	}
	valid := encode(seriesRoot("a", 1))
	for _, tc := range []struct {
		name string
		raw  []byte
	}{
		{name: "missing count", raw: base},
		{name: "excess count", raw: binary.AppendUvarint(slices.Clone(base), ^uint64(0))},
		{name: "empty prefix", raw: encode(seriesRoot("", 1))},
		{name: "missing digest", raw: encode(seriesRoot("a", 0))},
		{name: "traversal", raw: encode(seriesRoot("a/../b", 1))},
		{name: "duplicate", raw: encode(seriesRoot("a", 1), seriesRoot("a", 1))},
		{name: "conflicting digest", raw: encode(seriesRoot("a", 1), seriesRoot("a", 2))},
		{name: "unsorted", raw: encode(seriesRoot("b", 2), seriesRoot("a", 1))},
		{name: "trailing bytes", raw: append(slices.Clone(valid), 0)},
		{name: "oversized prefix", raw: append(append(slices.Clone(base), 1), binary.AppendUvarint(nil, ^uint64(0))...)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			_, err := bucketindex.Decode(tc.raw)
			require.ErrorIs(t, err, bucketindex.ErrCorrupt)
		})
	}
	for end := range valid {
		_, err := bucketindex.Decode(valid[:end])
		require.ErrorIs(t, err, bucketindex.ErrCorrupt, "truncated at %d", end)
	}
}

func TestSeriesRootsRebase(t *testing.T) {
	t.Parallel()
	be := backend.Memory()
	const key = "metrics/" + bucketindex.Object
	a, av, err := bucketindex.LoadVersioned(t.Context(), be, key)
	require.NoError(t, err)
	b, bv, err := bucketindex.LoadVersioned(t.Context(), be, key)
	require.NoError(t, err)
	require.NoError(t, a.AddSeriesRoot(seriesRoot("a", 1)))
	require.NoError(t, b.AddSeriesRoot(seriesRoot("b", 2)))
	_, err = a.Save(t.Context(), be, key, av)
	require.NoError(t, err)
	_, err = b.Save(t.Context(), be, key, bv)
	require.ErrorIs(t, err, bucketindex.ErrConflict)
	loaded, version, err := bucketindex.LoadVersioned(t.Context(), be, key)
	require.NoError(t, err)
	b.SeriesRoots, err = bucketindex.MergeSeriesRoots(loaded.SeriesRoots, b.SeriesRoots)
	require.NoError(t, err)
	b.Add(bucketindex.Entry{Prefix: "sample"})
	_, err = b.Save(t.Context(), be, key, version)
	require.NoError(t, err)
	final, err := bucketindex.Load(t.Context(), be, key)
	require.NoError(t, err)
	require.Equal(t, []bucketindex.SeriesRoot{seriesRoot("a", 1), seriesRoot("b", 2)}, final.SeriesRoots)
	require.Len(t, final.Entries, 1)
	before := final.AppendBinary(nil)
	require.ErrorIs(t, final.AddSeriesRoot(seriesRoot("a", 3)), bucketindex.ErrCorrupt)
	require.Equal(t, before, final.AppendBinary(nil), "failed adoption preserves the committed bindings")
}

func TestSeriesRootsSaveRejectsInvalidBindings(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name  string
		roots []bucketindex.SeriesRoot
	}{
		{name: "conflict", roots: []bucketindex.SeriesRoot{seriesRoot("a", 1), seriesRoot("a", 2)}},
		{name: "unsorted", roots: []bucketindex.SeriesRoot{seriesRoot("b", 1), seriesRoot("a", 2)}},
		{name: "unbound", roots: []bucketindex.SeriesRoot{seriesRoot("a", 0)}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			be := backend.Memory()
			ix := &bucketindex.Index{SeriesRoots: tc.roots}
			_, err := ix.Save(t.Context(), be, "index", backend.VersionAbsent)
			require.ErrorIs(t, err, bucketindex.ErrCorrupt)
			_, err = be.Read(t.Context(), "index")
			require.ErrorIs(t, err, backend.ErrNotExist)
		})
	}
}

func TestSeriesRootsAreIndependentOfSamples(t *testing.T) {
	t.Parallel()
	ix := &bucketindex.Index{SeriesRoots: []bucketindex.SeriesRoot{seriesRoot("root", 1)}}
	require.Empty(t, ix.Overlapping(-1, 1))
	require.Equal(t, bucketindex.Block{N: 1}, ix.NextBlock(0))
	want := bucketindex.Want{Prefix: "root"}
	_, found := ix.Satisfying(want)
	require.False(t, found)
	require.Equal(t, []bucketindex.Want{want}, bucketindex.TrimWants([]bucketindex.Want{want}, ix.Entries))
	ix.RecordLineage()
	require.Empty(t, ix.Catalog)
	require.Empty(t, ix.Holes())
}

func TestSeriesRetirementRoundTrip(t *testing.T) {
	t.Parallel()
	ix := &bucketindex.Index{SeriesRoots: []bucketindex.SeriesRoot{seriesRoot("merged", 3)}, RetiredSeriesRoots: []bucketindex.SeriesRoot{seriesRoot("a", 1), seriesRoot("b", 2)}}
	raw := ix.AppendBinary(nil)
	require.Equal(t, byte(10), raw[2])
	decoded, err := bucketindex.Decode(raw)
	require.NoError(t, err)
	require.Equal(t, ix, decoded)
	require.Equal(t, raw, decoded.AppendBinary(nil))
	legacy := slices.Clone(raw)
	legacy[2] = 9
	_, err = bucketindex.Decode(legacy)
	require.ErrorIs(t, err, bucketindex.ErrCorrupt, "v9 must not silently ignore retirement records")
	for n := 15; n < len(raw); n++ {
		_, err := bucketindex.Decode(raw[:n])
		require.ErrorIs(t, err, bucketindex.ErrCorrupt)
	}
}

func TestSeriesRetirementRebase(t *testing.T) {
	t.Parallel()
	a, b, c := seriesRoot("a", 1), seriesRoot("b", 2), seriesRoot("c", 3)
	active, retired, err := bucketindex.MergeSeriesCatalog([]bucketindex.SeriesRoot{a, b}, nil, []bucketindex.SeriesRoot{c}, []bucketindex.SeriesRoot{a, b})
	require.NoError(t, err)
	require.Equal(t, []bucketindex.SeriesRoot{c}, active)
	require.Equal(t, []bucketindex.SeriesRoot{a, b}, retired)
	active, retired, err = bucketindex.MergeSeriesCatalog(active, retired, []bucketindex.SeriesRoot{a, b}, nil)
	require.NoError(t, err)
	require.Equal(t, []bucketindex.SeriesRoot{c}, active)
	require.Len(t, retired, 2)
	_, _, err = bucketindex.MergeSeriesCatalog(nil, retired, []bucketindex.SeriesRoot{seriesRoot("a", 9)}, nil)
	require.ErrorIs(t, err, bucketindex.ErrCorrupt)
	invalid := &bucketindex.Index{SeriesRoots: []bucketindex.SeriesRoot{a}, RetiredSeriesRoots: []bucketindex.SeriesRoot{a}}
	_, err = invalid.Save(t.Context(), backend.Memory(), "index", backend.VersionAbsent)
	require.ErrorIs(t, err, bucketindex.ErrCorrupt)
	_, err = bucketindex.Decode(invalid.AppendBinary(nil))
	require.ErrorIs(t, err, bucketindex.ErrCorrupt)
}
