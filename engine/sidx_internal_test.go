package engine

import (
	"context"
	"encoding/hex"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/backend"
	"github.com/oteldb/storage/encoding/chunk"
	"github.com/oteldb/storage/signal"
)

// sidxColumn builds a sorted series column: distinct ascending ids, each repeated per runLens.
func sidxColumn(runLens []int) []chunk.U128 {
	var col []chunk.U128

	for k, n := range runLens {
		id := chunk.U128{Hi: uint64(k / 3), Lo: uint64(k * 17)}
		for range n {
			col = append(col, id)
		}
	}

	return col
}

// TestSeriesIndexRoundTrip pins encode∘parse == identity and the paged lookups against the
// resident index built from the same column — the equivalence contract of the two forms.
func TestSeriesIndexRoundTrip(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	for _, runLens := range [][]int{{}, {1}, {3}, {1, 1, 1}, {2, 5, 1, 8}, {4, 4, 4, 4, 4, 4, 4}} {
		col := sidxColumn(runLens)
		enc := encodeSeriesIndex(col)

		ents, n, total, err := parseSeriesIndex(enc, true)
		require.NoError(t, err)
		require.Equal(t, len(runLens), n)
		require.Equal(t, len(col), total)
		require.True(t, validSidxEntries(ents, n, total))

		// Serve the sidecar through a memory backend so the paged index is exercised end to end.
		be := backend.Memory()
		require.NoError(t, be.Write(ctx, sidxKey("p"), enc))

		paged, ok := openPagedIndex(ctx, be, "p", total)
		require.True(t, ok)

		pagedIdx := partIndex{paged: paged}
		resident := buildPartIndex(col)

		require.Equal(t, resident.seriesCount(), pagedIdx.seriesCount())
		require.Equal(t, resident.rows(), pagedIdx.rows())

		for _, id := range resident.ids {
			want, okR, err := resident.lookup(ctx, id)
			require.NoError(t, err)
			require.True(t, okR)

			got, okP, err := pagedIdx.lookup(ctx, id)
			require.NoError(t, err)
			require.True(t, okP)
			assert.Equal(t, want, got, "paged lookup must match resident")

			hasIt, err := pagedIdx.has(ctx, id)
			require.NoError(t, err)
			assert.True(t, hasIt)
		}

		absent := signal.SeriesID{Hi: ^uint64(0), Lo: ^uint64(0)}
		_, okP, err := pagedIdx.lookup(ctx, absent)
		require.NoError(t, err)
		assert.False(t, okP)

		// forEachID and intersectMark agree with the resident form.
		var pagedIDs []signal.SeriesID
		require.NoError(t, pagedIdx.forEachID(ctx, func(id signal.SeriesID) { pagedIDs = append(pagedIDs, id) }))
		assert.Equal(t, resident.ids, pagedIDs)

		if len(resident.ids) > 0 {
			probe := append([]signal.SeriesID{}, resident.ids...)
			probe = append(probe, absent) // absent sorts last (max id)

			activeP := make([]bool, len(probe))
			require.NoError(t, pagedIdx.intersectMark(ctx, probe, activeP))

			activeR := make([]bool, len(probe))
			require.NoError(t, resident.intersectMark(ctx, probe, activeR))
			assert.Equal(t, activeR, activeP)
			assert.False(t, activeP[len(probe)-1], "absent id stays unmarked")
		}
	}
}

// TestSeriesIndexGolden pins the sidecar bytes so accidental format drift is caught. The sidecar
// is derived (openPart falls back to the series column), so a deliberate format change may update
// this test — it exists to make the change deliberate.
func TestSeriesIndexGolden(t *testing.T) {
	t.Parallel()

	enc := encodeSeriesIndex(sidxColumn([]int{2, 1, 3}))

	const want = "4f545349" + // magic "OTSI"
		"01" + // version
		"03" + // 3 series
		"06" + // 6 rows
		"0000000000000000" + "0000000000000000" + "00000000" + // id {0,0} start 0
		"0000000000000000" + "0000000000000011" + "00000002" + // id {0,17} start 2
		"0000000000000000" + "0000000000000022" + "00000003" + // id {0,34} start 3
		"fcb87c5c" // crc32c

	assert.Equal(t, want, hex.EncodeToString(enc))
}

// TestSeriesIndexParseRejectsCorrupt walks the corruption cases parse and open must reject
// (falling back to the resident index) rather than mis-reading.
func TestSeriesIndexParseRejectsCorrupt(t *testing.T) {
	t.Parallel()

	enc := encodeSeriesIndex(sidxColumn([]int{2, 1, 3}))

	corrupt := func(mut func(b []byte)) []byte {
		c := append([]byte(nil), enc...)
		mut(c)

		return c
	}

	cases := map[string][]byte{
		"short":         enc[:6],
		"bad magic":     corrupt(func(b []byte) { b[0] ^= 0xFF }),
		"bad version":   corrupt(func(b []byte) { b[4] = 99 }),
		"flipped body":  corrupt(func(b []byte) { b[10] ^= 0xFF }),
		"flipped crc":   corrupt(func(b []byte) { b[len(b)-1] ^= 0xFF }),
		"truncated":     enc[:len(enc)-5],
		"trailing junk": append(append([]byte(nil), enc...), 0xAA),
	}

	for name, data := range cases {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			_, _, _, err := parseSeriesIndex(data, true)
			assert.Error(t, err)
		})
	}

	// A CRC-valid sidecar with broken invariants (unsorted ids / bad starts) is rejected by the
	// entry validation openPagedIndex runs.
	ents, n, total, err := parseSeriesIndex(enc, true)
	require.NoError(t, err)
	require.True(t, validSidxEntries(ents, n, total))

	bad := append([]byte(nil), ents...)
	copy(bad[0:], bad[sidxEntryW:2*sidxEntryW]) // duplicate entry 1 into slot 0: ids not ascending
	assert.False(t, validSidxEntries(bad, n, total))
}

// FuzzSeriesIndexParse asserts the sidecar parser never panics on arbitrary input and that every
// accepted input round-trips its header counts consistently with the entries region.
func FuzzSeriesIndexParse(f *testing.F) {
	f.Add(encodeSeriesIndex(sidxColumn([]int{2, 1, 3})))
	f.Add(encodeSeriesIndex(nil))
	f.Add([]byte("OTSI garbage"))

	f.Fuzz(func(t *testing.T, data []byte) {
		ents, n, total, err := parseSeriesIndex(data, true)
		if err != nil {
			return
		}

		if len(ents) != n*sidxEntryW {
			t.Fatalf("entries region %d bytes for %d entries", len(ents), n)
		}

		if validSidxEntries(ents, n, total) {
			for k := range n {
				_ = sidxEntryID(ents, k)
				if s := sidxEntryStart(ents, k); s < 0 || s >= total {
					t.Fatalf("validated start %d out of [0,%d)", s, total)
				}
			}
		}
	})
}

// hiddenViewer wraps a backend, hiding its [backend.Viewer] capability — the bare cold-tier shape.
type hiddenViewer struct{ backend.Backend }

// TestPagedIndexDropAndReload pins the residency lifecycle: with a Viewer backend the entries view
// drops on release (refs == 0) and reloads on the next use; without one the view is kept for the
// part's life (no re-read regression on a cache-less cold tier).
func TestPagedIndexDropAndReload(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	col := sidxColumn([]int{2, 5, 1})
	id := u128ToID(col[0])

	be := backend.Memory()
	require.NoError(t, be.Write(ctx, sidxKey("p"), encodeSeriesIndex(col)))

	paged, ok := openPagedIndex(ctx, be, "p", len(col))
	require.True(t, ok)
	require.False(t, paged.keep, "a Viewer backend allows dropping")
	require.NotNil(t, paged.view.Load(), "open retains the validated view for the first fetch")

	paged.drop()
	assert.Nil(t, paged.view.Load(), "drop releases the view")

	idx := partIndex{paged: paged}
	rng, found, err := idx.lookup(ctx, id)
	require.NoError(t, err)
	require.True(t, found)
	assert.Equal(t, rowRange{start: 0, end: 2}, rng)
	assert.NotNil(t, paged.view.Load(), "a lookup after drop reloads the view")

	// The part refcount wiring: the last release drops the paged view.
	p := &part{index: idx}
	p.acquire()
	p.acquire()
	p.release()
	assert.NotNil(t, paged.view.Load(), "still referenced: view stays")
	p.release()
	assert.Nil(t, paged.view.Load(), "last release drops the view")

	// A backend without Viewer keeps the view (loads once, stays resident).
	hidden := hiddenViewer{Backend: be}
	kept, ok := openPagedIndex(ctx, hidden, "p", len(col))
	require.True(t, ok)
	require.True(t, kept.keep)
	kept.drop()
	assert.NotNil(t, kept.view.Load(), "no cheap re-read path: the view is pinned")
}

// TestOpenPartUsesSidecarAndFallsBack covers openPart's two index paths end to end over a real
// flushed part: with the sidecar present the paged form is used (no resident ids); with it deleted
// or corrupted the resident fallback is built from the series column, and both agree.
func TestOpenPartUsesSidecarAndFallsBack(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	be := backend.Memory()
	e := New(Config{Backend: be, Prefix: "t/sidx"})

	s1 := signal.Series{Attributes: signal.NewAttributes(
		signal.KeyValue{Key: []byte("__name__"), Value: signal.StringValue([]byte("m"))},
		signal.KeyValue{Key: []byte("host"), Value: signal.StringValue([]byte("a"))},
	)}
	s2 := signal.Series{Attributes: signal.NewAttributes(
		signal.KeyValue{Key: []byte("__name__"), Value: signal.StringValue([]byte("m"))},
		signal.KeyValue{Key: []byte("host"), Value: signal.StringValue([]byte("b"))},
	)}

	for ts := int64(1); ts <= 3; ts++ {
		_, err := e.Append(s1, ts, float64(ts))
		require.NoError(t, err)
	}

	_, err := e.Append(s2, 2, 42)
	require.NoError(t, err)
	require.NoError(t, e.Flush(ctx))

	e.mu.RLock()
	require.Len(t, e.parts, 1)
	prefix := e.parts[0].prefix
	require.NotNil(t, e.parts[0].index.paged, "a fresh flush opens with the paged index")
	require.Nil(t, e.parts[0].index.ids, "no resident per-series index is built")
	e.mu.RUnlock()

	// Reopen the same part directly: paged again, and lookups resolve both series.
	p, err := openPart(ctx, be, prefix)
	require.NoError(t, err)
	require.NotNil(t, p.index.paged)

	rng1, ok, err := p.index.lookup(ctx, s1.Hash())
	require.NoError(t, err)
	require.True(t, ok)

	// Sidecar deleted (a part written by an older version): resident fallback, same answers.
	require.NoError(t, be.Delete(ctx, sidxKey(prefix)))

	pOld, err := openPart(ctx, be, prefix)
	require.NoError(t, err)
	require.Nil(t, pOld.index.paged, "no sidecar ⇒ resident index")
	require.NotEmpty(t, pOld.index.ids)

	rngOld, ok, err := pOld.index.lookup(ctx, s1.Hash())
	require.NoError(t, err)
	require.True(t, ok)
	assert.Equal(t, rng1, rngOld, "paged and resident agree")
	assert.Equal(t, p.index.seriesCount(), pOld.index.seriesCount())
	assert.Equal(t, p.rows(), pOld.rows())

	// Corrupt sidecar: rejected at open, resident fallback again.
	require.NoError(t, be.Write(ctx, sidxKey(prefix), []byte("OTSI junk")))

	pBad, err := openPart(ctx, be, prefix)
	require.NoError(t, err)
	assert.Nil(t, pBad.index.paged, "corrupt sidecar ⇒ resident fallback")
}
