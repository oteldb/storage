package block

import (
	"bytes"
	"context"
	"fmt"
	"math"
	"strconv"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/backend"
	"github.com/oteldb/storage/encoding/chunk"
	"github.com/oteldb/storage/encoding/compress"
)

// scanSource is a part's bytes column open for a forward walk, as a merge source holds it.
func scanSource(t *testing.T, vals [][]byte, gsize int) *Decoder {
	t.Helper()

	ctx := context.Background()
	b := backend.Memory()

	w := NewPartWriter(WithGranuleSize(gsize), WithCompression(compress.AlgorithmZSTD), WithCompressBlockBytes(64))
	require.NoError(t, w.AddColumn(Column{Name: "b", Kind: KindBytes, Bytes: vals, Block: true}))
	require.NoError(t, WritePart(ctx, b, "src", w))

	r, err := OpenPart(ctx, b, "src")
	require.NoError(t, err)

	d, err := r.ColumnScan(ctx, "b", 1<<10)
	require.NoError(t, err)

	return d
}

// walker routes a source's granules the way a merge cursor does: a granule on the shared dictionary
// through the binding bound once to it, any other through a binding re-bound per granule.
type walker struct {
	d            *Decoder
	shared, self *Binding
	sharedGen    DictGen
}

func newWalker(t *testing.T, d *Decoder, w *StreamWriter, col int) *walker {
	t.Helper()

	shared, err := w.Binding(col)
	require.NoError(t, err)
	self, err := w.Binding(col)
	require.NoError(t, err)

	entries, gen := d.SharedEntries()
	require.NoError(t, shared.BindStable(entries, gen))

	return &walker{d: d, shared: shared, self: self, sharedGen: gen}
}

func (wk *walker) granule(t *testing.T, blk int, lo, hi int) {
	t.Helper()

	dc, gen, lease, err := wk.d.DecodeBytesBlock(blk)
	require.NoError(t, err)

	if gen == wk.sharedGen {
		require.NoError(t, wk.shared.AppendDict(dc, gen, lease, lo, hi, nil))

		return
	}

	require.NoError(t, wk.self.Bind(dc.Entries, gen))
	require.NoError(t, wk.self.AppendDict(dc, gen, lease, lo, hi, nil))
}

// TestBindingWalkAcrossPartSplit walks a source whose granules go shared → self → shared, narrow and
// wide, into two output parts cut mid-granule, and checks each against [PartWriter] over its rows.
// The bindings of the first part die with it; the second part binds afresh.
func TestBindingWalkAcrossPartSplit(t *testing.T) {
	t.Parallel()

	vals := transitionValues()
	d := scanSource(t, vals, transitionGranule)
	cut := 5*transitionGranule + transitionGranule/2

	outGranule := 48
	opts := []PartOption{WithGranuleSize(outGranule), WithCompression(compress.AlgorithmZSTD)}

	var second *StreamWriter

	first := NewStreamWriter(opts...)
	require.NoError(t, first.AddColumn(Column{Name: "b", Kind: KindBytes, Block: true}))

	wk := newWalker(t, d, first, 0)
	stale := wk.shared

	for blk := range d.NumBlocks() {
		lo, hi := d.BlockSpan(blk)

		switch {
		case hi <= cut:
			wk.granule(t, blk, 0, hi-lo)
		case lo < cut:
			wk.granule(t, blk, 0, cut-lo)

			part := buildStream(t, first)
			assert.Equal(t, batchObjects(t, vals[:cut], opts), part.objects)

			sharedEntries, sharedGen := d.SharedEntries()
			require.ErrorIs(t, stale.Bind(sharedEntries, sharedGen), errWriterFinished)

			second = NewStreamWriter(opts...)
			require.NoError(t, second.AddColumn(Column{Name: "b", Kind: KindBytes, Block: true}))

			wk = newWalker(t, d, second, 0)
			wk.granule(t, blk, cut-lo, hi-lo)
		default:
			wk.granule(t, blk, 0, hi-lo)
		}
	}

	require.NotNil(t, second)
	assert.Equal(t, batchObjects(t, vals[cut:], opts), buildStream(t, second).objects)
}

func batchObjects(t *testing.T, vals [][]byte, opts []PartOption) [][]byte {
	t.Helper()

	w := NewPartWriter(opts...)
	require.NoError(t, w.AddColumn(Column{Name: "b", Kind: KindBytes, Bytes: vals, Block: true}))

	built, err := w.build()
	require.NoError(t, err)

	return built.objects
}

// TestBindingRejectsForeignTokens: a table is named by its owner's token, so a granule from another
// decoder and the zero token are errors, never a silent mis-map.
func TestBindingRejectsForeignTokens(t *testing.T) {
	t.Parallel()

	vals := transitionValues()
	d := scanSource(t, vals, transitionGranule)
	other := scanSource(t, vals, transitionGranule)

	w := NewStreamWriter(WithGranuleSize(transitionGranule))
	require.NoError(t, w.AddColumn(Column{Name: "b", Kind: KindBytes, Block: true}))

	wk := newWalker(t, d, w, 0)

	shared, sharedGen, lease, err := d.DecodeBytesBlock(0)
	require.NoError(t, err)
	assert.True(t, sameToken(wk.sharedGen, sharedGen))

	_, otherGen := other.SharedEntries()
	assert.False(t, sameToken(sharedGen, otherGen), "two decoders over one part never share a token")
	require.Error(t, wk.shared.AppendDict(shared, otherGen, lease, 0, 1, nil), "another decoder's token")
	require.Error(t, wk.shared.AppendDict(shared, DictGen{}, lease, 0, 1, nil), "the zero token")
	require.Error(t, wk.self.Bind(nil, DictGen{}), "binding the zero token")

	_, otherSelf, _, err := other.DecodeBytesBlock(8)
	require.NoError(t, err)

	self, selfGen, selfLease, err := d.DecodeBytesBlock(8)
	require.NoError(t, err)
	assert.False(t, sameToken(otherSelf, selfGen), "two decoders' self tokens differ")
	assert.False(t, sameToken(wk.sharedGen, selfGen), "a self granule gets its own token")
	require.Error(t, wk.shared.AppendDict(self, selfGen, selfLease, 0, 1, nil))
	require.Error(t, wk.self.BindStable(self.Entries, selfGen), "a frame-backed table cannot be kept by reference")

	require.NoError(t, wk.self.Bind(self.Entries, selfGen))
	require.NoError(t, wk.self.AppendDict(self, selfGen, selfLease, 0, 1, nil))
}

// TestBindingRejectsOverwrittenGranules is the stale-granule case: a decoded granule aliases the
// decoder's frame, so once a later decode crosses a frame boundary its lease is dead even though the
// binding still holds the table's token — a self granule's table and ids, and a shared granule's ids
// alike. The shared dictionary's token itself survives, so fresh shared granules still append.
func TestBindingRejectsOverwrittenGranules(t *testing.T) {
	t.Parallel()

	vals := transitionValues()
	d := scanSource(t, vals, transitionGranule)

	w := NewStreamWriter(WithGranuleSize(transitionGranule))
	require.NoError(t, w.AddColumn(Column{Name: "b", Kind: KindBytes, Block: true}))

	wk := newWalker(t, d, w, 0)

	self, selfGen, selfLease, err := d.DecodeBytesBlock(8)
	require.NoError(t, err)
	require.NoError(t, wk.self.Bind(self.Entries, selfGen))
	require.NoError(t, wk.self.AppendDict(self, selfGen, selfLease, 0, 1, nil))

	sharedA, sharedGen, leaseA, err := d.DecodeBytesBlock(0)
	require.NoError(t, err)
	require.Error(t, wk.self.AppendDict(self, selfGen, selfLease, 1, 2, nil), "an overwritten self table")
	require.Error(t, wk.self.Bind(self.Entries, selfGen), "rebinding a retired token")
	require.NoError(t, wk.shared.AppendDict(sharedA, sharedGen, leaseA, 0, 1, nil))

	require.NotEqual(t, d.streams.dir.frameOf(0), d.streams.dir.frameOf(11), "the next decode crosses a frame")

	next, _, _, err := d.DecodeBytesBlock(11)
	require.NoError(t, err)
	require.NotNil(t, next)
	require.Error(t, wk.shared.AppendDict(sharedA, sharedGen, leaseA, 1, 2, nil),
		"a shared granule whose frame was reused, under the live shared token")

	sharedB, sharedGenB, leaseB, err := d.DecodeBytesBlock(9)
	require.NoError(t, err)
	require.True(t, sameToken(wk.sharedGen, sharedGenB), "one shared token for the decoder's life")
	require.NoError(t, wk.shared.AppendDict(sharedB, sharedGenB, leaseB, 0, 2, nil),
		"self decodes leave the shared token live")

	_, gen2 := d.SharedEntries()
	assert.True(t, sameToken(wk.sharedGen, gen2))
}

// TestBindingAppendDictRejects covers AppendDict's argument checks.
func TestBindingAppendDictRejects(t *testing.T) {
	t.Parallel()

	w := NewStreamWriter(WithGranuleSize(4))
	require.NoError(t, w.AddColumn(Column{Name: "b", Kind: KindBytes, Block: true}))

	b, err := w.Binding(0)
	require.NoError(t, err)

	entries := valuesOf(3, func(i int) string { return fmt.Sprint("e", i) })
	gen := NewDictGen()
	require.NoError(t, b.Bind(entries, gen))

	dc := &chunk.DictColumn{Entries: entries, IDs: []byte{0, 1, 2, 1}, IDWidth: 1}

	require.Error(t, b.AppendDict(dc, gen, Lease{}, -1, 2, nil), "negative lo")
	require.Error(t, b.AppendDict(dc, gen, Lease{}, 3, 2, nil), "hi below lo")
	require.Error(t, b.AppendDict(dc, gen, Lease{}, 0, 5, nil), "past the rows")
	require.Error(t, b.AppendDict(dc, gen, Lease{}, 0, 2, []bool{true}), "keep of the wrong length")
	require.Error(t, b.AppendDict(&chunk.DictColumn{Entries: entries, IDs: []byte{0, 0, 0}, IDWidth: 3}, gen, Lease{}, 0, 1, nil), "id width 3")
	require.Error(t, b.AppendDict(&chunk.DictColumn{Entries: entries[:2], IDs: []byte{0}, IDWidth: 1}, gen, Lease{}, 0, 1, nil), "another table's size")
	require.ErrorIs(t, b.AppendDict(&chunk.DictColumn{Entries: entries, IDs: []byte{7}, IDWidth: 1}, gen, Lease{}, 0, 1, nil), ErrCorrupt)
	require.NoError(t, b.AppendDict(dc, gen, Lease{}, 0, 4, []bool{true, false, true, true}))
	assert.Equal(t, 3, w.Rows())
}

// TestBindingIDWidths feeds the same rows at every id width and flat, against [PartWriter].
func TestBindingIDWidths(t *testing.T) {
	t.Parallel()

	entries := make([][]byte, 0, 300)
	for i := range 300 {
		entries = append(entries, fmt.Appendf(nil, "entry-%d", i))
	}

	vals := make([][]byte, 0, 96)
	for i := range 96 {
		vals = append(vals, entries[(i*7)%12])
	}

	want := batchObjects(t, vals, []PartOption{WithGranuleSize(16)})

	for _, width := range []int{0, 1, 2} {
		t.Run(strconv.Itoa(width), func(t *testing.T) {
			t.Parallel()

			w := NewStreamWriter(WithGranuleSize(16))
			require.NoError(t, w.AddColumn(Column{Name: "b", Kind: KindBytes, Block: true}))

			b, err := w.Binding(0)
			require.NoError(t, err)

			tab := entries[:12]
			if width == 2 {
				tab = entries
			}

			dc := idsColumn(tab, vals, func(v []byte) int {
				for i, e := range tab {
					if bytes.Equal(e, v) {
						return i
					}
				}

				return -1
			})

			if width == 0 {
				dc, tab = &chunk.DictColumn{Entries: vals}, vals
			}

			require.Equal(t, width, dc.IDWidth)

			gen := NewDictGen()
			require.NoError(t, b.Bind(tab, gen))
			require.NoError(t, b.AppendDict(dc, gen, Lease{}, 0, 40, nil))
			require.NoError(t, b.AppendDict(dc, gen, Lease{}, 40, len(vals), nil))
			assert.Equal(t, want, buildStream(t, w).objects)
		})
	}
}

// TestBindingSurvivesGenerationWrap pins the stamp epoch: when the granule generation wraps, every
// cached stamp is cleared rather than read as current.
func TestBindingSurvivesGenerationWrap(t *testing.T) {
	t.Parallel()

	vals := valuesOf(64, func(i int) string { return fmt.Sprint("v", (i*5)%9) })
	want := batchObjects(t, vals, []PartOption{WithGranuleSize(8)})

	w := NewStreamWriter(WithGranuleSize(8))
	require.NoError(t, w.AddColumn(Column{Name: "b", Kind: KindBytes, Block: true}))
	w.cols[0].bytes.g.gen = math.MaxUint32 - 2

	b, err := w.Binding(0)
	require.NoError(t, err)

	entries, _ := splitBytesForm(vals)
	dc := idsColumn(entries, vals, func(v []byte) int {
		for i, e := range entries {
			if bytes.Equal(e, v) {
				return i
			}
		}

		return -1
	})

	gen := NewDictGen()
	require.NoError(t, b.Bind(entries, gen))

	for lo := 0; lo < len(vals); lo += 5 {
		require.NoError(t, b.AppendDict(dc, gen, Lease{}, lo, min(lo+5, len(vals)), nil))
	}

	assert.Equal(t, uint32(1), w.cols[0].bytes.g.epoch)
	assert.Equal(t, want, buildStream(t, w).objects)
}

// TestBindingRawColumn: a binding to a raw column appends each row's value.
func TestBindingRawColumn(t *testing.T) {
	t.Parallel()

	vals := valuesOf(20, func(i int) string { return fmt.Sprint("r", i%3) })

	pw := NewPartWriter(WithGranuleSize(8))
	require.NoError(t, pw.AddColumn(Column{Name: "b", Kind: KindBytes, Codec: chunk.CodecBytesRaw, Bytes: vals, Block: true}))
	want, err := pw.build()
	require.NoError(t, err)

	w := NewStreamWriter(WithGranuleSize(8))
	require.NoError(t, w.AddColumn(Column{Name: "b", Kind: KindBytes, Codec: chunk.CodecBytesRaw, Block: true}))

	b, err := w.Binding(0)
	require.NoError(t, err)

	tab := valuesOf(3, func(i int) string { return fmt.Sprint("r", i) })
	gen := NewDictGen()
	require.NoError(t, b.Bind(tab, gen))
	require.NoError(t, b.AppendDict(idsColumn(tab, vals, func(v []byte) int { return int(v[1] - '0') }), gen, Lease{}, 0, len(vals), nil))
	assert.Equal(t, want.objects, buildStream(t, w).objects)
}

// sameToken compares by owner identity; testify's Equal would compare the owners' contents.
func sameToken(a, b DictGen) bool { return a == b }
