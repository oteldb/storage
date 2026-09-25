package block

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/backend"
	"github.com/oteldb/storage/encoding/chunk"
	"github.com/oteldb/storage/encoding/compress"
)

const (
	v2FixtureGranule = 128
	v2FixtureRows    = 6 * v2FixtureGranule
	v2FixtureDir     = "testdata/v2part"
)

// v2FixtureColumns are the rows the committed part under testdata/v2part was written from, by the
// leading-dictionary PartWriter (manifest version 2) with WithGranuleSize(v2FixtureGranule) and
// WithSortKey("ts"). Object c/i is column i; dots in the file names stand for slashes in the keys.
//
// attrs mixes shared and self granules past 256 entries (2-byte ids), small's dictionary is a zstd
// frame too short to carry a content size, lz and plain are LZ4 and raw, and body declines every
// granule and so is unframed.
func v2FixtureColumns() []Column {
	ts := make([]int64, v2FixtureRows)
	attrs := make([][]byte, v2FixtureRows)
	small := make([][]byte, v2FixtureRows)
	lz := make([][]byte, v2FixtureRows)
	plain := make([][]byte, v2FixtureRows)
	body := make([][]byte, v2FixtureRows)

	for i := range v2FixtureRows {
		g := i / v2FixtureGranule
		ts[i] = 1000 + int64(i)*10

		if g == 2 {
			attrs[i] = fmt.Appendf(nil, "unique-attr-%d", i)
		} else {
			attrs[i] = fmt.Appendf(nil, "attr-%d", g*64+i%64)
		}

		small[i] = fmt.Appendf(nil, "kkkkkk-%02d", i%16)

		if g == 3 {
			lz[i] = fmt.Appendf(nil, "lz-unique-%d", i)
		} else {
			lz[i] = fmt.Appendf(nil, "lz-%d", i%16)
		}

		plain[i] = fmt.Appendf(nil, "plain-%d", i%8)
		body[i] = fmt.Appendf(nil, "body line %d with payload %d", i, i*7919)
	}

	return []Column{
		{Name: "ts", Kind: KindInt64, Codec: chunk.CodecDoD, Int64: ts, Block: true, Compress: compress.AlgorithmZSTD},
		{Name: "attrs", Kind: KindBytes, Bytes: attrs, Block: true, Compress: compress.AlgorithmZSTD},
		{Name: "small", Kind: KindBytes, Bytes: small, Block: true, Compress: compress.AlgorithmZSTD},
		{Name: "lz", Kind: KindBytes, Bytes: lz, Block: true, Compress: compress.AlgorithmLZ4},
		{Name: "plain", Kind: KindBytes, Bytes: plain, Block: true},
		{Name: "body", Kind: KindBytes, Bytes: body, Block: true, Compress: compress.AlgorithmZSTD},
	}
}

// loadV2Fixture serves the committed v2 part from b under prefix "p".
func loadV2Fixture(t *testing.T, b backend.Backend) {
	t.Helper()

	ctx := context.Background()

	files, err := os.ReadDir(v2FixtureDir)
	require.NoError(t, err)

	for _, f := range files {
		data, err := os.ReadFile(filepath.Join(v2FixtureDir, f.Name()))
		require.NoError(t, err)
		require.NoError(t, b.Write(ctx, "p/"+strings.ReplaceAll(f.Name(), ".", "/"), data))
	}
}

func requireRows(t *testing.T, want [][]byte, got *chunk.DictColumn, what string) {
	t.Helper()

	require.Equal(t, len(want), got.Len(), what)

	for i, v := range want {
		require.Equalf(t, v, got.At(i), "%s: row %d", what, i)
	}
}

// TestV2FixtureDecodes pins read compatibility with the leading-dictionary layout: the committed
// part decodes to the rows it was written from through every read path.
func TestV2FixtureDecodes(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	b := backend.Memory()
	loadV2Fixture(t, b)

	r, err := OpenPart(ctx, b, "p")
	require.NoError(t, err)
	require.Equal(t, manifestVersionChecked, r.Manifest().Version)

	for _, c := range v2FixtureColumns() {
		desc, ok := r.ColumnDescByName(c.Name)
		require.True(t, ok, c.Name)
		require.False(t, desc.TrailerDict || desc.HasSizing, c.Name)

		col, err := r.Column(ctx, c.Name)
		require.NoError(t, err)

		if c.Kind == KindInt64 {
			checkFixtureInts(t, r, col, c)

			continue
		}

		whole, err := col.Bytes()
		require.NoError(t, err)
		requireRows(t, c.Bytes, whole, c.Name+" whole")

		if !desc.Blocked {
			require.Equal(t, "body", c.Name, "only the all-decline column is unframed")

			continue
		}

		require.True(t, desc.SharedDict, c.Name)
		checkFixtureSelection(t, col, c)

		for _, open := range []func() (*Decoder, error){
			func() (*Decoder, error) { return r.ColumnBlocks(ctx, c.Name) },
			func() (*Decoder, error) { return r.ColumnScan(ctx, c.Name, 1<<10) },
			col.BlockDecoder,
		} {
			d, err := open()
			require.NoError(t, err)
			checkDecoderRows(t, d, c.Bytes, c.Name)
		}
	}
}

func checkFixtureInts(t *testing.T, r *PartReader, col *ColumnReader, c Column) {
	t.Helper()

	got, err := col.Int64(nil)
	require.NoError(t, err)
	require.Equal(t, c.Int64, got)

	d, err := r.ColumnBlocks(context.Background(), c.Name)
	require.NoError(t, err)

	var ranged []int64

	for g := range d.NumBlocks() {
		v, err := d.DecodeInt64(g)
		require.NoError(t, err)

		ranged = append(ranged, v...)
	}

	require.Equal(t, c.Int64, ranged)
}

func checkFixtureSelection(t *testing.T, col *ColumnReader, c Column) {
	t.Helper()

	sel := []int{0, 2, 5}

	packed, err := col.DecodeBlocksBytes(sel)
	require.NoError(t, err)

	scattered, err := col.DecodeBlocksBytesIntoColumn(sel)
	require.NoError(t, err)

	want := make([][]byte, 0, v2FixtureGranule*len(sel))

	for _, g := range sel {
		lo := g * v2FixtureGranule
		want = append(want, c.Bytes[lo:lo+v2FixtureGranule]...)

		for i := lo; i < lo+v2FixtureGranule; i++ {
			require.Equalf(t, c.Bytes[i], scattered.At(i), "%s scattered row %d", c.Name, i)
		}
	}

	requireRows(t, want, packed, c.Name+" packed")
}

// checkDecoderRows reads every row of d both whole and granule by granule.
func checkDecoderRows(t *testing.T, d *Decoder, want [][]byte, what string) {
	t.Helper()

	all, err := d.DecodeBytes(nil)
	require.NoError(t, err)
	requireRows(t, want, all, what+" decoder")

	for g := range d.NumBlocks() {
		gc, err := d.DecodeBytesBlock(g)
		require.NoError(t, err)

		lo, hi := d.BlockSpan(g)
		requireRows(t, want[lo:hi], gc, what+" granule")
	}
}
