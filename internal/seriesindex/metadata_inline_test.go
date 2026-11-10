package seriesindex

import (
	"bytes"
	"context"
	"encoding/binary"
	"math"
	"slices"
	"strconv"
	"testing"

	"github.com/go-faster/errors"
	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/backend"
	"github.com/oteldb/storage/index/postings"
	"github.com/oteldb/storage/signal"
)

func TestMetadataLegacyReferenceFallback(t *testing.T) {
	t.Parallel()
	src := indexSeries()
	data, _ := buildIndex(t, src)
	legacy := rewriteIndex(t, data, func(rec *testRecord) {
		if rec.key[0] == 'i' {
			rec.value = rec.value[:4]
		}
	})
	r := openTestIndex(t, legacy)
	reader := r.MetadataReader(t.Context())
	for i, s := range src {
		meta, found, err := reader.Lookup(s.Hash())
		require.NoError(t, err)
		require.True(t, found)
		require.EqualValues(t, i+10, meta.Covered)
		got, singleMeta, found, err := r.Lookup(t.Context(), s.Hash())
		require.NoError(t, err)
		require.True(t, found)
		require.True(t, s.Equal(got))
		require.True(t, equalMetadata(meta, singleMeta))
	}
	bad := rewriteIndex(t, legacy, func(rec *testRecord) {
		if bytes.Equal(rec.key, recordKey(0)) {
			rec.value[len(rec.value)-1] ^= 1
		}
	})
	r = openTestIndex(t, bad)
	_, _, err := r.Metadata(t.Context(), src[0].Hash())
	require.ErrorIs(t, err, ErrCorrupt)
	_, _, err = r.MetadataReader(t.Context()).Lookup(src[0].Hash())
	require.ErrorIs(t, err, ErrCorrupt)
}

func TestInlineMetadataRejectsInvalidReferences(t *testing.T) {
	t.Parallel()
	src := indexSeries()
	data, _ := buildIndex(t, src)
	for _, mode := range []string{"ordinal", "short", "flags", "trailing", "missing ID"} {
		t.Run(mode, func(t *testing.T) {
			t.Parallel()
			bad := rewriteIndex(t, data, func(rec *testRecord) {
				if !bytes.Equal(rec.key, idKey(src[0].Hash())) {
					return
				}
				switch mode {
				case "ordinal":
					binary.BigEndian.PutUint32(rec.value, 1)
				case "short":
					rec.value = rec.value[:8]
				case "flags":
					rec.value[12] = 2
				case "trailing":
					rec.value = append(rec.value, 0)
				case "missing ID":
					rec.key = nil
				}
			})
			r := openTestIndex(t, bad)
			_, _, err := r.Metadata(t.Context(), src[0].Hash())
			require.ErrorIs(t, err, ErrCorrupt)
			_, _, err = r.MetadataReader(t.Context()).Lookup(src[0].Hash())
			require.ErrorIs(t, err, ErrCorrupt)
			ids := r.IDs(t.Context())
			for ids.Next() {
			}
			require.ErrorIs(t, ids.Err(), ErrCorrupt)
		})
	}
}

func TestInlineMetadataDescriptorAgreement(t *testing.T) {
	t.Parallel()
	src := indexSeries()
	data, _ := buildIndex(t, src)
	bad := rewriteIndex(t, data, func(rec *testRecord) {
		if bytes.Equal(rec.key, idKey(src[0].Hash())) {
			binary.BigEndian.PutUint64(rec.value[4:], 1234)
		}
	})
	r := openTestIndex(t, bad)
	series, _, found, err := r.Lookup(t.Context(), src[0].Hash())
	require.Zero(t, series)
	require.False(t, found)
	require.ErrorIs(t, err, ErrCorrupt)
}

type descriptorFaultTable struct {
	*rangedTable

	offset int64
	err    error
}

func (b *descriptorFaultTable) ReadAtAppend(ctx context.Context, dst []byte, key string, off, n int64) ([]byte, error) {
	if off == b.offset {
		return dst, b.err
	}
	return b.rangedTable.ReadAtAppend(ctx, dst, key, off, n)
}

func TestInlineMetadataAvoidsDescriptorPages(t *testing.T) {
	t.Parallel()
	src := make([]signal.Series, 256)
	for i := range src {
		src[i] = signal.Series{Resource: signal.Resource{SchemaURL: bytes.Repeat([]byte("schema"), 600)},
			Attributes: signal.NewAttributes(signal.KeyValue{Key: []byte("host"), Value: signal.StringValue([]byte(strconv.Itoa(i)))})}
	}
	slices.SortFunc(src, func(a, b signal.Series) int { return a.Hash().Compare(b.Hash()) })
	data, _ := buildIndex(t, src)
	fault := errors.New("descriptor page unavailable")
	be := &descriptorFaultTable{rangedTable: &rangedTable{Backend: backend.Memory(), data: data}, offset: -1, err: fault}
	r, err := Open(t.Context(), be, "index")
	require.NoError(t, err)
	c := r.table.Cursor(t.Context())
	ref := uint32(len(src) - 1)
	require.True(t, c.Seek(recordKey(ref)))
	page, err := c.entry(uint32(c.page))
	require.NoError(t, err)
	be.offset = page.offset
	id := src[ref].Hash()
	meta, found, err := r.Metadata(t.Context(), id)
	require.NoError(t, err)
	require.True(t, found)
	require.EqualValues(t, int(ref)+10, meta.Covered)
	meta, found, err = r.MetadataReader(t.Context()).Lookup(id)
	require.NoError(t, err)
	require.True(t, found)
	require.EqualValues(t, int(ref)+10, meta.Covered)
	series, _, found, err := r.Lookup(t.Context(), id)
	require.Zero(t, series)
	require.False(t, found)
	require.ErrorIs(t, err, fault)
	err = r.Visit(t.Context(), postings.FromSlice([]signal.SeriesID{{Lo: uint64(ref)}}),
		func(signal.SeriesID, []byte, Metadata) error { return nil })
	require.ErrorIs(t, err, fault)
}

func metadataCorpus() []Metadata {
	return []Metadata{{}, {Covered: -123}, {Covered: math.MaxInt64, HasStats: true,
		Stats: Stats{Count: 3, Sum: math.Float64frombits(1 << 63), Min: math.Float64frombits(0x7ff8000000000042), Max: math.Inf(1)}}}
}

func TestMetadataCodec(t *testing.T) {
	t.Parallel()
	for _, meta := range metadataCorpus() {
		data := appendMetadata(binary.BigEndian.AppendUint32(nil, 7), meta)
		ref, got, inline, err := decodeIDValue(data)
		require.NoError(t, err)
		require.EqualValues(t, 7, ref)
		require.True(t, inline)
		require.True(t, equalMetadata(meta, got))
	}
}

func FuzzDecodeIDMetadata(f *testing.F) {
	f.Add([]byte{0, 0, 0, 1})
	for _, meta := range metadataCorpus() {
		f.Add(appendMetadata(binary.BigEndian.AppendUint32(nil, 7), meta))
	}
	f.Fuzz(func(t *testing.T, data []byte) {
		if len(data) > 128 {
			t.Skip()
		}
		ref, meta, inline, err := decodeIDValue(data)
		if err != nil {
			return
		}
		encoded := binary.BigEndian.AppendUint32(nil, ref)
		if inline {
			encoded = appendMetadata(encoded, meta)
		}
		require.Equal(t, data, encoded)
	})
}
