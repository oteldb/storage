package seriesindex

import (
	"bytes"
	"context"
	"encoding/binary"
	"slices"
	"strconv"
	"testing"

	"github.com/go-faster/errors"

	"github.com/oteldb/storage/backend"

	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/signal"
)

func TestMetadataReaderReset(t *testing.T) {
	t.Parallel()
	src := indexSeries()
	data, _ := buildIndex(t, src)
	for _, legacy := range []bool{false, true} {
		t.Run(strconv.FormatBool(legacy), func(t *testing.T) {
			t.Parallel()
			first := data
			if legacy {
				first = rewriteIndex(t, first, func(rec *testRecord) {
					if rec.key[0] == 'i' {
						rec.value = rec.value[:4]
					}
				})
			}
			second := rewriteIndex(t, first, func(rec *testRecord) {
				if rec.key[0] == 's' {
					binary.BigEndian.PutUint64(rec.value[16:], 999)
				}
				if rec.key[0] == 'i' && len(rec.value) > 4 {
					binary.BigEndian.PutUint64(rec.value[4:], 999)
				}
			})
			a, b := openTestIndex(t, first), openTestIndex(t, second)
			require.NoError(t, b.validateReferences(t.Context()))
			var reader MetadataReader
			reader.Reset(t.Context(), a)
			meta, found, err := reader.Lookup(src[0].Hash())
			require.NoError(t, err)
			require.True(t, found)
			require.EqualValues(t, 10, meta.Covered)
			ctx, cancel := context.WithCancel(t.Context())
			cancel()
			reader.Reset(ctx, b)
			_, _, err = reader.Lookup(src[0].Hash())
			require.ErrorIs(t, err, context.Canceled)
			reader.Reset(t.Context(), b)
			meta, found, err = reader.Lookup(src[0].Hash())
			require.NoError(t, err)
			require.True(t, found)
			require.EqualValues(t, 999, meta.Covered, "same key in a new part must not reuse the old value")
			reader.Reset(t.Context(), nil)
			_, _, err = reader.Lookup(src[0].Hash())
			require.Error(t, err)
			reader.Reset(t.Context(), a)
			meta, found, err = reader.Lookup(src[0].Hash())
			require.NoError(t, err)
			require.True(t, found)
			require.EqualValues(t, 10, meta.Covered)
		})
	}
}

func BenchmarkMetadataReaderAcrossParts(b *testing.B) {
	src := make([]signal.Series, 1024)
	for i := range src {
		src[i] = signal.Series{Attributes: signal.NewAttributes(signal.KeyValue{Key: []byte("host"), Value: signal.IntValue(int64(i))})}
	}
	slices.SortFunc(src, func(a, b signal.Series) int { return a.Hash().Compare(b.Hash()) })
	data, _ := buildIndex(b, src)
	ids := make([]signal.SeriesID, len(src))
	for i := range src {
		ids[i] = src[i].Hash()
	}
	indexes := make([]*Index, 8)
	for i := range indexes {
		indexes[i] = openTestIndex(b, data)
		require.NoError(b, indexes[i].validateReferences(b.Context()))
	}
	for _, reuse := range []bool{false, true} {
		b.Run(strconv.FormatBool(reuse), func(b *testing.B) {
			b.ReportAllocs()
			for b.Loop() {
				var reusable MetadataReader
				for _, index := range indexes {
					reader := &reusable
					if reuse {
						reader.Reset(b.Context(), index)
					} else {
						reader = index.MetadataReader(b.Context())
					}
					for i := 0; i < len(src); i += 16 {
						meta, found, err := reader.Lookup(ids[i])
						if err != nil || !found || meta.Covered != int64(i+10) {
							b.Fatalf("metadata lookup: %v, %v, %v", meta, found, err)
						}
					}
				}
			}
		})
	}
}

func TestMetadataReaderResetAcrossFormatsAfterDescriptorError(t *testing.T) {
	t.Parallel()
	src := make([]signal.Series, 256)
	for i := range src {
		src[i] = signal.Series{Resource: signal.Resource{SchemaURL: bytes.Repeat([]byte("schema"), 600)},
			Attributes: signal.NewAttributes(signal.KeyValue{Key: []byte("host"), Value: signal.IntValue(int64(i))})}
	}
	slices.SortFunc(src, func(a, b signal.Series) int { return a.Hash().Compare(b.Hash()) })
	inline, _ := buildIndex(t, src)
	legacy := rewriteIndex(t, inline, func(rec *testRecord) {
		if rec.key[0] == 'i' {
			rec.value = rec.value[:4]
		}
	})
	fault := errors.New("legacy descriptor unavailable")
	be := &descriptorFaultTable{rangedTable: &rangedTable{Backend: backend.Memory(), data: legacy}, offset: -1, err: fault}
	bad, err := Open(t.Context(), be, "legacy")
	require.NoError(t, err)
	ref := uint32(len(src) - 1)
	c := bad.table.Cursor(t.Context())
	require.True(t, c.Seek(recordKey(ref)))
	page, err := c.entry(uint32(c.page))
	require.NoError(t, err)
	be.offset = page.offset
	id := src[ref].Hash()
	var reader MetadataReader
	for _, part := range []*Index{openTestIndex(t, legacy), bad, openTestIndex(t, inline), openTestIndex(t, legacy)} {
		reader.Reset(t.Context(), part)
		meta, found, err := reader.Lookup(id)
		if part == bad {
			require.ErrorIs(t, err, fault)
			continue
		}
		require.NoError(t, err)
		require.True(t, found)
		require.EqualValues(t, int(ref)+10, meta.Covered)
	}
}
