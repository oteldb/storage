package recordengine

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/signal"
)

// buildFlushColumns must order each stream by ts without touching the source buffers: they are the
// detached head buffers, which the engine keeps fetchable (and so concurrently readable) until the
// part is published.
func TestBuildFlushColumnsSortsWithoutMutatingSource(t *testing.T) {
	t.Parallel()

	s := NewSchema(
		Column{Name: "sev", Kind: KindInt64},
		Column{Name: "body", Kind: KindBytes},
	)

	ts := []int64{50, 10, 30, 10, 20}

	build := func() *recordCols {
		c := newRecordCols(s, 0, fullSel(s))
		for i, v := range ts {
			c.appendClone(rec{ts: v, ints: []int64{int64(i)}, bytes: [][]byte{fmt.Appendf(nil, "b%d", i)}})
		}

		return c
	}

	src := build()
	records := map[signal.SeriesID]*recordCols{{Hi: 1, Lo: 2}: src}

	f := buildFlushColumns(s, records, nil)

	require.Equal(t, []int64{10, 10, 20, 30, 50}, f.cols.ts, "flush columns are ts-ordered")
	// Stable order: the two ts=10 rows keep their arrival order (rows 1 and 3).
	assert.Equal(t, []int64{1, 3, 4, 2, 0}, f.cols.ints[0], "the whole row travels with its ts")

	got := make([]string, f.cols.len())
	for i := range got {
		got[i] = string(f.cols.bytes[0].at(i))
	}

	assert.Equal(t, []string{"b1", "b3", "b4", "b2", "b0"}, got, "byte cells follow the same order")

	// The source is byte-for-byte what it was before the flush.
	want := build()
	assert.Equal(t, want.ts, src.ts, "source timestamps untouched")
	assert.Equal(t, want.ints, src.ints, "source int columns untouched")
	assert.Equal(t, want.bytes, src.bytes, "source byte columns untouched")
}

// BenchmarkBuildFlushColumnsUnsorted measures the flush gather when every stream is out of ts order,
// so the permutation is computed and applied for each of them.
func BenchmarkBuildFlushColumnsUnsorted(b *testing.B) {
	const (
		streams = 64
		rows    = 512
	)

	schema := NewSchema(
		Column{Name: "body", Kind: KindBytes},
		Column{Name: "attrs", Kind: KindBytes},
	)

	records := make(map[signal.SeriesID]*recordCols, streams)
	for s := range streams {
		c := newRecordCols(schema, rows, fullSel(schema))
		for i := range rows {
			c.appendClone(rec{
				ts:    int64(rows - i), // strictly descending ⇒ always needs reordering
				bytes: [][]byte{[]byte("some log body text here"), []byte("k1=v1 k2=v2")},
			})
		}

		records[signal.SeriesID{Hi: uint64(s), Lo: uint64(s)}] = c
	}

	b.ReportAllocs()

	var f *flushColumns
	for b.Loop() {
		f = buildFlushColumns(schema, records, f)
	}
}
