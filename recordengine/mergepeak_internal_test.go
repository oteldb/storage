//go:build !race

package recordengine

import (
	"context"
	"encoding/hex"
	"math/rand/v2"
	"runtime"
	"strconv"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/backend/file"
	"github.com/oteldb/storage/encoding/compress"
	"github.com/oteldb/storage/internal/heaptest"
	"github.com/oteldb/storage/signal"
)

// mergeGrantPeak runs a merge of e's parts over b and returns the heap it added and the grant it
// ended with.
func mergeGrantPeak(t *testing.T, e *Engine, b *heaptest.FileSampler) (heap, grant int64) {
	t.Helper()

	defer SetMergeResidentObserver(func(_, _, _, g int64) { grant = g })()

	heap = int64(heaptest.Resident(t, b, func() {
		_, err := e.compactParts(context.Background(), e.parts, minInt64, e.mergeCapBytes(), nil)
		require.NoError(t, err)
	}))

	runtime.KeepAlive(e)

	return heap, grant
}

// TestMergeGatherHoldsItsGrant: a source whose one large stream is out of timestamp order is decoded
// whole and its stream gathered and sorted — copies of every row, a sort index and a second set of
// columns — and the merge's grant covers all of it.
//
//nolint:paralleltest // samples the process-wide heap
func TestMergeGatherHoldsItsGrant(t *testing.T) {
	fb, err := file.New(t.TempDir())
	require.NoError(t, err)

	b := &heaptest.FileSampler{File: fb, Writes: true}
	e := New(Config{Schema: headTestSchema, Backend: b, Prefix: "t/recs", MergeMemoryBytes: 1 << 20})

	const rows = 200_000

	ts := make([]int64, rows)
	for i := range ts {
		ts[i] = int64(i)
	}

	f := streamColumns(t, e, map[string][]int64{"api": ts})
	for i := range f.cols.ts {
		f.cols.ts[i] = int64(rows - i)
	}

	e.parts = append(e.parts[:0], writeTestPart(t, e, b, f))

	heap, grant := mergeGrantPeak(t, e, b)

	t.Logf("gather of %d rows: merge heap %.1f MiB, grant %.1f MiB", rows, float64(heap)/(1<<20), float64(grant)/(1<<20))
	require.True(t, e.parts[0].tsDisorder.Load(), "the stream must have been gathered")
	assert.LessOrEqual(t, heap, grant, "the gather outgrew the merge's grant")
}

// TestMergeFinishHoldsItsGrant: day writers over large incompressible dictionaries, blooms and record
// keys finish while the others are still open, and keep the parts they sealed until the merge
// commits; the merge's grant covers the finishes too. The heap is sampled at every object a finish
// writes and reads back.
//
//nolint:paralleltest // samples the process-wide heap
func TestMergeFinishHoldsItsGrant(t *testing.T) {
	ctx := context.Background()

	fb, err := file.New(t.TempDir())
	require.NoError(t, err)

	b := &heaptest.FileSampler{File: fb, Writes: true}
	e := New(Config{
		Schema: wideDictSchema, Backend: b, Prefix: "t/wide",
		MergeMemoryBytes: 32 << 20, MergeCompression: compress.AlgorithmZSTD,
	})

	r := rand.New(rand.NewPCG(3, 9))
	token := func() []byte {
		v := make([]byte, 24)
		for i := range v {
			v[i] = byte(r.Uint32())
		}

		return []byte(hex.EncodeToString(v))
	}

	const (
		sources = 4
		days    = 8
		rows    = 16 << 10
	)

	for s := range sources {
		series := signal.Series{Resource: signal.Resource{Attributes: signal.NewAttributes(
			signal.KeyValue{Key: []byte("service.name"), Value: signal.StringValue([]byte("svc-" + strconv.Itoa(s)))},
		)}}
		batch := &Batch{
			Stream: series.Hash(), Identity: func() signal.Series { return series },
			Ints: make([][]int64, 1), Bytes: make([][][]byte, 3),
		}

		pool := make([][]byte, 4096)
		for i := range pool {
			pool[i] = token()
		}

		for j := range rows {
			v := pool[r.IntN(len(pool))]

			batch.Ts = append(batch.Ts, streamedDay+int64(j*days/rows)*streamedDay+int64(j))
			batch.Ints[0] = append(batch.Ints[0], int64(j%5))
			batch.Bytes[0] = append(batch.Bytes[0], append(append([]byte{}, v...), ' '))
			batch.Bytes[1] = append(batch.Bytes[1], v)
			batch.Bytes[2] = append(batch.Bytes[2], signal.NewAttributes(
				signal.KeyValue{Key: v[:16], Value: signal.StringValue(v)},
			).AppendHashInput(nil))
		}

		_, err := e.AppendBatch(batch, AppendLimits{})
		require.NoError(t, err)
		require.NoError(t, e.Flush(ctx))
	}

	heap, grant := mergeGrantPeak(t, e, b)

	t.Logf("%d sources × %d days: merge heap %.1f MiB, grant %.1f MiB", sources, days, float64(heap)/(1<<20), float64(grant)/(1<<20))
	assert.LessOrEqual(t, heap, grant, "the finishes outgrew the merge's grant")
}
