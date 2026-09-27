//go:build !race

// The race detector's instrumentation changes what escapes and when the heap is collected, so the
// live-heap measurements below are taken only in an uninstrumented build.

package recordengine

import (
	"context"
	"fmt"
	"math/rand/v2"
	"runtime"
	"strconv"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/backend"
	"github.com/oteldb/storage/backend/file"
	"github.com/oteldb/storage/encoding/chunk"
	"github.com/oteldb/storage/internal/heaptest"
	"github.com/oteldb/storage/internal/mergestream"
	"github.com/oteldb/storage/signal"
)

// wideDictSchema has three dictionary columns every source writes on a large shared dictionary, the
// shape that makes a writer's per-source bindings its largest term; one carries a bloom and one is the
// attributes column, so the sidecar state is exercised too.
var wideDictSchema = NewSchema(
	Column{Name: "sev", Kind: KindInt64, Codec: chunk.CodecT64},
	Column{Name: "a", Kind: KindBytes, Codec: chunk.CodecDict, Bloom: BloomFullText},
	Column{Name: "b", Kind: KindBytes, Codec: chunk.CodecDict},
	Column{Name: "attrs", Kind: KindBytes, Codec: chunk.CodecDict, Bloom: BloomAttrs},
)

// wideDictEngine flushes `sources` parts over be, each holding `streams` streams of `rows` records spread
// evenly over `days` days, with column values drawn so every granule joins a shared dictionary that
// grows by ~2000 entries per 4096 rows.
func wideDictEngine(tb testing.TB, be backend.Backend, cfg Config, sources, streams, rows, days int) *Engine {
	tb.Helper()

	ctx := context.Background()
	cfg.Schema, cfg.Backend, cfg.Prefix = wideDictSchema, be, "t/wide"
	e := New(cfg)
	r := rand.New(rand.NewPCG(11, 13))

	for s := range sources {
		i := 0

		for st := range streams {
			series := signal.Series{Resource: signal.Resource{Attributes: signal.NewAttributes(
				signal.KeyValue{Key: []byte("service.name"), Value: signal.StringValue([]byte("svc-" + strconv.Itoa(st)))},
			)}}
			b := &Batch{
				Stream: series.Hash(), Identity: func() signal.Series { return series },
				Ints: make([][]int64, 1), Bytes: make([][][]byte, 3),
			}

			per := rows / streams
			for j := range per {
				day := int64(j * days / per)
				pick := func(col int) int { return s*1_000_000 + col*100_000 + (i/4096)*2000 + r.IntN(2000) }

				b.Ts = append(b.Ts, streamedDay+day*streamedDay+int64(j))
				b.Ints[0] = append(b.Ints[0], int64(j%5))
				b.Bytes[0] = append(b.Bytes[0], fmt.Appendf(nil, "word%07d text", pick(0)))
				b.Bytes[1] = append(b.Bytes[1], fmt.Appendf(nil, "value-%07d", pick(1)))
				b.Bytes[2] = append(b.Bytes[2], signal.NewAttributes(
					signal.KeyValue{Key: []byte("k"), Value: signal.StringValue(fmt.Appendf(nil, "%07d", pick(2)))},
				).AppendHashInput(nil))
				i++
			}

			_, err := e.AppendBatch(b, AppendLimits{})
			require.NoError(tb, err)
		}

		require.NoError(tb, e.Flush(ctx))
	}

	for _, p := range e.parts {
		for _, name := range []string{"a", "b", "attrs"} {
			desc, ok := p.reader.ColumnDescByName(name)
			require.True(tb, ok)
			require.True(tb, desc.SharedDict, "column %q must be on a shared dictionary", name)
		}
	}

	return e
}

// TestRecordPartWriterResidentTracksHeap: what a writer reports holding is what dropping it gives
// back to the heap — its dictionaries, staged granules, frames, bindings to every source's dictionary
// and sidecar state — so the router sheds on a figure that is not an undercount.
//
//nolint:paralleltest // collects and samples the process-wide heap, so it must not run concurrently
func TestRecordPartWriterResidentTracksHeap(t *testing.T) {
	ctx := context.Background()
	e := wideDictEngine(t, backend.Memory(), Config{MergeMemoryBytes: -1}, 6, 4, 32<<10, 1)
	src := e.parts

	sources, err := e.openMergeSources(ctx, src)
	require.NoError(t, err)

	w, err := newRecordPartStreamWriter(ctx, e, src, nil)
	require.NoError(t, err)

	var (
		keys mergestream.Keys
		heap runHeap
	)

	mergeKeys(src, &keys)

	for keys.Next() {
		id := keys.Key()
		require.NoError(t, heap.reset(sources, id, minInt64))

		for heap.len() > 0 {
			require.NoError(t, heap.fill(w, idToU128(id), maxInt64, 0))
		}
	}

	claim := w.residentBytes()

	runtime.GC()

	held := heaptest.Live()

	w.abort()
	w = nil //nolint:wastedassign // drops the writer before the heap is measured

	runtime.GC()

	released := heaptest.Live()

	runtime.KeepAlive(sources)
	runtime.KeepAlive(e)

	measured := int64(held) - int64(released)
	t.Logf("writer reports %.1f MiB, dropping it releases %.1f MiB", float64(claim)/(1<<20), float64(measured)/(1<<20))

	require.Greater(t, measured, int64(8<<20), "the writer must hold enough for the comparison to mean anything")
	assert.GreaterOrEqual(t, float64(claim), 0.9*float64(measured), "the writer undercounts what it holds")
	assert.LessOrEqual(t, float64(claim), 1.5*float64(measured), "the writer's figure does not track what it holds")
}

// TestMergeWritersHoldAdmittedShare: a straddling merge of many sources with large dictionaries binds
// every source's dictionary in every day's writer, so its writers' state multiplies with sources ×
// days. The router sheds them at the merge's admitted share, and the heap shows the memory it claims
// to have shed is really given back.
//
//nolint:paralleltest // collects and samples the process-wide heap, so it must not run concurrently
func TestMergeWritersHoldAdmittedShare(t *testing.T) {
	if testing.Short() {
		t.Skip("ingests 256k rows twice")
	}

	const share = 32 << 20

	run := func(memory int64) (peak, appended, limit int64, heapPeak uint64) {
		t.Helper()

		ctx := context.Background()

		fb, err := file.New(t.TempDir())
		require.NoError(t, err)

		b := &heaptest.FileSampler{File: fb, KeyContains: "/c/", Writes: true}
		e := wideDictEngine(t, b, Config{MergeMemoryBytes: memory}, 8, 4, 32<<10, 8)

		defer SetMergeResidentObserver(func(p, r, l int64) { peak, appended, limit = p, r, l })()

		heapPeak = heaptest.Resident(t, b, func() {
			_, err := e.compactParts(ctx, e.parts, minInt64, 0)
			require.NoError(t, err)
		})

		runtime.KeepAlive(e)

		return peak, appended, limit, heapPeak
	}

	unboundedPeak, _, _, unboundedHeap := run(-1)
	peak, appended, limit, heap := run(share)

	t.Logf("unbounded: writers report %.1f MiB, heap %.1f MiB", float64(unboundedPeak)/(1<<20), float64(unboundedHeap)/(1<<20))
	t.Logf("share %.1f MiB: writers report %.1f MiB (one append %.1f MiB), heap %.1f MiB",
		float64(limit)/(1<<20), float64(peak)/(1<<20), float64(appended)/(1<<20), float64(heap)/(1<<20))

	require.Equal(t, int64(share), limit)
	require.Greater(t, unboundedPeak, int64(2*share), "the writers must outgrow the share for the bound to be tested")

	assert.LessOrEqual(t, peak, limit+appended, "the writers outgrew the admitted share")
	assert.Less(t, appended, limit/3, "one append must be small against the share for the bound to mean anything")

	shed := unboundedPeak - peak
	assert.Less(t, float64(heap), float64(unboundedHeap)-0.5*float64(shed),
		"the heap did not give back the memory the writers claim to have shed")
}
