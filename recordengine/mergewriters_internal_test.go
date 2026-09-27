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
	"github.com/oteldb/storage/encoding/compress"
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
// days. The writers get what the admitted share leaves beside the sources and the coders, less room
// for one append, and the heap the merge adds stays inside the share. The heap is measured above a
// baseline taken once the sources are written, over the file backend, whose objects are not on the
// heap.
//
//nolint:paralleltest // collects and samples the process-wide heap, so it must not run concurrently
func TestMergeWritersHoldAdmittedShare(t *testing.T) {
	if testing.Short() {
		t.Skip("ingests 256k rows twice")
	}

	const share = 64 << 20

	run := func(memory int64) (peak, appended, limit int64, heapPeak uint64) {
		t.Helper()

		ctx := context.Background()

		fb, err := file.New(t.TempDir())
		require.NoError(t, err)

		b := &heaptest.FileSampler{File: fb, KeyContains: "/c/", Writes: true}
		cfg := Config{MergeMemoryBytes: memory, MergeCompression: compress.AlgorithmZSTD}
		e := wideDictEngine(t, b, cfg, 8, 4, 32<<10, 8)

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
	t.Logf("share %.1f MiB: writers limited to %.1f MiB, report %.1f MiB (one append %.1f MiB), heap %.1f MiB",
		float64(share)/(1<<20), float64(limit)/(1<<20), float64(peak)/(1<<20), float64(appended)/(1<<20),
		float64(heap)/(1<<20))

	require.Less(t, limit, int64(share), "the sources and coders must come off the share")
	require.Greater(t, unboundedPeak, 2*limit, "the writers must outgrow their limit for the bound to be tested")

	assert.LessOrEqual(t, peak, limit, "the writers outgrew what the share left them")
	assert.LessOrEqual(t, int64(heap), int64(share), "the merge's heap outgrew its share")
	assert.Less(t, heap, unboundedHeap, "shedding the writers must give their memory back")
}
