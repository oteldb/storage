//go:build !race

// The race detector's instrumentation changes what escapes and when the heap is collected, so the
// live-heap measurements below are taken only in an uninstrumented build.

package recordengine

import (
	"context"
	"runtime"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/backend"
	"github.com/oteldb/storage/backend/file"
	"github.com/oteldb/storage/encoding/compress"
	"github.com/oteldb/storage/internal/heaptest"
	"github.com/oteldb/storage/internal/mergestream"
)

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

	claim := w.residentBytes() - w.finishBytes()

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
// days. The merge reserves its share, or what its sources, encoder and writers' floor need if that is
// more; the writers get what the grant leaves beside the sources and the encoder, less room for one
// append, and the heap the merge adds stays inside the grant. The heap is measured above a baseline
// taken once the sources are written, over the file backend, whose objects are not on the heap.
//
//nolint:paralleltest // collects and samples the process-wide heap, so it must not run concurrently
func TestMergeWritersHoldAdmittedShare(t *testing.T) {
	if testing.Short() {
		t.Skip("ingests 256k rows twice")
	}

	const share = 32 << 20

	type result struct {
		peak, appended, limit, grant int64
		heap                         int64
	}

	run := func(memory int64) result {
		t.Helper()

		ctx := context.Background()

		fb, err := file.New(t.TempDir())
		require.NoError(t, err)

		b := &heaptest.FileSampler{File: fb, KeyContains: "/c/", Writes: true}
		cfg := Config{MergeMemoryBytes: memory, MergeCompression: compress.AlgorithmZSTD}
		e := wideDictEngine(t, b, cfg, 8, 4, 32<<10, 8)

		var r result

		defer SetMergeResidentObserver(func(p, a, l, g int64) { r.peak, r.appended, r.limit, r.grant = p, a, l, g })()

		r.heap = int64(heaptest.Resident(t, b, func() {
			_, err := e.compactParts(ctx, e.parts, minInt64, 0, nil)
			require.NoError(t, err)
		}))

		runtime.KeepAlive(e)

		return r
	}

	unbounded := run(-1)
	r := run(share)

	t.Logf("unbounded: writers report %.1f MiB, heap %.1f MiB", float64(unbounded.peak)/(1<<20), float64(unbounded.heap)/(1<<20))
	t.Logf("share %.1f MiB: grant %.1f MiB, writers limited to %.1f MiB, report %.1f MiB (one append %.1f MiB), heap %.1f MiB",
		float64(share)/(1<<20), float64(r.grant)/(1<<20), float64(r.limit)/(1<<20), float64(r.peak)/(1<<20),
		float64(r.appended)/(1<<20), float64(r.heap)/(1<<20))

	require.GreaterOrEqual(t, r.grant, int64(share), "a merge reserves at least its share")
	require.Less(t, r.limit, r.grant, "the sources and encoder must come off the grant")
	require.Greater(t, unbounded.peak, 2*r.limit, "the writers must outgrow their limit for the bound to be tested")

	assert.LessOrEqual(t, r.peak, r.limit, "the writers outgrew what the grant left them")
	assert.LessOrEqual(t, r.heap, r.grant, "the merge's heap outgrew what it reserved")
	assert.Less(t, r.heap, unbounded.heap, "shedding the writers must give their memory back")
}
