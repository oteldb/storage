package recordengine

import (
	"context"
	"fmt"
	"math/rand/v2"
	"strconv"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/backend"
	"github.com/oteldb/storage/encoding/chunk"
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

// TestSourceBoundCoversOpened: what a merge reserves for a source from its manifest, before opening
// it, is at least what the opened source reports, read forward or decoded whole.
//
//nolint:paralleltest // sets the package-global whole-decode seam
func TestSourceBoundCoversOpened(t *testing.T) {
	ctx := context.Background()
	e := wideDictEngine(t, backend.Memory(), Config{MergeMemoryBytes: -1}, 3, 4, 8<<10, 2)

	for _, whole := range []bool{false, true} {
		t.Run(fmt.Sprintf("whole=%v", whole), func(t *testing.T) {
			defer SetMergeReadWhole(whole)()

			sources, err := e.openMergeSources(ctx, e.parts)
			require.NoError(t, err)

			for i, p := range e.parts {
				b, ok := e.sourceBound(ctx, p)
				require.True(t, ok, "source %d", i)
				assert.GreaterOrEqual(t, b.steady, sources[i].residentBytes(), "source %d", i)
				assert.GreaterOrEqual(t, b.entries, sources[i].dictEntries(), "source %d", i)
			}
		})
	}
}

// TestWriterFinishAtCapCoversAWholePart: the floor's estimate of a finish at the cap is at least what
// a writer holding a whole part charges for its finish, so the floor never sheds a writer for its own
// finish before its part is full.
func TestWriterFinishAtCapCoversAWholePart(t *testing.T) {
	t.Parallel()

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

	estimate := e.writerFinishAtCap(src, 0)
	t.Logf("finish charged %.1f MiB, estimated at the cap %.1f MiB", float64(w.finishBytes())/(1<<20), float64(estimate)/(1<<20))
	assert.GreaterOrEqual(t, estimate, w.finishBytes())

	w.abort()
}
