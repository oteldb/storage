package recordengine_test

import (
	"bytes"
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/backend"
	"github.com/oteldb/storage/encoding/chunk"
	"github.com/oteldb/storage/query/fetch"
	"github.com/oteldb/storage/recordengine"
	"github.com/oteldb/storage/signal"
)

// rawIDSchema mirrors the trace_id shape (signal/trace.Schema): a CodecBytesRaw, BloomEquality
// id column, so equality conditions against it are eligible for [eqFastPathCols]'s whole-column
// AVX2/stride scan instead of a per-row [chunk.DictColumn] decode+Match.
var rawIDSchema = recordengine.NewSchema(
	recordengine.Column{Name: "body", Kind: recordengine.KindBytes, Codec: chunk.CodecDict, Bloom: recordengine.BloomFullText},
	recordengine.Column{Name: "trace_id", Kind: recordengine.KindBytes, Codec: chunk.CodecBytesRaw, Bloom: recordengine.BloomEquality},
)

type rawIDRec struct {
	ts   int64
	body string
	id   []byte
}

func mkRawIDBatch(svc string, recs ...rawIDRec) *recordengine.Batch {
	res := signal.Resource{Attributes: signal.NewAttributes(
		signal.KeyValue{Key: []byte("service.name"), Value: signal.StringValue([]byte(svc))},
	)}
	series := signal.Series{Resource: res}

	b := &recordengine.Batch{
		Stream:   series.Hash(),
		Identity: func() signal.Series { return series },
		Bytes:    make([][][]byte, 2),
	}

	for _, r := range recs {
		b.Ts = append(b.Ts, r.ts)
		b.Bytes[0] = append(b.Bytes[0], []byte(r.body))
		b.Bytes[1] = append(b.Bytes[1], r.id)
	}

	return b
}

func newRawIDEngine(t *testing.T, be backend.Backend) *recordengine.Engine {
	t.Helper()

	return recordengine.New(recordengine.Config{Schema: rawIDSchema, Backend: be, Prefix: "t/rawid"})
}

// id16 builds a 16-byte id whose bytes are all b (distinct b values ⇒ distinct ids), the width
// [simd.EqualFixed16] requires.
func id16(b byte) []byte { return bytes.Repeat([]byte{b}, 16) }

func traceIDEquals(id []byte) fetch.Condition {
	return fetch.Condition{
		Column: "trace_id",
		Match:  func(v signal.Value) bool { return bytes.Equal(v.Str(), id) },
		Equal:  &fetch.EqualMatcher{Name: "trace_id", Value: string(id)},
	}
}

func rawIDBodies(t *testing.T, e *recordengine.Engine, r fetch.Request) []string {
	t.Helper()

	it, err := e.Fetch(context.Background(), r)
	require.NoError(t, err)
	got, err := fetch.Drain(context.Background(), it)
	require.NoError(t, err)

	var out []string
	for _, b := range got {
		col, ok := b.Column("body")
		require.True(t, ok)
		for _, v := range col.Bytes {
			out = append(out, string(v))
		}
	}

	return out
}

// TestEqFastPathEndToEnd exercises the [eqFastPathCols] whole-column AVX2/stride scan through a
// real fetch: two flushed parts with disjoint ids plus a head batch, queried by exact trace_id.
func TestEqFastPathEndToEnd(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	id1, id2, idAbsent := id16(1), id16(2), id16(0xFF)

	e := newRawIDEngine(t, backend.Memory())
	ingest(t, e, mkRawIDBatch("api", rawIDRec{ts: 100, body: "a1", id: id1}, rawIDRec{ts: 110, body: "a2", id: id2}))
	require.NoError(t, e.Flush(ctx))
	ingest(t, e, mkRawIDBatch("api", rawIDRec{ts: 200, body: "b1", id: id2}))
	require.NoError(t, e.Flush(ctx))
	ingest(t, e, mkRawIDBatch("api", rawIDRec{ts: 300, body: "c1", id: id1})) // unflushed (head)

	req := func(conds ...fetch.Condition) fetch.Request {
		return fetch.Request{Start: 0, End: 1 << 60, AllConditions: true, Conditions: conds}
	}

	t.Run("matches id1 across both parts and the head", func(t *testing.T) {
		t.Parallel()

		got := rawIDBodies(t, e, req(traceIDEquals(id1)))
		assert.ElementsMatch(t, []string{"a1", "c1"}, got)
	})

	t.Run("matches id2 in the first part and the second part, not the head", func(t *testing.T) {
		t.Parallel()

		got := rawIDBodies(t, e, req(traceIDEquals(id2)))
		assert.ElementsMatch(t, []string{"a2", "b1"}, got)
	})

	t.Run("an id present nowhere returns nothing", func(t *testing.T) {
		t.Parallel()

		assert.Empty(t, rawIDBodies(t, e, req(traceIDEquals(idAbsent))))
	})

	t.Run("projecting trace_id itself still returns correct values (fast path engages via rawBlob)", func(t *testing.T) {
		t.Parallel()

		r := req(traceIDEquals(id1))
		r.Projection = []string{"body", "trace_id"}

		it, err := e.Fetch(ctx, r)
		require.NoError(t, err)
		got, err := fetch.Drain(ctx, it)
		require.NoError(t, err)

		bodiesOut := make([]string, 0, len(got))
		idsOut := make([][]byte, 0, len(got))
		for _, b := range got {
			bc, ok := b.Column("body")
			require.True(t, ok)
			bodiesOut = append(bodiesOut, stringsOf(bc.Bytes)...)

			ic, ok := b.Column("trace_id")
			require.True(t, ok)
			idsOut = append(idsOut, ic.Bytes...)
		}

		assert.ElementsMatch(t, []string{"a1", "c1"}, bodiesOut)
		for _, id := range idsOut {
			assert.Equal(t, id1, id)
		}
	})
}

func stringsOf(vals [][]byte) []string {
	out := make([]string, len(vals))
	for i, v := range vals {
		out[i] = string(v)
	}

	return out
}
