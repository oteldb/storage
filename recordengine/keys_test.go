package recordengine_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/backend"
	"github.com/oteldb/storage/recordengine"
	"github.com/oteldb/storage/signal"
)

// keyScopes drains Engine.Keys into a key→scope map for order-independent assertions.
func keyScopes(keys []recordengine.KeyInfo) map[string]recordengine.KeyScope {
	out := make(map[string]recordengine.KeyScope, len(keys))
	for _, k := range keys {
		out[string(k.Key)] = k.Scope
	}

	return out
}

// mkScopedBatch builds a one-stream batch with resource + scope attributes and per-record attrs.
func mkScopedBatch(svc string, recs ...rrec) *recordengine.Batch {
	res := signal.Resource{Attributes: signal.NewAttributes(
		signal.KeyValue{Key: []byte("service.name"), Value: signal.StringValue([]byte(svc))},
	)}
	scope := signal.Scope{
		Name:       []byte("instr"),
		Attributes: signal.NewAttributes(signal.KeyValue{Key: []byte("scope.kind"), Value: signal.StringValue([]byte("lib"))}),
	}
	series := signal.Series{Resource: res, Scope: scope}

	b := &recordengine.Batch{
		Stream:   series.Hash(),
		Identity: func() signal.Series { return series },
		Ints:     make([][]int64, 1),
		Bytes:    make([][][]byte, 3),
	}

	for _, r := range recs {
		var attrs []byte
		if r.attr[0] != "" {
			attrs = signal.NewAttributes(signal.KeyValue{Key: []byte(r.attr[0]), Value: signal.StringValue([]byte(r.attr[1]))}).AppendHashInput(nil)
		}

		b.Ts = append(b.Ts, r.ts)
		b.Ints[iSev] = append(b.Ints[iSev], r.sev)
		b.Bytes[bBody] = append(b.Bytes[bBody], []byte(r.body))
		b.Bytes[bID] = append(b.Bytes[bID], nil)
		b.Bytes[bAttrs] = append(b.Bytes[bAttrs], attrs)
	}

	return b
}

func TestKeysHeadScopes(t *testing.T) {
	t.Parallel()

	e := newEngine(t, nil)
	ingest(t, e, mkScopedBatch("api",
		rrec{ts: 100, attr: [2]string{"http.method", "GET"}},
		rrec{ts: 200, attr: [2]string{"http.status_code", "200"}},
	))

	got := keyScopes(e.Keys(0, 0))

	assert.Equal(t, recordengine.KeyScopeResource, got["service.name"])
	assert.Equal(t, recordengine.KeyScopeScope, got["scope.kind"])
	assert.Equal(t, recordengine.KeyScopeScope, got["otel.scope.name"])
	assert.Equal(t, recordengine.KeyScopeRecord, got["http.method"])
	assert.Equal(t, recordengine.KeyScopeRecord, got["http.status_code"])
	assert.NotContains(t, got, "otel.scope.version", "no version ⇒ no key")
}

func TestKeysMixedScope(t *testing.T) {
	t.Parallel()

	// "env" is a resource attribute on one stream and a record attribute on another ⇒ both bits.
	res := signal.Resource{Attributes: signal.NewAttributes(
		signal.KeyValue{Key: []byte("service.name"), Value: signal.StringValue([]byte("svc"))},
		signal.KeyValue{Key: []byte("env"), Value: signal.StringValue([]byte("prod"))},
	)}
	series := signal.Series{Resource: res}
	resBatch := &recordengine.Batch{
		Stream:   series.Hash(),
		Identity: func() signal.Series { return series },
		Ints:     make([][]int64, 1),
		Bytes:    make([][][]byte, 3),
	}
	resBatch.Ts = []int64{100}
	resBatch.Ints[iSev] = []int64{0}
	resBatch.Bytes[bBody] = [][]byte{nil}
	resBatch.Bytes[bID] = [][]byte{nil}
	resBatch.Bytes[bAttrs] = [][]byte{nil}

	e := newEngine(t, nil)
	ingest(t, e, resBatch)
	ingest(t, e, mkScopedBatch("other", rrec{ts: 150, attr: [2]string{"env", "dev"}}))

	got := keyScopes(e.Keys(0, 0))
	assert.Equal(t, recordengine.KeyScopeResource|recordengine.KeyScopeRecord, got["env"], "env spans both scopes")
}

func TestKeysWindowFilter(t *testing.T) {
	t.Parallel()

	e := newEngine(t, nil)
	ingest(t, e, mkScopedBatch("api", rrec{ts: 100, attr: [2]string{"early", "x"}}))
	ingest(t, e, mkScopedBatch("api", rrec{ts: 500, attr: [2]string{"late", "y"}}))

	got := keyScopes(e.Keys(400, 600))
	assert.Contains(t, got, "late")
	assert.NotContains(t, got, "early", "ts=100 record is outside [400,600]")
	assert.Contains(t, got, "service.name", "stream is in-window via its ts=500 record")
}

func TestKeysAcrossFlushAndMerge(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	e := newEngine(t, backend.Memory())

	ingest(t, e, mkScopedBatch("api", rrec{ts: 100, attr: [2]string{"part1.key", "a"}}))
	require.NoError(t, e.Flush(ctx))
	ingest(t, e, mkScopedBatch("api", rrec{ts: 200, attr: [2]string{"part2.key", "b"}}))
	require.NoError(t, e.Flush(ctx))
	ingest(t, e, mkScopedBatch("api", rrec{ts: 300, attr: [2]string{"head.key", "c"}}))

	got := keyScopes(e.Keys(0, 0))
	assert.Equal(t, recordengine.KeyScopeRecord, got["part1.key"], "from flushed part footer")
	assert.Equal(t, recordengine.KeyScopeRecord, got["part2.key"], "from flushed part footer")
	assert.Equal(t, recordengine.KeyScopeRecord, got["head.key"], "from the live head")
	assert.Equal(t, recordengine.KeyScopeResource, got["service.name"])

	// Merge compacts the parts; the merged part's footer must still carry both record keys.
	require.NoError(t, e.Merge(ctx, 0))
	require.Equal(t, 1, e.PartCount())

	got = keyScopes(e.Keys(0, 0))
	assert.Equal(t, recordengine.KeyScopeRecord, got["part1.key"])
	assert.Equal(t, recordengine.KeyScopeRecord, got["part2.key"])
	assert.Equal(t, recordengine.KeyScopeRecord, got["head.key"])
}

func TestKeysStatelessReload(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	be := backend.Memory()

	w := newEngine(t, be)
	ingest(t, w, mkScopedBatch("api", rrec{ts: 100, attr: [2]string{"http.method", "GET"}}))
	require.NoError(t, w.Flush(ctx))

	r := newEngine(t, be)
	require.NoError(t, r.LoadParts(ctx))

	got := keyScopes(r.Keys(0, 0))
	assert.Equal(t, recordengine.KeyScopeRecord, got["http.method"], "record key recovered from the part footer")
	assert.Equal(t, recordengine.KeyScopeResource, got["service.name"], "resource key recovered from streams.bin")
	assert.Equal(t, recordengine.KeyScopeScope, got["scope.kind"])
}

func TestKeysWindowPrunesPart(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	e := newEngine(t, backend.Memory())

	ingest(t, e, mkScopedBatch("api", rrec{ts: 100, attr: [2]string{"old.key", "a"}}))
	require.NoError(t, e.Flush(ctx))
	ingest(t, e, mkScopedBatch("api", rrec{ts: 900, attr: [2]string{"new.key", "b"}}))
	require.NoError(t, e.Flush(ctx))

	// A window over only the second part must skip the first part's footer entirely.
	got := keyScopes(e.Keys(800, 1000))
	assert.Contains(t, got, "new.key")
	assert.NotContains(t, got, "old.key", "first part is time-pruned")
}

// TestKeysPartWithoutRecordAttrs covers a flushed part carrying no record attributes: no keys.bin is
// written, and the missing footer loads as nil (the part contributes only stream-identity keys).
func TestKeysPartWithoutRecordAttrs(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	be := backend.Memory()

	w := newEngine(t, be)
	ingest(t, w, mkScopedBatch("api", rrec{ts: 100})) // no attr ⇒ empty attrs blobs
	require.NoError(t, w.Flush(ctx))

	r := newEngine(t, be)
	require.NoError(t, r.LoadParts(ctx)) // exercises loadRecordKeys' ErrNotExist path

	got := keyScopes(r.Keys(0, 0))
	assert.Equal(t, recordengine.KeyScopeResource, got["service.name"])
	assert.NotContains(t, got, "http.method")
}

func TestKeysEmpty(t *testing.T) {
	t.Parallel()

	e := newEngine(t, nil)
	assert.Empty(t, e.Keys(0, 0))
}

func TestKeysSortedDeterministic(t *testing.T) {
	t.Parallel()

	e := newEngine(t, nil)
	ingest(t, e, mkScopedBatch("api",
		rrec{ts: 100, attr: [2]string{"zeta", "1"}},
		rrec{ts: 101, attr: [2]string{"alpha", "2"}},
	))

	keys := e.Keys(0, 0)
	for i := 1; i < len(keys); i++ {
		assert.Less(t, string(keys[i-1].Key), string(keys[i].Key), "keys returned in sorted order")
	}
}
