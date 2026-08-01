package recordengine_test

import (
	"bytes"
	"context"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/backend"
	"github.com/oteldb/storage/encoding/chunk"
	"github.com/oteldb/storage/query/fetch"
	"github.com/oteldb/storage/query/profile"
	"github.com/oteldb/storage/recordengine"
	"github.com/oteldb/storage/signal"
)

// gramSchema is testSchema with a gram index on the body column, which is the shape a log signal
// opting into substring pruning would declare.
var gramSchema = recordengine.NewSchema(
	recordengine.Column{Name: "sev", Kind: recordengine.KindInt64, Codec: chunk.CodecT64},
	recordengine.Column{
		Name: "body", Kind: recordengine.KindBytes, Codec: chunk.CodecDict,
		Bloom: recordengine.BloomFullText, Grams: true,
	},
	recordengine.Column{Name: "id", Kind: recordengine.KindBytes, Codec: chunk.CodecDict, Bloom: recordengine.BloomEquality},
	recordengine.Column{Name: "attrs", Kind: recordengine.KindBytes, Codec: chunk.CodecDict, Bloom: recordengine.BloomAttrs},
)

// containsCond is the condition a language lowers an unanchored substring filter to: the Match
// predicate the engine re-checks per row, plus the raw literal as a pruning hint.
func containsCond(column, lit string) fetch.Condition {
	want := []byte(lit)

	return fetch.Condition{
		Column:     column,
		Match:      func(v signal.Value) bool { return bytes.Contains(v.Str(), want) },
		Substrings: [][]byte{want},
	}
}

// fetchWithProfile drains a request and returns the batches plus the fetch's counters.
func fetchWithProfile(t *testing.T, e *recordengine.Engine, r fetch.Request) ([]*fetch.Batch, map[string]int64) {
	t.Helper()

	ctx, c := profile.WithCollector(context.Background())

	it, err := e.Fetch(ctx, r)
	require.NoError(t, err)

	got, err := fetch.Drain(ctx, it)
	require.NoError(t, err)

	counters := map[string]int64{}

	var walk func(*profile.Node)
	walk = func(n *profile.Node) {
		for k, v := range n.Counters {
			counters[k] += v
		}

		for _, ch := range n.Children {
			walk(ch)
		}
	}
	walk(c.Root())

	return got, counters
}

func TestGramSubstringPrunesParts(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	e := recordengine.New(recordengine.Config{Schema: gramSchema, Backend: backend.Memory(), Prefix: "t/recs"})

	// Three parts, each holding one distinctive identifier glued into a larger token — the case a
	// token bloom cannot prune, because the literal has no whole token of its own.
	ids := []string{
		"deadbeefcafebabe0123456789abcdef",
		"0123456789abcdefdeadbeefcafebabe",
		"fedcba9876543210fedcba9876543210",
	}

	for i, id := range ids {
		ingest(t, e, mkBatch("api", rrec{ts: int64(i*100 + 1), sev: 9, body: "req trace[" + id + "] handled"}))
		require.NoError(t, e.Flush(ctx))
	}

	t.Run("present literal is found", func(t *testing.T) {
		t.Parallel()

		got, counters := fetchWithProfile(t, e, req("api", containsCond("body", ids[1])))

		found := make([]string, 0, len(got))
		for _, b := range got {
			found = append(found, bodies(b)...)
		}

		require.Len(t, found, 1)
		assert.Contains(t, found[0], ids[1])

		// The two parts that cannot hold the literal are pruned before any column is read.
		assert.Equal(t, int64(2), counters["parts_pruned_gram"])
	})

	t.Run("absent literal prunes every part", func(t *testing.T) {
		t.Parallel()

		got, counters := fetchWithProfile(t, e,
			req("api", containsCond("body", "00000000000000000000badf00d00000")))

		assert.Empty(t, got)
		assert.Equal(t, int64(3), counters["parts_pruned_gram"])
	})

	t.Run("short literal prunes nothing but stays correct", func(t *testing.T) {
		t.Parallel()

		// Under gramMinLen: no grams, so no pruning — the per-row Match still filters.
		got, counters := fetchWithProfile(t, e, req("api", containsCond("body", "zz")))

		assert.Empty(t, got)
		assert.Zero(t, counters["parts_pruned_gram"])
	})
}

// TestGramSurvivesMerge pins that the gram sidecar is rebuilt by the merge path, not just the flush
// path — a merged part that lost its sidecar would silently stop pruning.
func TestGramSurvivesMerge(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	be := backend.Memory()
	e := recordengine.New(recordengine.Config{Schema: gramSchema, Backend: be, Prefix: "t/recs"})

	for i := range 4 {
		ingest(t, e, mkBatch("api", rrec{
			ts: int64(i*100 + 1), sev: 9,
			body: "req trace[deadbeefcafebabe012345678" + string(rune('a'+i)) + "] handled",
		}))
		require.NoError(t, e.Flush(ctx))
	}

	require.NoError(t, e.Merge(ctx, 0))

	keys, err := be.List(ctx, "t/recs")
	require.NoError(t, err)

	var sidecars int

	for _, k := range keys {
		if strings.Contains(k, "/grams-body.bin") {
			sidecars++
		}
	}

	require.NotZero(t, sidecars, "merged parts carry a gram sidecar")

	got, counters := fetchWithProfile(t, e, req("api", containsCond("body", "deadbeefcafebabe0123456789")))
	assert.Empty(t, got, "no record holds that literal")
	assert.NotZero(t, counters["parts_pruned_gram"], "the merged part still prunes")
}

// TestGramOptOut pins that a schema without Grams writes no sidecar and ignores substring hints, so
// the feature costs nothing until a column asks for it.
func TestGramOptOut(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	be := backend.Memory()
	e := newEngine(t, be) // testSchema: no gram columns

	ingest(t, e, mkBatch("api", rrec{ts: 1, sev: 9, body: "req trace[deadbeefcafebabe0123456789abcdef] handled"}))
	require.NoError(t, e.Flush(ctx))

	keys, err := be.List(ctx, "t/recs")
	require.NoError(t, err)

	for _, k := range keys {
		assert.NotContains(t, k, "/grams-", "no gram sidecar without Column.Grams")
	}

	// The hint is simply ignored: nothing prunes, and the per-row Match still decides.
	got, counters := fetchWithProfile(t, e, req("api", containsCond("body", "00000000000000000000badf00d00000")))
	assert.Empty(t, got)
	assert.Zero(t, counters["parts_pruned_gram"])
}
