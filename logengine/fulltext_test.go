package logengine_test

import (
	"bytes"
	"context"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/backend"
	"github.com/oteldb/storage/index/bloom"
	"github.com/oteldb/storage/logengine"
	"github.com/oteldb/storage/query/fetch"
	"github.com/oteldb/storage/signal"
)

// bodyFullText is a full-text body condition: an exact substring Match plus the bloom Tokens that
// let the engine prune parts whose bloom proves a token absent.
func bodyFullText(term string) fetch.Condition {
	want := []byte(term)

	return fetch.Condition{
		Column: "body",
		Match:  func(v signal.Value) bool { return bytes.Contains(v.Str(), want) },
		Tokens: bloom.Tokenize(nil, want),
	}
}

func TestFullTextAcrossPartsAndPrune(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	e := logengine.New(logengine.Config{Backend: backend.Memory(), Prefix: "t/logs"})

	// Two parts with disjoint vocabularies, plus a head record.
	ingest(t, e, richStream("api", richRec{100, 9, "connection refused alpha", "x"}))
	require.NoError(t, e.Flush(ctx))
	ingest(t, e, richStream("api", richRec{200, 9, "timeout beta", "y"}))
	require.NoError(t, e.Flush(ctx))
	ingest(t, e, richStream("api", richRec{300, 9, "alpha in head", "z"}))
	require.Equal(t, 2, e.PartCount())

	// "alpha" is in part 1 and the head, not part 2 (which is bloom-pruned). The per-row Match
	// re-checks, so the result is exact.
	got := fetchAll(t, e, condReq("api", bodyFullText("alpha")))
	require.Len(t, got, 1)
	assert.Equal(t, []string{"connection refused alpha", "alpha in head"}, bodies(got[0]))

	// A token in no part nor head ⇒ every part pruned, head scanned, nothing matches.
	none := fetchAll(t, e, condReq("api", bodyFullText("nonexistentterm")))
	assert.Empty(t, none)
}

func TestFullTextMultiTokenAnd(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	e := logengine.New(logengine.Config{Backend: backend.Memory(), Prefix: "t/logs"})

	ingest(t, e, richStream("api",
		richRec{100, 9, "user alice login ok", "a"},
		richRec{200, 9, "user bob login fail", "b"},
		richRec{300, 9, "alice logout", "a"},
	))
	require.NoError(t, e.Flush(ctx))

	// "alice login" tokenizes to {alice, login}; only the first record contains both.
	got := fetchAll(t, e, condReq("api", bodyFullText("alice login")))
	require.Len(t, got, 1)
	assert.Equal(t, []string{"user alice login ok"}, bodies(got[0]))
}

func TestFullTextSurvivesBloomLoss(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	be := backend.Memory()
	e := logengine.New(logengine.Config{Backend: be, Prefix: "t/logs"})

	ingest(t, e, richStream("api", richRec{100, 9, "alpha record", "x"}))
	require.NoError(t, e.Flush(ctx))

	// Delete the bloom object, then reload: a part with no bloom is always scanned (never pruned),
	// so queries still return correct results.
	keys, err := be.List(ctx, "t/logs")
	require.NoError(t, err)
	for _, k := range keys {
		if strings.HasSuffix(k, "bloom-body.bin") {
			require.NoError(t, be.Delete(ctx, k))
		}
	}

	reader := logengine.New(logengine.Config{Backend: be, Prefix: "t/logs"})
	require.NoError(t, reader.LoadParts(ctx))

	got := fetchAll(t, reader, condReq("api", bodyFullText("alpha")))
	require.Len(t, got, 1)
	assert.Equal(t, []string{"alpha record"}, bodies(got[0]))
}
