package engine_test

import (
	"context"
	"slices"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/backend"
	"github.com/oteldb/storage/backend/bucketindex"
	"github.com/oteldb/storage/engine"
	"github.com/oteldb/storage/query/fetch"
)

const lostPrefix = "default/metrics"

func lostIndexKey() string { return lostPrefix + "/" + bucketindex.Object }

func prefixes(entries []bucketindex.Entry) []string {
	out := make([]string, len(entries))
	for i := range entries {
		out[i] = entries[i].Prefix
	}

	return out
}

func wantPrefixes(wants []bucketindex.Want) []string {
	out := make([]string, len(wants))
	for i := range wants {
		out[i] = wants[i].Prefix
	}

	return out
}

// diskPartKeys returns the backend objects of the part with the given id.
func diskPartKeys(ctx context.Context, t *testing.T, be backend.Backend, id string) []string {
	t.Helper()

	keys, err := be.List(ctx, lostPrefix+"/"+id+"/")
	require.NoError(t, err)

	return keys
}

// erasePart deletes every backend object of the part with the given id, the disk failure a repair
// exists for.
func erasePart(ctx context.Context, t *testing.T, be backend.Backend, id string) {
	t.Helper()

	for _, k := range diskPartKeys(ctx, t, be, id) {
		require.NoError(t, be.Delete(ctx, k))
	}
}

func newLostEngine(be backend.Backend) *engine.Engine {
	return engine.New(engine.Config{Backend: be, Prefix: lostPrefix})
}

func lostValues(t *testing.T, e *engine.Engine) []float64 {
	t.Helper()

	var out []float64

	for _, b := range fetchAll(t, e, fetch.Request{
		Matchers: []fetch.Matcher{eqMatcher("job", "api")},
		Start:    0, End: 1 << 60,
	}) {
		out = append(out, b.Values...)
	}

	slices.Sort(out)

	return out
}
