package recordengine_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/query/fetch"
	"github.com/oteldb/storage/recordengine"
)

func TestApplyPrimaryRejectsOOOAndConverges(t *testing.T) {
	t.Parallel()

	primary := recordengine.New(recordengine.Config{Schema: testSchema, OOOWindow: 50})

	// 2000 sets newest; 900 is far below ⇒ rejected by the primary's single OOO decision.
	accepted, res, err := primary.ApplyPrimary(recordengine.EncodeWAL(
		mkBatch("api", rrec{ts: 2000, body: "a"}, rrec{ts: 900, body: "old"}),
	), recordengine.AppendLimits{})
	require.NoError(t, err)
	assert.Equal(t, 1, res.RejectedOOO)

	// A secondary applies the accepted payload verbatim and converges with the primary.
	secondary := recordengine.New(recordengine.Config{Schema: testSchema})
	require.NoError(t, secondary.ApplyReplicated(accepted))

	for name, e := range map[string]*recordengine.Engine{"primary": primary, "secondary": secondary} {
		got := fetchAll(t, e, req("api"))
		require.Lenf(t, got, 1, "%s serves the stream", name)
		assert.Equalf(t, []int64{2000}, got[0].Timestamps, "%s holds only the accepted record", name)
		assert.Equalf(t, []string{"a"}, bodies(got[0]), "%s body", name)
	}
}

func TestApplyReplicatedMultiStream(t *testing.T) {
	t.Parallel()

	e := recordengine.New(recordengine.Config{Schema: testSchema})

	// Two streams' payloads concatenated (the cluster write path framing).
	payload := append(
		recordengine.EncodeWAL(mkBatch("api", rrec{ts: 100, body: "a"})),
		recordengine.EncodeWAL(mkBatch("web", rrec{ts: 200, body: "b"}))...,
	)
	require.NoError(t, e.ApplyReplicated(payload))

	all := fetchAll(t, e, fetch.Request{Start: 0, End: 1 << 60})
	require.Len(t, all, 2, "both streams applied")
}

// FuzzRecordRoundTrip asserts a batch survives EncodeWAL → ApplyReplicated unchanged for arbitrary
// record values (and that decode never panics).
func FuzzRecordRoundTrip(f *testing.F) {
	f.Add(int64(100), int64(9), "body", "id")
	f.Add(int64(-1), int64(0), "", "")

	f.Fuzz(func(t *testing.T, ts, sev int64, body, id string) {
		e := recordengine.New(recordengine.Config{Schema: testSchema})
		require.NoError(t, e.ApplyReplicated(recordengine.EncodeWAL(
			mkBatch("api", rrec{ts: ts, sev: sev, body: body, id: id}),
		)))

		got := fetchAll(t, e, fetch.Request{Start: minI64, End: maxI64, Matchers: []fetch.Matcher{svcMatcher("api")}})
		require.Len(t, got, 1)
		assert.Equal(t, []int64{ts}, got[0].Timestamps)
		assert.Equal(t, body, bodies(got[0])[0])
	})
}

const (
	minI64 = int64(-1 << 63)
	maxI64 = int64(1<<63 - 1)
)
