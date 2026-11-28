package recordengine

import (
	"math/rand/v2"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/backend/backendtest"
	"github.com/oteldb/storage/backend/file"
	"github.com/oteldb/storage/encoding/chunk"
	"github.com/oteldb/storage/query/fetch"
	"github.com/oteldb/storage/readbudget"
	"github.com/oteldb/storage/signal"
)

func TestSparseProjectionReadsMatchingGranules(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name   string
		bodies int
		ranged bool
	}{
		{name: "framed", bodies: 64, ranged: true},
		{name: "unframed", bodies: 32 << 10},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			checkSparseProjection(t, tc.bodies, tc.ranged)
		})
	}
}

func checkSparseProjection(t *testing.T, distinctBodies int, wantRanged bool) {
	t.Helper()
	ctx := t.Context()
	fs, err := file.New(t.TempDir())
	require.NoError(t, err)
	be := backendtest.NewSizedByteCounter(fs)
	schema := NewSchema(
		Column{Name: "match", Kind: KindInt64, Codec: chunk.CodecT64},
		Column{Name: "status", Kind: KindInt64, Codec: chunk.CodecT64},
		Column{Name: "body", Kind: KindBytes, Codec: chunk.CodecDict},
		Column{Name: "level", Kind: KindBytes, Codec: chunk.CodecDict},
	)
	e := New(Config{Schema: schema, Backend: be, Prefix: "sparse"})
	const count = 32 << 10
	times := make([]int64, count)
	b := testBatchColumns(schema, times)
	r := rand.New(rand.NewPCG(1, 2))
	bodies := make([][]byte, distinctBodies)
	for i := range bodies {
		body := make([]byte, 256)
		for j := range body {
			body[j] = byte(r.Uint32())
		}
		bodies[i] = body
	}
	for i := range times {
		times[i] = int64(i)
		b.Ints[0][i] = 0
		if i == 61 || i == 62 || i == count-4 {
			b.Ints[0][i] = 1
		}
		b.Ints[1][i] = int64(i * 3)
		b.Bytes[0][i] = bodies[i%len(bodies)]
		b.Bytes[1][i] = []byte("info")
	}
	_, err = e.AppendBatch(b, AppendLimits{})
	require.NoError(t, err)
	require.NoError(t, e.Flush(ctx))
	_, _, ranged := e.parts[0].reader.ScanBound(ctx, "body", 0)
	require.Equal(t, wantRanged, ranged, "fixture byte layout")
	req := fetch.Request{Start: 0, End: count - 1, AllConditions: true, Conditions: []fetch.Condition{
		{Column: "match", Match: func(v signal.Value) bool { return v.Int() == 1 }},
	}}
	be.Reset()
	wantIt, err := e.Fetch(ctx, req)
	require.NoError(t, err)
	want, err := fetch.Drain(ctx, wantIt)
	require.NoError(t, err)
	require.Len(t, want, 1)
	require.Equal(t, []int64{61, 62, count - 4}, want[0].Timestamps)
	bulkReads := be.Bytes()
	be.Reset()
	budget := readbudget.New(64 << 20)
	limited := readbudget.With(ctx, budget)
	gotIt, err := e.Fetch(limited, req)
	require.NoError(t, err)
	got, err := fetch.Drain(limited, gotIt)
	require.NoError(t, err)
	require.Equal(t, want, got)
	require.EqualValues(t, 64<<20, budget.Remaining())
	if wantRanged {
		require.Less(t, be.Bytes(), bulkReads, be.Report())
		t.Logf("backend reads for the same projected rows: %d → %d bytes", bulkReads, be.Bytes())
	}
}
