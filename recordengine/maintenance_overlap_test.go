package recordengine_test

import (
	"context"
	"strconv"
	"strings"
	"sync"
	"testing"
	"testing/synctest"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/backend"
	"github.com/oteldb/storage/backend/faultbackend"
	"github.com/oteldb/storage/internal/memlimit"
	"github.com/oteldb/storage/query/fetch"
	"github.com/oteldb/storage/recordengine"
)

func TestFlushDuringMergePreservesRecords(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		be := faultbackend.Wrap(backend.Memory())
		pool := memlimit.NewPool(256 << 20)
		e := recordengine.New(recordengine.Config{
			Schema: testSchema, Backend: be, Prefix: "records", MergeMemoryBytes: 64 << 20,
			FlushAdmission: pool.Acquire,
			MergeAdmission: func(ctx context.Context, bytes int64, _ bool) (func(), bool, error) {
				release, err := pool.Acquire(ctx, bytes)
				return release, err == nil, err
			},
		})
		for round := range 2 {
			for row := range 16 {
				_, err := e.AppendBatch(mkBatch("api", rrec{ts: int64(round*16 + row + 1), body: strconv.Itoa(row)}), recordengine.AppendLimits{})
				require.NoError(t, err)
			}
			require.NoError(t, e.Flush(t.Context()))
		}
		gate := faultbackend.NewGate()
		release := sync.OnceFunc(gate.Release)
		defer release()
		be.Add(gate.Rule(faultbackend.Read, func(op faultbackend.Op) bool { return strings.Contains(op.Key, "/c/") }))
		merged := make(chan error, 1)
		go func() { merged <- e.MergeWith(t.Context(), recordengine.MergeOptions{Force: true}) }()
		gate.Await(t)
		be.Reset()
		_, err := e.AppendBatch(mkBatch("api", rrec{ts: 100, body: "last"}), recordengine.AppendLimits{})
		require.NoError(t, err)
		flushed := make(chan error, 1)
		go func() { flushed <- e.Flush(t.Context()) }()
		synctest.Wait()
		require.NoError(t, <-flushed)
		assert.Equal(t, 3, e.PartCount())
		release()
		require.NoError(t, <-merged)
		got := fetchAll(t, e, fetch.Request{Start: 0, End: 1000})
		require.Len(t, got, 1)
		assert.Len(t, got[0].Timestamps, 33, "record multiplicity survives both publications")
		assert.Equal(t, 2, e.PartCount())
	})
}
