package engine_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/backend"
	"github.com/oteldb/storage/engine"
	"github.com/oteldb/storage/query/fetch"
	"github.com/oteldb/storage/signal"
	"github.com/oteldb/storage/wal"
)

//nolint:paralleltest,tparallel // Exercises one ordered engine lifecycle.
func TestReadRevision(t *testing.T) {
	t.Parallel()
	e := engine.New(engine.Config{Backend: backend.Memory(), Prefix: "metrics"})
	initial, ok := e.ReadRevision()
	require.True(t, ok)
	other, ok := engine.New(engine.Config{}).ReadRevision()
	require.True(t, ok)
	require.NotEqual(t, initial, other, "engine identity is part of the token")
	series := mkSeries("job", "api")
	before := initial
	for _, tt := range []struct {
		name   string
		mutate func() error
	}{
		{"append", func() error { _, err := e.Append(series, 100, 1); return err }},
		{"batch", func() error {
			_, err := e.AppendBatch([]signal.SeriesID{series.Hash()}, []int64{110}, []float64{2}, nil, func(int) signal.Series { return series }, engine.AppendLimits{})
			return err
		}},
		{"flush", func() error { return e.Flush(t.Context()) }},
		{"load", func() error { return e.LoadParts(t.Context()) }},
		{"load-readonly", func() error { return e.LoadPartsReadOnly(t.Context()) }},
		{"load-unclaimed", func() error { return e.LoadPartsUnclaimed(t.Context()) }},
		{"reset", func() error { return e.Reset(t.Context()) }},
	} {
		t.Run(tt.name, func(t *testing.T) {
			require.NoError(t, tt.mutate())
			after, ok := e.ReadRevision()
			require.True(t, ok)
			require.NotEqual(t, before, after)
			before = after
		})
	}
	_, err := e.Append(series, 100, 1)
	require.NoError(t, err)
	before, _ = e.ReadRevision()
	it, err := e.Fetch(t.Context(), fetch.Request{Start: 1, End: 200})
	require.NoError(t, err)
	_, err = fetch.Drain(t.Context(), it)
	require.NoError(t, err)
	after, ok := e.ReadRevision()
	require.True(t, ok)
	require.Equal(t, before, after, "lazy postings sorting is derived state")
}

func TestReadRevisionReplicatedAndReplay(t *testing.T) {
	t.Parallel()
	series := mkSeries("job", "api")
	data := walOf(t, func(w *wal.Writer) {
		require.NoError(t, w.WriteSeries(series.Hash(), series))
		require.NoError(t, w.WriteSamples(series.Hash(), []int64{100}, []float64{1}))
	})
	dir := t.TempDir()
	log, err := wal.Create(dir, 0)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, log.Close()) })
	primary := engine.New(engine.Config{WAL: log})
	before, _ := primary.ReadRevision()
	accepted, _, err := primary.ApplyPrimary(data, engine.AppendLimits{})
	require.NoError(t, err)
	after, ok := primary.ReadRevision()
	require.True(t, ok)
	require.NotEqual(t, before, after)
	secondary := engine.New(engine.Config{})
	before, _ = secondary.ReadRevision()
	require.NoError(t, secondary.ApplyReplicated(accepted))
	after, ok = secondary.ReadRevision()
	require.True(t, ok)
	require.NotEqual(t, before, after)
	require.NoError(t, log.Close())
	recovered := engine.New(engine.Config{})
	before, _ = recovered.ReadRevision()
	require.NoError(t, recovered.Replay(t.Context(), dir))
	after, ok = recovered.ReadRevision()
	require.True(t, ok)
	require.NotEqual(t, before, after)
}

func TestReadRevisionRetentionPublication(t *testing.T) {
	t.Parallel()
	e := engine.New(engine.Config{Backend: backend.Memory(), Prefix: "metrics"})
	series := mkSeries("job", "api")
	for _, ts := range []int64{100, 200} {
		_, err := e.Append(series, ts, float64(ts))
		require.NoError(t, err)
		require.NoError(t, e.Flush(t.Context()))
	}
	before, _ := e.ReadRevision()
	require.NoError(t, e.Merge(t.Context(), 150))
	after, ok := e.ReadRevision()
	require.True(t, ok)
	require.NotEqual(t, before, after)
	values := fetchAll(t, e, fetch.Request{Start: 1, End: 300})
	require.Len(t, values, 1)
	require.Equal(t, []int64{200}, values[0].Timestamps)
}
