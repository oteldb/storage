package seriesindex_test

import (
	"bytes"
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/backend"
	"github.com/oteldb/storage/engine"
	"github.com/oteldb/storage/internal/reproduce"
	"github.com/oteldb/storage/internal/seriesindex"
	"github.com/oteldb/storage/signal"
)

func TestCopiedIndexCannotChangeAdmission(t *testing.T) {
	reproduce.Unfixed(t, 661, "an index copied from a different part with the same IDs changes OOO admission")
	t.Parallel()
	b := backend.Memory()
	identity := signal.Series{Attributes: signal.NewAttributes(signal.KeyValue{Key: []byte("metric"), Value: signal.StringValue([]byte("m"))})}
	write := func(prefix string, ts int64, value float64) string {
		e := engine.New(engine.Config{Backend: b, Prefix: prefix, DiskSeries: true, AggregateStats: true})
		t.Cleanup(func() { require.NoError(t, e.Close(context.Background())) })
		accepted, err := e.Append(identity, ts, value)
		require.NoError(t, err)
		require.True(t, accepted)
		require.NoError(t, e.Flush(t.Context()))
		parts := e.Parts()
		require.Len(t, parts, 1)
		return parts[0].ID
	}
	first := write("first", 10, 1)
	second := write("second", 100, 999)
	other, err := b.Read(t.Context(), second+"/series-index")
	require.NoError(t, err)
	require.NoError(t, b.Write(t.Context(), first+"/series-index", other))
	reader := engine.New(engine.Config{Backend: b, Prefix: "first", DiskSeries: true, OOOWindow: 10})
	t.Cleanup(func() { require.NoError(t, reader.Close(context.Background())) })
	err = reader.LoadPartsReadOnly(t.Context())
	if err != nil {
		return
	}
	idx, err := seriesindex.Open(t.Context(), b, first+"/series-index")
	require.NoError(t, err)
	_, meta, found, err := idx.Lookup(t.Context(), identity.Hash())
	require.NoError(t, err)
	require.True(t, found)
	t.Logf("copied index accepted over timestamp-10 part: Covered=%d, Stats.Sum=%v", meta.Covered, meta.Stats.Sum)
	accepted, err := reader.Append(identity, 20, 2)
	require.NoError(t, err)
	require.True(t, accepted, "the only stored sample is at 10; copied coverage 100 must not reject a valid append at 20")
}

func TestCopiedSameManifestIndexCannotChangeStats(t *testing.T) {
	reproduce.Unfixed(t, 661, "identical parent manifests permit swapping a different part's per-series stats")
	t.Parallel()
	b := backend.Memory()
	a := signal.Series{Attributes: signal.NewAttributes(signal.KeyValue{Key: []byte("id"), Value: signal.IntValue(1)})}
	other := signal.Series{Attributes: signal.NewAttributes(signal.KeyValue{Key: []byte("id"), Value: signal.IntValue(2)})}
	write := func(prefix string, swap bool) string {
		e := engine.New(engine.Config{Backend: b, Prefix: prefix, DiskSeries: true, AggregateStats: true})
		t.Cleanup(func() { require.NoError(t, e.Close(context.Background())) })
		first, second := int64(10), int64(20)
		if swap {
			first, second = second, first
		}
		_, err := e.Append(a, 10, float64(first))
		require.NoError(t, err)
		_, err = e.Append(other, 20, float64(second))
		require.NoError(t, err)
		require.NoError(t, e.Flush(t.Context()))
		parts := e.Parts()
		require.Len(t, parts, 1)
		return parts[0].ID
	}
	first := write("coverage-first", false)
	second := write("coverage-second", true)
	firstManifest, err := b.Read(t.Context(), first+"/manifest")
	require.NoError(t, err)
	secondManifest, err := b.Read(t.Context(), second+"/manifest")
	require.NoError(t, err)

	t.Logf("different sample assignments have identical manifest bytes: %v", bytes.Equal(firstManifest, secondManifest))
	copied, err := b.Read(t.Context(), second+"/series-index")
	require.NoError(t, err)
	require.NoError(t, b.Write(t.Context(), first+"/series-index", copied))
	reader := engine.New(engine.Config{Backend: b, Prefix: "coverage-first", DiskSeries: true})
	t.Cleanup(func() { require.NoError(t, reader.Close(context.Background())) })
	if err := reader.LoadPartsReadOnly(t.Context()); err != nil {
		return
	}
	idx, err := seriesindex.Open(t.Context(), b, first+"/series-index")
	require.NoError(t, err)
	_, meta, found, err := idx.Lookup(t.Context(), a.Hash())
	require.NoError(t, err)
	require.True(t, found)
	require.True(t, meta.HasStats)
	require.Equal(t, float64(10), meta.Stats.Sum, "this part stores A=10; a matching raw manifest hash must not authorize copied A=20 stats")
}
