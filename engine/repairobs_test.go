package engine_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/attribute"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"

	"github.com/oteldb/storage/backend"
	"github.com/oteldb/storage/backend/bucketindex"
	"github.com/oteldb/storage/engine"
	"github.com/oteldb/storage/internal/obs"
)

func newObservedRepairEngine(
	t *testing.T, be backend.Backend, r engine.PartFetcher,
) (*engine.Engine, *sdkmetric.ManualReader) {
	t.Helper()

	reader := sdkmetric.NewManualReader()

	o, err := obs.New(obs.Config{MeterProvider: sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))})
	require.NoError(t, err)

	return engine.New(engine.Config{Backend: be, Prefix: "default/metrics", Repair: r, Obs: o}), reader
}

// observedRepair reads the repair instruments back into the shape of [engine.RepairStats], so the
// two operator surfaces compare field by field.
func observedRepair(t *testing.T, reader *sdkmetric.ManualReader) engine.RepairStats {
	t.Helper()

	var rm metricdata.ResourceMetrics
	require.NoError(t, reader.Collect(context.Background(), &rm))

	var s engine.RepairStats

	attempts := map[string]*int64{
		"local": &s.Local, "fetched": &s.Fetched, "absent": &s.Unsatisfiable,
		"incomplete": &s.Incomplete, "failed": &s.Failed,
	}

	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			sum, ok := m.Data.(metricdata.Sum[int64])
			if !ok {
				continue
			}

			for _, dp := range sum.DataPoints {
				switch m.Name {
				case "storage.repair.attempts":
					result, _ := dp.Attributes.Value(attribute.Key("result"))
					dst, known := attempts[result.AsString()]
					require.Truef(t, known, "unexpected repair result %q", result.AsString())

					*dst += dp.Value
				case "storage.repair.lost_parts":
					s.Lost += dp.Value
				case "storage.repair.revoked_holes":
					s.Revoked += dp.Value
				}
			}
		}
	}

	return s
}

// TestRepairFailedCommitObservesNoLoss: a pass that concluded a part lost but could not commit the
// hole acknowledged nothing, so the monotone lost_parts counter must not move.
func TestRepairFailedCommitObservesNoLoss(t *testing.T) {
	t.Parallel()

	be := &rejectIndexWrites{Backend: backend.Memory()}
	e, reader := newObservedRepairEngine(t, be, answerAlways(bucketindex.WantAbsent, nil))

	lost := loseFirstOfTwo(t, e, be)

	mergeTimes(t, e, 2)

	be.armed.Store(true)
	mergeTimes(t, e, 1)
	be.armed.Store(false)

	assert.Equal(t, []string{lost}, e.WantPrefixes(), "the want is still owed")
	assert.Empty(t, e.Holes())
	assert.Zero(t, e.LostParts())
	assert.Zero(t, e.RepairStats().Lost)
	assert.Zero(t, observedRepair(t, reader).Lost, "no hole was committed, so no loss was acknowledged")
	assert.Equal(t, e.RepairStats(), observedRepair(t, reader))

	mergeTimes(t, e, 3)

	require.Len(t, e.Holes(), 1)
	assert.Equal(t, int64(1), e.RepairStats().Lost)
	assert.Equal(t, e.RepairStats(), observedRepair(t, reader))
}

// TestRepairUnopenablePartObservedAsFailed: a fetched part that will not open is a failure on both
// surfaces, not a fetch on one and a failure on the other.
func TestRepairUnopenablePartObservedAsFailed(t *testing.T) {
	t.Parallel()

	const phantom = "default/metrics/00000000000000000000000000"

	be := backend.Memory()
	f := &fakeFetcher{answer: func(w bucketindex.Want) (bucketindex.Entry, bucketindex.WantOutcome, error) {
		return bucketindex.Entry{Prefix: phantom, MinTime: 100, MaxTime: 100, Blocks: w.Blocks}, bucketindex.WantSatisfied, nil
	}}
	e, reader := newObservedRepairEngine(t, be, f)

	lost := loseOneOfThree(t, e, be)

	require.NoError(t, e.Merge(context.Background(), 0))

	assert.Equal(t, []string{lost}, e.WantPrefixes())
	assert.Zero(t, e.RepairStats().Fetched)
	assert.Equal(t, int64(1), e.RepairStats().Failed)
	assert.Equal(t, e.RepairStats(), observedRepair(t, reader))
}
