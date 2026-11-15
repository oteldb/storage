package obs_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/internal/obs/obstest"
)

func TestPreparedAdmissionCounters(t *testing.T) {
	t.Parallel()
	o, metrics := obstest.New(t)
	o.Admission.PreparedAttempt(t.Context(), "pressure")
	o.Admission.PreparedAttempt(t.Context(), "done")
	o.Admission.PreparedResult(t.Context(), "parallel", "retry_pressure", 7)
	o.Admission.PreparedResult(t.Context(), "ordered", "work_limit", 0)
	require.EqualValues(t, 1, metrics.Counter("storage.ingest.prepared.attempts", "outcome", "pressure"))
	require.EqualValues(t, 2, metrics.Counter("storage.ingest.prepared.batches"))
	require.EqualValues(t, 1, metrics.Counter("storage.ingest.prepared.batches", "path", "ordered", "reason", "work_limit"))
	require.EqualValues(t, 7, metrics.Counter("storage.ingest.prepared.samples", "path", "parallel"))
	require.Zero(t, metrics.Counter("storage.ingest.prepared.samples", "path", "ordered"))
}
