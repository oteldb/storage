package cluster

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/recordengine"
	"github.com/oteldb/storage/signal"
	"github.com/oteldb/storage/signal/log"
	"github.com/oteldb/storage/wal"
)

// buildLogs makes one stream per scope name, each carrying n records, so streams are distinct and
// land on different shards.
func buildLogs(streams int, recordsPer int) log.Logs {
	var ld log.Logs

	rl := ld.AddResource()
	rl.Resource = signal.Resource{Attributes: signal.NewAttributes(
		signal.KeyValue{Key: []byte("service.name"), Value: signal.StringValue([]byte("api"))},
	)}

	for i := range streams {
		sl := rl.AddScope()
		sl.Scope = signal.Scope{Name: []byte{byte('a' + i)}}

		for j := range recordsPer {
			r := sl.AddRecord()
			r.Timestamp = int64(1_000 + j)
			r.Body = []byte("hello")
			r.SeverityNumber = 9
		}
	}

	return ld
}

func logProjector(ld log.Logs) RecordProjector {
	return func(emit func(*recordengine.Batch)) int { return log.Project(ld, emit) }
}

// countRecords replays a framed payload and reports how many streams and records it carries.
func countRecords(t *testing.T, payload []byte) (streams, records int) {
	t.Helper()

	require.NoError(t, wal.Replay(payload, wal.Handlers{
		OnSeries: func(signal.SeriesID, signal.Series) error {
			streams++

			return nil
		},
		OnRecords: func(signal.SeriesID, []byte) error {
			records++

			return nil
		},
	}))

	return streams, records
}

func TestFrameRecordsSingleShard(t *testing.T) {
	t.Parallel()

	f := FrameRecords(logProjector(buildLogs(3, 2)), 1, nil, nil)

	assert.Equal(t, 6, f.Emitted)
	assert.Zero(t, f.Shed)

	require.Len(t, f.Shards, 1)
	require.Contains(t, f.Shards, DefaultTenant)

	streams, _ := countRecords(t, f.Shards[DefaultTenant])
	assert.Equal(t, 3, streams, "every stream registered once")
}

func TestFrameRecordsKeepsAStreamWhole(t *testing.T) {
	t.Parallel()

	const shards = 4

	f := FrameRecords(logProjector(buildLogs(8, 3)), shards, nil, nil)
	assert.Equal(t, 24, f.Emitted)

	seen := 0

	for key, payload := range f.Shards {
		assert.Equal(t, DefaultTenant, TenantOfShard(key))

		// A stream's records must all be in the shard its id maps to: splitting one across
		// primaries would scatter a log stream over the ring.
		require.NoError(t, wal.Replay(payload, wal.Handlers{
			OnSeries: func(id signal.SeriesID, _ signal.Series) error {
				seen++
				assert.Equal(t, key, ShardKeyOf(DefaultTenant, ShardOf(id, shards), shards))

				return nil
			},
		}))
	}

	assert.Equal(t, 8, seen, "every stream framed exactly once")
}

func TestFrameRecordsTenantAndAdmit(t *testing.T) {
	t.Parallel()

	f := FrameRecords(logProjector(buildLogs(2, 1)), 1,
		func(signal.Resource, signal.Scope) signal.TenantID { return "acme" }, nil)
	require.Contains(t, f.Shards, signal.TenantID("acme"))

	// A shed stream is counted but never framed.
	f = FrameRecords(logProjector(buildLogs(2, 3)), 1, nil,
		func(signal.TenantID, *recordengine.Batch) bool { return false })

	assert.Equal(t, 6, f.Emitted)
	assert.Equal(t, 6, f.Shed)
	assert.Empty(t, f.Shards)
}

func TestFrameRecordsEmpty(t *testing.T) {
	t.Parallel()

	f := FrameRecords(logProjector(log.Logs{}), 4, nil, nil)

	assert.Zero(t, f.Emitted)
	assert.Empty(t, f.Shards)
}
