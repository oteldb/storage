package logengine_test

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/logengine"
	"github.com/oteldb/storage/query/fetch"
	"github.com/oteldb/storage/signal"
	"github.com/oteldb/storage/wal"
)

// walPayload frames a stream registration + log records the way the cluster write path does.
func walPayload(svc string, recs ...wal.LogRecord) []byte {
	s := signal.Series{Resource: signal.Resource{Attributes: signal.NewAttributes(
		signal.KeyValue{Key: []byte("service.name"), Value: signal.StringValue([]byte(svc))},
	)}}
	id := s.Hash()

	var buf bytes.Buffer
	w := wal.NewWriter(&buf)
	_ = w.WriteSeries(id, s)
	_ = w.WriteLogRecords(id, recs)

	return buf.Bytes()
}

func TestApplyPrimaryRejectsOutOfOrderAndConverges(t *testing.T) {
	t.Parallel()

	primary := logengine.New(logengine.Config{OOOWindow: 50})

	// 2000 sets newest; 900 is far below (2000-50) ⇒ rejected by the primary's single OOO decision.
	accepted, rejected, err := primary.ApplyPrimary(walPayload("api",
		wal.LogRecord{Timestamp: 2000, Body: []byte("a")},
		wal.LogRecord{Timestamp: 900, Body: []byte("old")},
	))
	require.NoError(t, err)
	assert.Equal(t, 1, rejected, "the out-of-order record is rejected")

	// A secondary applies the accepted payload verbatim and converges with the primary.
	secondary := logengine.New(logengine.Config{})
	require.NoError(t, secondary.ApplyReplicated(accepted))

	for name, e := range map[string]*logengine.Engine{"primary": primary, "secondary": secondary} {
		got := fetchAll(t, e, fetch.Request{
			Start: 0, End: 1 << 60, Matchers: []fetch.Matcher{eqMatcher("service.name", "api")},
		})
		require.Lenf(t, got, 1, "%s serves the stream", name)
		assert.Equalf(t, []int64{2000}, got[0].Timestamps, "%s holds only the accepted record", name)
		assert.Equalf(t, []string{"a"}, bodies(got[0]), "%s body", name)
	}
}

func TestApplyReplicatedAppendsVerbatim(t *testing.T) {
	t.Parallel()

	e := logengine.New(logengine.Config{})
	require.NoError(t, e.ApplyReplicated(walPayload("web",
		wal.LogRecord{Timestamp: 100, Body: []byte("x")},
		wal.LogRecord{Timestamp: 200, Body: []byte("y")},
	)))

	got := fetchAll(t, e, fetch.Request{Start: 0, End: 1 << 60})
	require.Len(t, got, 1)
	assert.Equal(t, []int64{100, 200}, got[0].Timestamps)
}
