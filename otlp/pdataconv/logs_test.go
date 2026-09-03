package pdataconv

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/plog"

	"github.com/oteldb/storage/signal/log"
)

func TestAppendLogs(t *testing.T) {
	t.Parallel()

	ld := plog.NewLogs()
	rl := ld.ResourceLogs().AppendEmpty()
	rl.SetSchemaUrl("https://schema/res")
	rl.Resource().Attributes().PutStr("service.name", "api")
	sl := rl.ScopeLogs().AppendEmpty()
	sl.Scope().SetName("lib")

	traceID := pcommon.TraceID([16]byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16})
	spanID := pcommon.SpanID([8]byte{1, 2, 3, 4, 5, 6, 7, 8})

	r := sl.LogRecords().AppendEmpty()
	r.SetTimestamp(pcommon.Timestamp(1000))
	r.SetObservedTimestamp(pcommon.Timestamp(1100))
	r.SetSeverityNumber(plog.SeverityNumberInfo)
	r.SetSeverityText("INFO")
	r.Body().SetStr("hello")
	r.SetTraceID(traceID)
	r.SetSpanID(spanID)
	r.SetFlags(plog.LogRecordFlags(3))
	r.Attributes().PutStr("k", "v")

	var out log.Logs
	require.Equal(t, 0, AppendLogs(&out, ld))

	require.Len(t, out.Resources, 1)
	res := out.Resources[0]
	assert.Equal(t, []byte("https://schema/res"), res.Resource.SchemaURL)
	require.Len(t, res.Scopes, 1)
	require.Len(t, res.Scopes[0].Records, 1)

	rec := res.Scopes[0].Records[0]
	assert.Equal(t, int64(1000), rec.Timestamp)
	assert.Equal(t, int64(1100), rec.ObservedTimestamp)
	assert.Equal(t, int32(plog.SeverityNumberInfo), rec.SeverityNumber)
	assert.Equal(t, []byte("INFO"), rec.SeverityText)
	assert.Equal(t, []byte("hello"), rec.Body)
	assert.Equal(t, traceID[:], rec.TraceID)
	assert.Equal(t, spanID[:], rec.SpanID)
	assert.Equal(t, uint32(3), rec.Flags)
	av, _ := rec.Attributes.Get([]byte("k"))
	assert.Equal(t, []byte("v"), av.Str())
}

func TestAppendLogsNonStringBody(t *testing.T) {
	t.Parallel()

	ld := plog.NewLogs()
	r := ld.ResourceLogs().AppendEmpty().ScopeLogs().AppendEmpty().LogRecords().AppendEmpty()
	r.SetObservedTimestamp(pcommon.Timestamp(1100)) // a record needs a time to be representable
	r.Body().SetInt(42)

	var out log.Logs
	require.Equal(t, 0, AppendLogs(&out, ld))

	rec := out.Resources[0].Scopes[0].Records[0]
	assert.Equal(t, []byte("42"), rec.Body)
	assert.Nil(t, rec.TraceID)
	assert.Nil(t, rec.SpanID)
}

// TestAppendLogsObservedTimestampFallback is #485: time_unix_nano is optional in OTLP, and a record
// carrying only an observed time must not land at the unix epoch — no query window covers it and
// retention drops the part as ancient, silently.
func TestAppendLogsObservedTimestampFallback(t *testing.T) {
	t.Parallel()

	const observed = 1788436458053287901

	tests := []struct {
		name        string
		ts          int64
		observed    int64
		wantDropped int
		wantTs      int64
	}{
		{"event time wins", 1000, observed, 0, 1000},
		{"observed fills in an unset event time", 0, observed, 0, observed},
		{"neither is refused, not stored at the epoch", 0, 0, 1, 0},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			ld := plog.NewLogs()
			sl := ld.ResourceLogs().AppendEmpty().ScopeLogs().AppendEmpty()
			r := sl.LogRecords().AppendEmpty()
			r.SetTimestamp(pcommon.Timestamp(tt.ts))
			r.SetObservedTimestamp(pcommon.Timestamp(tt.observed))
			r.Body().SetStr("hello")

			var out log.Logs
			assert.Equal(t, tt.wantDropped, AppendLogs(&out, ld))

			records := out.Resources[0].Scopes[0].Records
			if tt.wantDropped > 0 {
				assert.Empty(t, records, "a record with no usable time is refused, not stored")

				return
			}

			require.Len(t, records, 1)
			assert.Equal(t, tt.wantTs, records[0].Timestamp)
			assert.Equal(t, tt.observed, records[0].ObservedTimestamp,
				"the observed time is preserved whether or not it was used")
		})
	}
}
