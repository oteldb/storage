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
	r.Body().SetInt(42)

	var out log.Logs
	require.Equal(t, 0, AppendLogs(&out, ld))

	rec := out.Resources[0].Scopes[0].Records[0]
	assert.Equal(t, []byte("42"), rec.Body)
	assert.Nil(t, rec.TraceID)
	assert.Nil(t, rec.SpanID)
}
