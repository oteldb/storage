package pdataconv

import (
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/plog"

	"github.com/oteldb/storage/signal"
	"github.com/oteldb/storage/signal/log"
)

// AppendLogs converts an OTLP logs batch into dst, reusing dst's retained capacity (call
// [log.Logs.Reset] or use [log.GetLogs] for a recycled batch). Every record is representable, so
// dropped is always 0; it is returned for symmetry with [AppendMetrics]. Non-string record bodies
// are rendered to their textual form, since the internal model stores a body as text bytes.
//
//nolint:dupl // per-signal OTLP converter; identical resource/scope walk, types differ
func AppendLogs(dst *log.Logs, ld plog.Logs) (dropped int) {
	rls := ld.ResourceLogs()
	for i := range rls.Len() {
		srl := rls.At(i)

		rl := dst.AddResource()
		rl.Resource = signal.Resource{
			SchemaURL:  []byte(srl.SchemaUrl()),
			Attributes: convertMap(srl.Resource().Attributes()),
		}

		sls := srl.ScopeLogs()
		for j := range sls.Len() {
			ssl := sls.At(j)

			sl := rl.AddScope()
			sl.Scope = signal.Scope{
				Name:       []byte(ssl.Scope().Name()),
				Version:    []byte(ssl.Scope().Version()),
				SchemaURL:  []byte(ssl.SchemaUrl()),
				Attributes: convertMap(ssl.Scope().Attributes()),
			}

			records := ssl.LogRecords()
			for k := range records.Len() {
				appendRecord(sl, records.At(k))
			}
		}
	}

	return dropped
}

func appendRecord(sl *log.ScopeLogs, r plog.LogRecord) {
	rec := sl.AddRecord()
	rec.Timestamp = int64(r.Timestamp())
	rec.ObservedTimestamp = int64(r.ObservedTimestamp())
	rec.SeverityNumber = int32(r.SeverityNumber())
	rec.SeverityText = []byte(r.SeverityText())
	rec.Body = bodyBytes(r.Body())
	rec.TraceID = traceIDBytes(r.TraceID())
	rec.SpanID = spanIDBytes(r.SpanID())
	rec.Flags = uint32(r.Flags())
	rec.Dropped = r.DroppedAttributesCount()
	rec.Attributes = convertMap(r.Attributes())
}

// bodyBytes renders a log record body to text bytes: a string body is copied verbatim, any other
// type is rendered via its OTLP string form.
func bodyBytes(v pcommon.Value) []byte {
	if v.Type() == pcommon.ValueTypeStr {
		return []byte(v.Str())
	}

	return []byte(v.AsString())
}
