package pdataconv

import (
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/plog"

	"github.com/oteldb/storage/signal"
	"github.com/oteldb/storage/signal/log"
)

// AppendLogs converts an OTLP logs batch into dst, reusing dst's retained capacity (call
// [log.Logs.Reset] or use [log.GetLogs] for a recycled batch). dropped counts the records carrying
// no usable time at all (see [appendRecord]). Non-string record bodies are rendered to their textual
// form, since the internal model stores a body as text bytes.
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
				if !appendRecord(sl, records.At(k)) {
					dropped++
				}
			}
		}
	}

	return dropped
}

// appendRecord converts one OTLP record, reporting whether it was representable. time_unix_nano is
// optional in OTLP — a receiver tailing files or journald with no timestamp parser leaves it unset —
// so an unset event time falls back to the observed time, as the spec prescribes. Without the
// fallback such a record sorts at the unix epoch, which no query window covers and which retention
// then drops as ancient: silent loss of a whole class of ordinary logs. A record with neither time
// has nothing to sort or query by and is refused here, where the caller can still report it.
func appendRecord(sl *log.ScopeLogs, r plog.LogRecord) bool {
	ts, observed := int64(r.Timestamp()), int64(r.ObservedTimestamp())
	if ts == 0 {
		ts = observed
	}

	if ts == 0 {
		return false
	}

	rec := sl.AddRecord()
	rec.Timestamp = ts
	rec.ObservedTimestamp = observed
	rec.SeverityNumber = int32(r.SeverityNumber())
	rec.SeverityText = []byte(r.SeverityText())
	rec.Body = bodyBytes(r.Body())
	rec.TraceID = traceIDBytes(r.TraceID())
	rec.SpanID = spanIDBytes(r.SpanID())
	rec.Flags = uint32(r.Flags())
	rec.Dropped = r.DroppedAttributesCount()
	rec.Attributes = convertMap(r.Attributes())

	return true
}

// bodyBytes renders a log record body to text bytes: a string body is copied verbatim, any other
// type is rendered via its OTLP string form.
func bodyBytes(v pcommon.Value) []byte {
	if v.Type() == pcommon.ValueTypeStr {
		return []byte(v.Str())
	}

	return []byte(v.AsString())
}
