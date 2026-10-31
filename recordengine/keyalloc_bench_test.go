package recordengine

import (
	"testing"

	"github.com/oteldb/storage/signal"
)

func BenchmarkRecordKeyAllocation(b *testing.B) {
	attrs := signal.NewAttributes(
		signal.KeyValue{Key: []byte("http.method"), Value: signal.StringValue([]byte("GET"))},
		signal.KeyValue{Key: []byte("http.route"), Value: signal.StringValue([]byte("/users"))},
		signal.KeyValue{Key: []byte("http.status"), Value: signal.IntValue(200)},
		signal.KeyValue{Key: []byte("server.address"), Value: signal.StringValue([]byte("upstream"))},
		signal.KeyValue{Key: []byte("db.system"), Value: signal.StringValue([]byte("postgresql"))},
		signal.KeyValue{Key: []byte("db.statement"), Value: signal.StringValue([]byte("SELECT * FROM users"))}, //nolint:unqueryvet // Synthetic payload, never executed.
		signal.KeyValue{Key: []byte("request.id"), Value: signal.StringValue([]byte("12345"))},
		signal.KeyValue{Key: []byte("payload"), Value: signal.StringValue(make([]byte, 512))},
	)
	blob := attrs.AppendHashInput(nil)
	set := make(recordKeySet)
	var scratch signal.Attributes
	b.SetBytes(int64(len(blob)))
	b.ReportAllocs()
	for b.Loop() {
		set.add(blob, &scratch)
	}
}
