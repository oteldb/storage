package block

import (
	"encoding/binary"
	"testing"

	"github.com/oteldb/storage/encoding/chunk"
)

// TestIDColumnEncodedSizes measures the on-disk (codec + zstd) size of the trace id columns as
// stored today (KindBytes + CodecDict) against the analytical fixed-width raw floor, to decide
// whether a raw bytes codec (or an Int64/Int128 kind) is worth adding. Run with -v to see the table.
func TestIDColumnEncodedSizes(t *testing.T) {
	t.Parallel()

	const (
		spansPerTrace = 8
		traces        = 3200
		rows          = traces * spansPerTrace // 25,600 spans
	)

	// span_id: 8 bytes, unique per span. parent_span_id: root id per non-root span (empty for roots).
	// trace_id: 16 bytes, one per trace, repeated spansPerTrace times.
	spanID := make([][]byte, rows)
	parentSpanID := make([][]byte, rows)
	traceID := make([][]byte, rows)

	for tr := range traces {
		var tid [16]byte
		binary.BigEndian.PutUint64(tid[:8], uint64(0x7f00_0000_0000_0000)|uint64(tr))
		binary.BigEndian.PutUint64(tid[8:], uint64(tr)*0x9E3779B97F4A7C15)

		var rootID [8]byte
		binary.BigEndian.PutUint64(rootID[:], uint64(tr)<<8)

		for sp := range spansPerTrace {
			i := tr*spansPerTrace + sp

			var sid [8]byte
			binary.BigEndian.PutUint64(sid[:], uint64(i)|0x0100_0000_0000_0000)

			traceID[i] = append([]byte(nil), tid[:]...)
			spanID[i] = append([]byte(nil), sid[:]...)
			if sp != 0 {
				parentSpanID[i] = append([]byte(nil), rootID[:]...)
			} else {
				parentSpanID[i] = nil
			}
		}
	}

	encSize := func(name string, codec chunk.Codec, vals [][]byte) int {
		w := NewPartWriter()
		if err := w.AddColumn(Column{Name: name, Kind: KindBytes, Codec: codec, Bytes: vals}); err != nil {
			t.Fatal(err)
		}

		built, err := w.build()
		if err != nil {
			t.Fatal(err)
		}

		return len(built.objects[0])
	}

	type row struct {
		name      string
		dict, raw int
	}

	report := []row{
		{"trace_id (16B, 8×dedup)", encSize("trace_id", chunk.CodecDict, traceID), encSize("trace_id", chunk.CodecBytesRaw, traceID)},
		{"span_id (8B, unique)", encSize("span_id", chunk.CodecDict, spanID), encSize("span_id", chunk.CodecBytesRaw, spanID)},
		{"parent_span_id (8B, root-ptr)", encSize("parent_span_id", chunk.CodecDict, parentSpanID), encSize("parent_span_id", chunk.CodecBytesRaw, parentSpanID)},
	}

	t.Logf("rows=%d", rows)
	t.Logf("%-32s %10s %10s %10s %10s", "column", "dict(B)", "dict/row", "raw(B)", "raw/row")
	for _, r := range report {
		t.Logf("%-32s %10d %10.2f %10d %10.2f",
			r.name, r.dict, float64(r.dict)/rows, r.raw, float64(r.raw)/rows)
	}

	// span_id is all-unique: the fixed-width raw codec must beat the dictionary.
	if rawSpan, dictSpan := report[1].raw, report[1].dict; rawSpan >= dictSpan {
		t.Errorf("span_id raw (%d) should be smaller than dict (%d)", rawSpan, dictSpan)
	}
	// trace_id has 8× repetition: the dictionary must beat fixed-width raw.
	if rawTrace, dictTrace := report[0].raw, report[0].dict; dictTrace >= rawTrace {
		t.Errorf("trace_id dict (%d) should be smaller than raw (%d)", dictTrace, rawTrace)
	}
}
