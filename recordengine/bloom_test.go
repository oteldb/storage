package recordengine

import (
	"bytes"
	"strconv"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/index/bloom"
	"github.com/oteldb/storage/signal"
)

// buildColumnBloomReference is the single-pass build that [buildColumnBloom] replaced: it
// materializes every token, sketches the materialized set to size the filter, and hashes them in a
// second loop. It is kept here
// purely as the oracle for the two-pass build — the encoded filter must be byte-identical, since
// blooms are persisted per part and an old part must stay readable by the new code (and vice
// versa).
func buildColumnBloomReference(mode BloomMode, values *byteCol) []byte {
	var (
		tokens  [][]byte
		words   [][]byte
		scratch []byte
	)

	rows := values.rows()

	switch mode {
	case BloomFullText:
		for i := range rows {
			tokens = bloom.Tokenize(tokens, values.at(i))
		}
	case BloomEquality:
		for i := range rows {
			if v := values.at(i); len(v) > 0 {
				tokens = append(tokens, v)
			}
		}
	case BloomAttrs:
		for i := range rows {
			a, _, err := signal.DecodeAttributes(values.at(i))
			if err != nil {
				continue
			}

			for i := range a {
				scratch = a[i].Value.AppendText(scratch[:0])
				tokens = append(tokens, attrToken(a[i].Key, scratch))

				words = bloom.Tokenize(words[:0], scratch)
				for _, w := range words {
					tokens = append(tokens, attrToken(a[i].Key, w))
				}
			}
		}
	case BloomNone:
		return nil
	}

	n := len(tokens)
	if len(values.data) > 1<<20 || bloom.Bits(n, falsePositiveRate(mode))/8 > smallFilterBytes {
		var sk bloom.Sketch
		for _, tk := range tokens {
			sk.Add(tk)
		}

		n = sk.Estimate()
	}

	f := bloom.New(n, falsePositiveRate(mode))
	for _, tk := range tokens {
		f.Add(tk)
	}

	return f.Encode(nil)
}

func colOf(cells ...[]byte) *byteCol {
	var c byteCol
	for _, cell := range cells {
		c.appendCell(cell)
	}

	return &c
}

func attrsCell(tb testing.TB, kvs ...signal.KeyValue) []byte {
	tb.Helper()

	// The canonical attribute encoding the Attrs column carries is the hash pre-image; it is what
	// signal.AppendAttributes parses back.
	return signal.NewAttributes(kvs...).AppendHashInput(nil)
}

func TestBuildColumnBloomMatchesReference(t *testing.T) {
	t.Parallel()

	logLines := func(n int) *byteCol {
		var c byteCol
		for i := range n {
			c.appendCell([]byte("2026-07-23T09:45:37Z INFO checkout-service handler=CreateOrder user=" +
				strconv.Itoa(i%37) + " latency_ms=42 status=OK Region=EU-Central-1"))
		}

		return &c
	}

	for _, tt := range []struct {
		name   string
		mode   BloomMode
		values *byteCol
	}{
		{"none", BloomNone, colOf([]byte("x"))},
		{"fulltext empty column", BloomFullText, &byteCol{}},
		{"fulltext single", BloomFullText, colOf([]byte("hello world"))},
		{"fulltext many rows", BloomFullText, logLines(500)},
		{"fulltext mixed case", BloomFullText, colOf([]byte("MiXeD CaSe TOKENS lower"))},
		{"fulltext no alnum", BloomFullText, colOf([]byte("--- ... ///"))},
		{"fulltext empty cells", BloomFullText, colOf(nil, []byte("a"), nil, []byte("b c"))},
		{"equality", BloomEquality, colOf([]byte("abc"), []byte("def"))},
		{"equality with empties", BloomEquality, colOf(nil, []byte("abc"), nil, nil)},
		{"equality all empty", BloomEquality, colOf(nil, nil)},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			require.Equal(t, buildColumnBloomReference(tt.mode, tt.values), buildColumnBloom(tt.mode, tt.values))
		})
	}
}

func TestBuildColumnBloomAttrsMatchesReference(t *testing.T) {
	t.Parallel()

	kv := func(k string, v signal.Value) signal.KeyValue {
		return signal.KeyValue{Key: []byte(k), Value: v}
	}
	str := func(s string) signal.Value { return signal.StringValue([]byte(s)) }

	for _, tt := range []struct {
		name   string
		values *byteCol
	}{
		{"empty column", &byteCol{}},
		{
			"single attribute",
			colOf(attrsCell(t, kv("service.name", str("checkout")))),
		},
		{
			"multi-word and mixed case values",
			colOf(attrsCell(t,
				kv("http.route", str("/api/v1/Orders")),
				kv("k8s.pod.name", str("checkout-7d9f8b6c5d-Abc12")),
			)),
		},
		{
			"non-string value kinds",
			colOf(attrsCell(t,
				kv("count", signal.IntValue(42)),
				kv("ok", signal.BoolValue(true)),
				kv("ratio", signal.DoubleValue(1.5)),
			)),
		},
		{
			"value with no alphanumerics",
			colOf(attrsCell(t, kv("sep", str("--- ///")))),
		},
		{
			"undecodable cell is skipped",
			colOf([]byte{0xff, 0xff}, attrsCell(t, kv("a", str("b")))),
		},
		{
			"many rows",
			func() *byteCol {
				var c byteCol
				for i := range 300 {
					c.appendCell(attrsCell(t,
						kv("service.name", str("svc-"+strconv.Itoa(i%7))),
						kv("http.route", str("/api/v1/orders/"+strconv.Itoa(i))),
					))
				}

				return &c
			}(),
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			require.Equal(t, buildColumnBloomReference(BloomAttrs, tt.values), buildColumnBloom(BloomAttrs, tt.values))
		})
	}
}

// FuzzBuildColumnBloomMatchesReference fuzzes arbitrary column bytes through both builds; the
// encoded filters must not diverge for any input, including malformed attribute blobs.
func FuzzBuildColumnBloomMatchesReference(f *testing.F) {
	f.Add([]byte("hello world"), []byte("Second ROW here"))
	f.Add([]byte(""), []byte(""))
	f.Add([]byte("--- ///"), []byte("a"))
	f.Add([]byte{0x01, 0x01, 0x61, 0x00}, []byte{0xff})

	f.Fuzz(func(t *testing.T, a, b []byte) {
		values := colOf(a, b)
		for _, mode := range []BloomMode{BloomFullText, BloomEquality, BloomAttrs, BloomNone} {
			want := buildColumnBloomReference(mode, values)
			if got := buildColumnBloom(mode, values); !bytes.Equal(want, got) {
				t.Fatalf("mode %v: filter diverged from reference", mode)
			}
		}
	})
}

// benchBuild runs the two-pass build against the single-pass reference it replaced, so the
// difference is measurable in-tree (benchstat the two sub-benchmarks). Throughput is sized by the
// column's uncompressed bytes.
func benchBuild(b *testing.B, mode BloomMode, c *byteCol) {
	b.Helper()

	for _, tt := range []struct {
		name  string
		build func(BloomMode, *byteCol) []byte
	}{
		{"current", buildColumnBloom},
		{"reference", buildColumnBloomReference},
	} {
		b.Run(tt.name, func(b *testing.B) {
			b.ReportAllocs()
			b.SetBytes(int64(len(c.data)))

			for b.Loop() {
				tt.build(mode, c)
			}
		})
	}
}

func BenchmarkBuildColumnBloomFullText(b *testing.B) {
	var c byteCol
	for i := range 20000 {
		c.appendCell([]byte("2026-07-23T09:45:37Z INFO checkout-service handler=CreateOrder user_id=8f3a2b91 " +
			"latency_ms=" + strconv.Itoa(i%97) + " status=OK region=eu-central-1"))
	}

	benchBuild(b, BloomFullText, &c)
}

func BenchmarkBuildColumnBloomAttrs(b *testing.B) {
	kv := func(k, v string) signal.KeyValue {
		return signal.KeyValue{Key: []byte(k), Value: signal.StringValue([]byte(v))}
	}

	var c byteCol
	for i := range 20000 {
		c.appendCell(attrsCell(b,
			kv("service.name", "checkout-service"),
			kv("http.route", "/api/v1/orders/"+strconv.Itoa(i%997)),
			kv("k8s.pod.name", "checkout-7d9f8b6c5d-"+strconv.Itoa(i%31)),
		))
	}

	benchBuild(b, BloomAttrs, &c)
}
