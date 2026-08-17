package recordengine

import (
	"bytes"
	"encoding/binary"
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

// bloomCorpus is a set of repetitive, realistically shaped columns per mode — repetitive so that the
// first-occurrence skip actually has duplicates to drop.
func bloomCorpus(tb testing.TB) []struct {
	name   string
	mode   BloomMode
	values *byteCol
} {
	tb.Helper()

	kv := func(k, v string) signal.KeyValue {
		return signal.KeyValue{Key: []byte(k), Value: signal.StringValue([]byte(v))}
	}

	var logs byteCol
	for i := range 2000 {
		logs.appendCell([]byte("2026-07-23T09:45:37Z INFO checkout-service handler=CreateOrder user=" +
			strconv.Itoa(i%11) + " status=OK"))
	}

	var attrs byteCol
	for i := range 2000 {
		attrs.appendCell(attrsCell(tb,
			kv("service.name", "checkout-service"),
			kv("http.route", "/api/v1/orders/"+strconv.Itoa(i%13)),
		))
	}

	var eqDup byteCol
	for i := range 2000 {
		eqDup.appendCell([]byte("trace-" + strconv.Itoa(i%9)))
	}

	return []struct {
		name   string
		mode   BloomMode
		values *byteCol
	}{
		{"none", BloomNone, colOf([]byte("x"))},
		{"fulltext empty", BloomFullText, &byteCol{}},
		{"fulltext repetitive", BloomFullText, &logs},
		{"attrs empty", BloomAttrs, &byteCol{}},
		{"attrs repetitive", BloomAttrs, &attrs},
		{"equality repetitive", BloomEquality, &eqDup},
		{"equality distinct", BloomEquality, traceIDCol(2000)},
		{"equality with empties", BloomEquality, colOf(nil, []byte("abc"), nil, []byte("abc"))},
	}
}

// TestBuildColumnBloomDedupIsBitIdentical pins the property the mode-gated dedup rests on: which
// rows are walked never changes the encoded filter. A repeated value re-derives tokens the filter
// and the distinct-count sketch already hold, and both are idempotent per token — so forcing the
// first-occurrence skip on, or off, must produce the same bytes as the build's own choice, for every
// mode. (Without this, gating the pass off for [BloomEquality] would be a format change.)
func TestBuildColumnBloomDedupIsBitIdentical(t *testing.T) {
	t.Parallel()

	// forced walks the rows the caller picked rather than the ones build would.
	forced := func(dedup bool, mode BloomMode, values *byteCol) []byte {
		if mode == BloomNone {
			return nil
		}

		var bb bloomBuilder

		v := cells{flat: values}
		if dedup {
			bb.markRows(&v)
		}

		f := bloom.New(bb.sizeTokens(mode, &v), falsePositiveRate(mode))
		bb.forEachToken(mode, &v, f.Add)

		return f.Encode(nil)
	}

	for _, tt := range bloomCorpus(t) {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			want := buildColumnBloom(tt.mode, tt.values)
			require.Equal(t, want, forced(true, tt.mode, tt.values), "with dedup")
			require.Equal(t, want, forced(false, tt.mode, tt.values), "without dedup")
		})
	}
}

// TestBloomBuilderReuseMatchesFresh runs every corpus column through one shared builder — the
// flush/merge path's shape — and requires each filter to match a throwaway-builder build, so a
// scratch buffer left un-armed between columns (notably the first-occurrence row set) is caught.
func TestBloomBuilderReuseMatchesFresh(t *testing.T) {
	t.Parallel()

	corpus := bloomCorpus(t)

	var bb bloomBuilder

	// Twice, so a column also sees the state a *previous* pass left behind.
	for range 2 {
		for _, tt := range corpus {
			require.Equal(t, buildColumnBloom(tt.mode, tt.values), bb.build(tt.mode, cells{flat: tt.values}), tt.name)
		}
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

		// One shared builder walked twice over every mode: the filters must match the reference
		// whatever scratch the previous column (and the previous mode) left behind.
		var bb bloomBuilder

		for range 2 {
			for _, mode := range []BloomMode{BloomFullText, BloomEquality, BloomAttrs, BloomNone} {
				want := buildColumnBloomReference(mode, values)
				if got := buildColumnBloom(mode, values); !bytes.Equal(want, got) {
					t.Fatalf("mode %v: filter diverged from reference", mode)
				}

				if got := bb.build(mode, cells{flat: values}); !bytes.Equal(want, got) {
					t.Fatalf("mode %v: reused builder diverged from reference", mode)
				}
			}
		}
	})
}

// benchBuild runs the build in its two callable shapes — a throwaway builder per column
// (impl=fresh) and one re-armed builder (impl=reused, the flush/merge path) — against the
// single-pass reference it replaced, so both the per-column scratch cost and the two-pass win are
// measurable in one run (`benchstat -col /impl`). Throughput is sized by the column's uncompressed
// bytes.
func benchBuild(b *testing.B, mode BloomMode, c *byteCol) {
	b.Helper()

	b.Run("impl=fresh", func(b *testing.B) {
		b.ReportAllocs()
		b.SetBytes(int64(len(c.data)))

		for b.Loop() {
			buildColumnBloom(mode, c)
		}
	})

	b.Run("impl=reused", func(b *testing.B) {
		var bb bloomBuilder

		b.ReportAllocs()
		b.SetBytes(int64(len(c.data)))

		for b.Loop() {
			bb.build(mode, cells{flat: c})
		}
	})

	b.Run("impl=reference", func(b *testing.B) {
		b.ReportAllocs()
		b.SetBytes(int64(len(c.data)))

		for b.Loop() {
			buildColumnBloomReference(mode, c)
		}
	})
}

func BenchmarkBuildColumnBloomFullText(b *testing.B) {
	var c byteCol
	for i := range 20000 {
		c.appendCell([]byte("2026-07-23T09:45:37Z INFO checkout-service handler=CreateOrder user_id=8f3a2b91 " +
			"latency_ms=" + strconv.Itoa(i%97) + " status=OK region=eu-central-1"))
	}

	benchBuild(b, BloomFullText, &c)
}

// traceIDCol builds a trace_id-shaped equality column: 16 raw bytes per row, ~82% of rows distinct
// (real trace data is dominated by single-span traces), so the repeated-value dedup has almost
// nothing to skip and its per-row hash + map insert is pure cost.
func traceIDCol(rows int) *byteCol {
	var (
		c  byteCol
		id [16]byte
		n  uint64
	)

	for i := range rows {
		if i%100 < 82 {
			n++
		}

		binary.BigEndian.PutUint64(id[:8], n*0x9e3779b97f4a7c15)
		binary.BigEndian.PutUint64(id[8:], n)
		c.appendCell(id[:])
	}

	return &c
}

func BenchmarkBuildColumnBloomEquality(b *testing.B) {
	benchBuild(b, BloomEquality, traceIDCol(50000))
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
