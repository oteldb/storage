package recordengine_test

import (
	"bytes"
	"context"
	"fmt"
	"maps"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/backend"
	"github.com/oteldb/storage/encoding/chunk"
	"github.com/oteldb/storage/internal/partid"
	"github.com/oteldb/storage/recordengine"
)

// mergeRawIDSchema is [testSchema] with the id column raw-coded, the trace_id shape: it decodes with no
// dictionary, so the writer takes its rows as values while body and attrs arrive as ids.
var mergeRawIDSchema = recordengine.NewSchema(
	recordengine.Column{Name: "sev", Kind: recordengine.KindInt64, Codec: chunk.CodecT64},
	recordengine.Column{Name: "body", Kind: recordengine.KindBytes, Codec: chunk.CodecDict, Bloom: recordengine.BloomFullText},
	recordengine.Column{Name: "id", Kind: recordengine.KindBytes, Codec: chunk.CodecBytesRaw, Bloom: recordengine.BloomEquality},
	recordengine.Column{Name: "attrs", Kind: recordengine.KindBytes, Codec: chunk.CodecDict, Bloom: recordengine.BloomAttrs},
)

// dumpBackend reads every object the backend holds, so two runs can be compared byte for byte. Part
// ids are minted per run, so they are canonicalized away (see [canonicalizePartIDs]) — what the
// comparison is about is the part *contents*, which the ids would otherwise mask.
func dumpBackend(t *testing.T, b backend.Backend) map[string][]byte {
	t.Helper()

	ctx := context.Background()

	keys, err := b.List(ctx, "")
	require.NoError(t, err)

	out := make(map[string][]byte, len(keys))

	for _, k := range keys {
		v, err := b.Read(ctx, k)
		require.NoError(t, err)
		out[k] = v
	}

	return canonicalizePartIDs(out)
}

// partIDPattern matches a part id that follows a path separator, the only shape one takes in a key
// or in the length-prefixed prefixes an object body embeds.
var partIDPattern = regexp.MustCompile(`/([0-9A-HJKMNP-TV-Z]{26})`)

// canonicalizePartIDs rewrites every part id in a backend dump — in keys and in object bodies, which
// embed their own prefix — to its rank in creation order. The stand-in is the same length as an id,
// so length-prefixed framing inside an object stays byte-identical.
func canonicalizePartIDs(objs map[string][]byte) map[string][]byte {
	ids := make(map[string]struct{})

	collect := func(b []byte) {
		for _, m := range partIDPattern.FindAllSubmatch(b, -1) {
			if id := string(m[1]); partid.Valid(id) {
				ids[id] = struct{}{}
			}
		}
	}

	for k, v := range objs {
		collect([]byte(k))
		// Bodies name parts too — the bucket index lists the merge's removed prefixes, which are
		// gone from the backend and so appear in no key.
		collect(v)
	}

	rename := make(map[string]string, len(ids))
	for i, id := range slices.Sorted(maps.Keys(ids)) {
		rename[id] = fmt.Sprintf("PART%022d", i)
	}

	out := make(map[string][]byte, len(objs))

	for k, v := range objs {
		for id, name := range rename {
			k = strings.ReplaceAll(k, id, name)
			v = bytes.ReplaceAll(v, []byte(id), []byte(name))
		}

		out[k] = v
	}

	return out
}

// mergeCase is one store to build and merge; each test runs it through two paths whose outputs must
// agree.
type mergeCase struct {
	name    string
	schema  *recordengine.Schema
	maxPart int64
	retain  int64
	fill    func(t *testing.T, e *recordengine.Engine)
	// idle marks a case whose merge selects nothing, so it proves nothing about a merge path.
	idle bool
}

// dictRecs is n log-shaped records: a body drawn from a handful of templates and an attribute value
// from a handful of hosts, so every byte column dictionary-encodes heavily and the union of the
// sources' dictionaries is much smaller than the row count.
func dictRecs(from, n int) []rrec {
	out := make([]rrec, 0, n)

	for i := from; i < from+n; i++ {
		out = append(out, rrec{
			ts:   int64(i + 1),
			sev:  int64(i % 5),
			body: fmt.Sprintf("GET /api/v1/orders status=%d latency_ms=%d", 200+(i%3)*100, i%7),
			id:   fmt.Sprintf("%032x", i%11),
			attr: [2]string{"host", "node-" + strconv.Itoa(i%4)},
		})
	}

	return out
}

// fillParts flushes parts of n records each, count of them.
func fillParts(count, n int) func(*testing.T, *recordengine.Engine) {
	return func(t *testing.T, e *recordengine.Engine) {
		t.Helper()

		for p := range count {
			ingest(t, e, mkBatch("api", dictRecs(p*n, n)...))
			ingest(t, e, mkBatch("web", dictRecs(p*n, n)...))
			require.NoError(t, e.Flush(context.Background()))
		}
	}
}

// byteHeavyCase is a merge whose output-part seal is decided by the byte columns rather than by the
// fixed-width ones: many streams each holding a few long, heavily repeated bodies.
func byteHeavyCase() mergeCase {
	const (
		streams = 24
		rows    = 6
	)

	body := strings.Repeat("templated log line with a long stable prefix ", 12)

	return mergeCase{
		name: "byte-heavy cap", schema: testSchema, maxPart: 6 << 10,
		fill: func(t *testing.T, e *recordengine.Engine) {
			t.Helper()

			for p := range 2 {
				for s := range streams {
					recs := make([]rrec, 0, rows)
					for i := range rows {
						recs = append(recs, rrec{
							ts:   int64(p*rows + i + 1),
							body: body + strconv.Itoa(i%2),
							id:   fmt.Sprintf("%032x", i%3),
							attr: [2]string{"host", "node-" + strconv.Itoa(s%4)},
						})
					}

					ingest(t, e, mkBatch("svc-"+strconv.Itoa(s), recs...))
				}

				require.NoError(t, e.Flush(context.Background()))
			}
		},
	}
}

func mergeCases() []mergeCase {
	return []mergeCase{{
		name:   "repetitive",
		schema: testSchema,
		fill:   fillParts(3, 40),
	}, {
		name:    "multiple output parts",
		schema:  testSchema,
		maxPart: 512, // ≈ a handful of rows per part, so both flush and merge split
		fill:    fillParts(4, 30),
	}, {
		name:   "retention drops rows",
		schema: testSchema,
		retain: 45, // inside the first part's range, so the merge rewrites rather than drops it
		fill:   fillParts(3, 40),
	}, {
		name:   "mixed: raw id column",
		schema: mergeRawIDSchema,
		fill:   fillParts(3, 40),
	}, {
		name:   "single distinct value per column",
		schema: testSchema,
		fill: func(t *testing.T, e *recordengine.Engine) {
			t.Helper()

			for p := range 3 {
				recs := make([]rrec, 0, 20)
				for i := range 20 {
					recs = append(recs, rrec{
						ts: int64(p*20 + i + 1), body: "same", id: "same", attr: [2]string{"host", "same"},
					})
				}

				ingest(t, e, mkBatch("api", recs...))
				require.NoError(t, e.Flush(context.Background()))
			}
		},
	}, {
		name:   "empty column values",
		schema: testSchema,
		fill: func(t *testing.T, e *recordengine.Engine) {
			t.Helper()

			for p := range 2 {
				ingest(t, e, mkBatch("api",
					rrec{ts: int64(p*10 + 1)},
					rrec{ts: int64(p*10 + 2), body: "x"},
				))
				require.NoError(t, e.Flush(context.Background()))
			}
		},
	}, {
		name:   "nothing to merge",
		schema: testSchema,
		idle:   true,
		fill: func(t *testing.T, e *recordengine.Engine) {
			t.Helper()

			ingest(t, e, mkBatch("api", dictRecs(0, 5)...))
			require.NoError(t, e.Flush(context.Background()))
		},
	}, byteHeavyCase()}
}

// fuzzFill flushes parts of records whose byte columns cycle through slices of values, so a shape
// ranges from one distinct value per column to one per row.
func fuzzFill(streams, rows, parts int, values []byte) func(*testing.T, *recordengine.Engine) {
	pick := func(i int) string {
		if len(values) == 0 {
			return ""
		}

		return string(values[i%len(values):])
	}

	return func(t *testing.T, e *recordengine.Engine) {
		t.Helper()

		for p := range parts {
			for s := range streams {
				recs := make([]rrec, 0, rows)
				for i := range rows {
					recs = append(recs, rrec{
						ts:   int64(p*rows + i + 1),
						sev:  int64(i),
						body: pick(i),
						id:   pick(i + s),
						attr: [2]string{"host", pick(i + p)},
					})
				}

				ingest(t, e, mkBatch("svc-"+strconv.Itoa(s), recs...))
			}

			require.NoError(t, e.Flush(context.Background()))
		}
	}
}
