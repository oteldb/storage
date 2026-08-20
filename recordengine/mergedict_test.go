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

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/backend"
	"github.com/oteldb/storage/encoding/chunk"
	"github.com/oteldb/storage/internal/partid"
	"github.com/oteldb/storage/recordengine"
)

// mergeRawIDSchema is [testSchema] with the id column raw-coded, the trace_id shape: it decodes with no
// dictionary, so its column must fall back to the flat merge path while body and attrs still take
// the split one — the mixed set.
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

// mergeCase is one merge to run twice — once with the split (union dictionary + ids) carry forced
// off, once with it on — whose backend must come out byte-identical either way.
type mergeCase struct {
	name    string
	schema  *recordengine.Schema
	maxPart int64
	retain  int64
	fill    func(t *testing.T, e *recordengine.Engine)
	// wantSplit is the expected per-byte-column decision of the (single) merge that decodes
	// something. Nil means the merge selects nothing and no decision is made.
	wantSplit []bool
}

// runMergeCase builds the case's store from scratch, merges it, and returns the backend's objects
// plus every per-column split decision the merge made.
func runMergeCase(t *testing.T, c mergeCase, split bool) (map[string][]byte, [][]bool) {
	t.Helper()

	defer recordengine.SetMergeSplitDict(split)()

	var decisions [][]bool

	defer recordengine.ObserveMergeSplit(func(d []bool) {
		decisions = append(decisions, append([]bool(nil), d...))
	})()

	be := backend.Memory()
	e := recordengine.New(recordengine.Config{
		Schema: c.schema, Backend: be, Prefix: "t/dict", MaxPartBytes: c.maxPart,
	})

	c.fill(t, e)

	require.NoError(t, e.Merge(context.Background(), c.retain))

	return dumpBackend(t, be), decisions
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
// fixed-width ones: many streams (the seal is only checked at a stream boundary) each holding a few
// long, heavily repeated bodies. It is the shape that can tell an expanded size accounting from one
// that reports the id array.
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
		wantSplit: []bool{true, true, true},
	}
}

func mergeCases() []mergeCase {
	const allSplit = true

	return []mergeCase{{
		name:      "repetitive",
		schema:    testSchema,
		fill:      fillParts(3, 40),
		wantSplit: []bool{allSplit, allSplit, allSplit},
	}, {
		name:      "multiple output parts",
		schema:    testSchema,
		maxPart:   512, // ≈ a handful of rows per part, so both flush and merge split
		fill:      fillParts(4, 30),
		wantSplit: []bool{allSplit, allSplit, allSplit},
	}, {
		name:      "retention drops rows",
		schema:    testSchema,
		retain:    45, // inside the first part's range, so the merge rewrites rather than drops it
		fill:      fillParts(3, 40),
		wantSplit: []bool{allSplit, allSplit, allSplit},
	}, {
		name:      "mixed: raw id column stays flat",
		schema:    mergeRawIDSchema,
		fill:      fillParts(3, 40),
		wantSplit: []bool{allSplit, false, allSplit},
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
		wantSplit: []bool{allSplit, allSplit, allSplit},
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
		wantSplit: []bool{allSplit, allSplit, allSplit},
	}, {
		name:   "nothing to merge",
		schema: testSchema,
		fill: func(t *testing.T, e *recordengine.Engine) {
			t.Helper()

			ingest(t, e, mkBatch("api", dictRecs(0, 5)...))
			require.NoError(t, e.Flush(context.Background()))
		},
	}, byteHeavyCase()}
}

// TestMergeSplitDictMatchesFlat is the property the whole split carry rests on: a merge's output
// must be byte-identical whether the union-dictionary path or the flat blob path produced it — the
// same part objects, blooms, identity objects and record-keys footer. Every layer beneath
// guarantees it, so any mistake in the remap, the row ordering or the size accounting surfaces here.
//
//nolint:paralleltest // the split carry is a package-level seam this test flips
func TestMergeSplitDictMatchesFlat(t *testing.T) {
	for _, c := range mergeCases() {
		t.Run(c.name, func(t *testing.T) {
			flat, flatDecisions := runMergeCase(t, c, false)
			got, decisions := runMergeCase(t, c, true)

			require.Equal(t, flat, got, "merge output differs between the flat and split paths")

			for _, d := range flatDecisions {
				assert.NotContains(t, d, true, "forced-off merge took the split path")
			}

			if c.wantSplit == nil {
				assert.Empty(t, decisions, "expected no merge to decode anything")

				return
			}

			require.NotEmpty(t, decisions, "no merge ran, so the case proves nothing")
			for _, d := range decisions {
				assert.Equal(t, c.wantSplit, d, "per-column split decision")
			}
		})
	}
}

// FuzzMergeSplitDictMatchesFlat is [TestMergeSplitDictMatchesFlat] over generated stream/row shapes
// and column values: the same equality must hold for any of them.
func FuzzMergeSplitDictMatchesFlat(f *testing.F) {
	f.Add(byte(2), byte(3), byte(2), []byte("ab"))
	f.Add(byte(1), byte(1), byte(1), []byte(""))
	f.Add(byte(5), byte(7), byte(4), []byte("the quick brown fox"))
	f.Add(byte(3), byte(2), byte(9), []byte{0x00, 0xff, 0x01})

	f.Fuzz(func(t *testing.T, streams, rows, parts byte, values []byte) {
		c := mergeCase{
			schema: testSchema,
			fill:   fuzzFill(int(streams)%6+1, int(rows)%9+1, int(parts)%4+2, values),
		}

		flat, _ := runMergeCase(t, c, false)
		got, split := runMergeCase(t, c, true)

		require.Equal(t, flat, got)
		require.NotEmpty(t, split)
	})
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

// TestMergeSplitDictOutputPartsMatch checks the merge seals the same output parts either way, the
// visible half of the expanded size accounting ([TestSplitColSizeAccountingIsExpanded] pins the
// accounting itself).
//
//nolint:paralleltest // the split carry is a package-level seam this test flips
func TestMergeSplitDictOutputPartsMatch(t *testing.T) {
	c := byteHeavyCase()

	flat, _ := runMergeCase(t, c, false)
	got, _ := runMergeCase(t, c, true)

	require.Equal(t, partPrefixes(flat), partPrefixes(got))
	assert.Greater(t, len(partPrefixes(got)), 1)
}

// partPrefixes is the sorted set of part manifests in a backend dump, one per written part.
func partPrefixes(objs map[string][]byte) []string {
	var out []string

	for k := range objs {
		if strings.HasSuffix(k, "/manifest") {
			out = append(out, k)
		}
	}

	slices.Sort(out)

	return out
}
