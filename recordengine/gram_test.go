package recordengine

import (
	"context"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/backend"
	"github.com/oteldb/storage/index/bloom"
	"github.com/oteldb/storage/internal/sparsegram"
	"github.com/oteldb/storage/query/fetch"
)

// gramProbeSchema is the minimal schema for the probe tests: one gram-bearing byte column named
// "body" and one without, so the opt-out path is covered too.
var gramProbeSchema = NewSchema(
	Column{Name: "body", Kind: KindBytes, Grams: true},
	Column{Name: "plain", Kind: KindBytes},
)

// gramPart writes the column's gram sidecar to a memory backend and returns a part reading it, which
// is the whole demand-load path gramsMayMatch exercises.
func gramPart(tb testing.TB, column string, values *byteCol) (*part, backend.Backend) {
	tb.Helper()

	data := buildColumnGrams(values)
	require.NotNil(tb, data)

	be := backend.Memory()
	require.NoError(tb, be.Write(context.Background(), gramKey("p/0", column), data))

	return &part{schema: gramProbeSchema, prefix: "p/0"}, be
}

// mayMatchSubstring is the whole query path for one literal: extract the hints once, then probe.
func mayMatchSubstring(p *part, be backend.Backend, column string, lit []byte) bool {
	conds := []fetch.Condition{{Column: column, Substrings: [][]byte{lit}}}

	hints := buildGramHints(conds)
	if hints == nil {
		return true
	}

	return p.gramsMayMatch(context.Background(), be, newGramCache(0), conds, hints)
}

// countingBackend counts Read calls, to tell a cache hit from a backend round-trip. gate, when
// non-nil, holds each read until it is closed, so a test can pile concurrent callers onto one.
type countingBackend struct {
	backend.Backend

	reads atomic.Int64
	gate  chan struct{}
}

func (b *countingBackend) Read(ctx context.Context, key string) ([]byte, error) {
	b.reads.Add(1)

	if b.gate != nil {
		<-b.gate
	}

	return b.Backend.Read(ctx, key)
}

func TestGramFilterPrunes(t *testing.T) {
	t.Parallel()

	col := colOf(
		[]byte("2026-07-23T09:45:37Z INFO checkout-service handler=CreateOrder status=OK"),
		[]byte("2026-07-23T09:45:38Z WARN payment-gateway retry=3 upstream=stripe-api"),
		[]byte("trace deadbeefcafebabe0123456789abcdef span=root"),
	)

	p, be := gramPart(t, "body", col)

	for _, tt := range []struct {
		name string
		lit  string
		want bool
	}{
		// Present verbatim, including literals no whole token contains.
		{"whole token", "checkout-service", true},
		{"interior substring", "eckout-serv", true},
		{"spans a separator", "handler=CreateOrder", true},
		{"hex identifier", "deadbeefcafebabe0123456789abcdef", true},
		{"interior of hex identifier", "cafebabe01234567", true},

		// Absent: a long, distinctive literal is what the gram index is for.
		{"absent identifier", "0000000000000000badf00d000000000", false},
		{"absent phrase", "handler=DeleteOrder status=FAIL", false},

		// Shorter than gramMinLen ⇒ no grams ⇒ no pruning, and that is correct, not a miss.
		{"too short to prune", "zz", true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			assert.Equal(t, tt.want, mayMatchSubstring(p, be, "body", []byte(tt.lit)))
		})
	}
}

func TestGramFilterNoColumn(t *testing.T) {
	t.Parallel()

	p, be := gramPart(t, "body", colOf([]byte("checkout-service handler=CreateOrder")))

	absent := []byte("0000000000000000badf00d000000000")

	// A column the schema does not know (a per-record attribute) never prunes, however distinctive.
	assert.True(t, mayMatchSubstring(p, be, "other", absent))

	// Neither does a schema column that did not opt into a gram index — no sidecar is ever read.
	assert.True(t, mayMatchSubstring(p, be, "plain", absent))

	// Nor a part whose sidecar is missing from the backend.
	assert.True(t, mayMatchSubstring(&part{schema: gramProbeSchema, prefix: "nope"}, be, "body", absent))
}

// TestGramCacheReadsOnce pins the demand-load contract: the sidecar is read from the backend once
// per (part, column) and served from the cache after that — the point of not holding it resident is
// lost if every query re-reads it.
func TestGramCacheReadsOnce(t *testing.T) {
	t.Parallel()

	p, be := gramPart(t, "body", colOf([]byte("checkout-service handler=CreateOrder")))

	counted := &countingBackend{Backend: be}
	cache := newGramCache(0)

	conds := []fetch.Condition{{Column: "body", Substrings: [][]byte{[]byte("0000000000000000badf00d000000000")}}}
	hints := buildGramHints(conds)

	for range 5 {
		assert.False(t, p.gramsMayMatch(context.Background(), counted, cache, conds, hints))
	}

	assert.Equal(t, int64(1), counted.reads.Load())

	t.Run("a missing sidecar is negatively cached", func(t *testing.T) {
		t.Parallel()

		missing := &countingBackend{Backend: backend.Memory()}
		c := newGramCache(0)
		absentPart := &part{schema: gramProbeSchema, prefix: "p/0"}

		for range 5 {
			assert.True(t, absentPart.gramsMayMatch(context.Background(), missing, c, conds, hints))
		}

		assert.Equal(t, int64(1), missing.reads.Load(), "a part without a sidecar is not re-probed")
	})
}

// TestGramCacheSingleFlight pins that concurrent fetches asking for the same sidecar collapse into
// roughly one backend read rather than one each — on an object-store backend that is the difference
// between one GET and one per in-flight query.
//
// It deliberately does not assert *exactly* one. otter's Get checks the map and then starts a call,
// and the winning call can complete and be retired in the window between: measured, ~1 run in 20
// does a second load. That is harmless here (a sidecar is immutable, so a duplicate read returns the
// same bytes), so the property worth pinning is that the herd collapses, not that it collapses
// perfectly.
func TestGramCacheSingleFlight(t *testing.T) {
	t.Parallel()

	p, be := gramPart(t, "body", colOf([]byte("checkout-service handler=CreateOrder")))

	// The read is held open until every goroutine has reached the probe, so all of them are in
	// flight at once — dedupe applies to concurrent calls, and a caller that arrives after the
	// value lands simply hits the cache. Either way exactly one read reaches the backend.
	release := make(chan struct{})
	counted := &countingBackend{Backend: be, gate: release}
	cache := newGramCache(0)

	conds := []fetch.Condition{{Column: "body", Substrings: [][]byte{[]byte("0000000000000000badf00d000000000")}}}
	hints := buildGramHints(conds)

	const goroutines = 32

	var (
		wg      sync.WaitGroup
		start   = make(chan struct{})
		arrived = make(chan struct{}, goroutines)
	)

	results := make([]bool, goroutines)

	for i := range goroutines {
		wg.Go(func() {
			<-start
			arrived <- struct{}{}

			results[i] = p.gramsMayMatch(context.Background(), counted, cache, conds, hints)
		})
	}

	close(start)

	for range goroutines {
		<-arrived
	}

	close(release)
	wg.Wait()

	assert.Less(t, counted.reads.Load(), int64(goroutines/4),
		"%d concurrent probes should collapse into a handful of reads", goroutines)

	for i, got := range results {
		assert.False(t, got, "goroutine %d saw a different verdict", i)
	}
}

// TestGramCacheEvicts pins that the cache honors its byte bound rather than growing with part count
// — the whole reason gram filters are not resident. The eviction policy itself is otter's business;
// what this asserts is that the weigher is wired to real filter bytes, so the bound means something.
func TestGramCacheEvicts(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	be := backend.Memory()

	var big byteCol
	for i := range 2000 {
		big.appendCell([]byte("checkout-service handler=CreateOrder trace[deadbeefcafebabe" + strconv.Itoa(i) + "]"))
	}

	data := buildColumnGrams(&big)
	require.NotNil(t, data)

	const parts = 32

	for i := range parts {
		require.NoError(t, be.Write(ctx, gramKey("p/"+strconv.Itoa(i), "body"), data))
	}

	f, err := decodeGramFilter(data)
	require.NoError(t, err)
	require.NotZero(t, f.Bytes())

	// Room for two filters, not thirty-two.
	maxBytes := int64(f.Bytes()) * 2
	cache := newGramCache(maxBytes)

	for i := range parts {
		got, err := gramFilterFor(ctx, cache, be, gramKey("p/"+strconv.Itoa(i), "body"))
		require.NoError(t, err)
		require.NotNil(t, got, "every part has a readable sidecar")
	}

	// Eviction is amortized, so the bound is honored eventually rather than on the instant of the
	// overflowing Put.
	require.Eventually(t, func() bool {
		return cache.WeightedSize() <= uint64(maxBytes)
	}, time.Second, 10*time.Millisecond, "weighted size stayed above the bound")

	assert.Less(t, cache.EstimatedSize(), parts, "the cache did not grow with the part count")
}

// TestGramCacheWeighsMissesFree pins that a cached "no sidecar here" verdict costs no weight: a
// store full of gram-less parts must not evict the real filters.
func TestGramCacheWeighsMissesFree(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	be := backend.Memory()
	cache := newGramCache(1 << 20)

	for i := range 1000 {
		f, err := gramFilterFor(ctx, cache, be, gramKey("absent/"+strconv.Itoa(i), "body"))
		require.NoError(t, err)
		require.Nil(t, f)
	}

	assert.Zero(t, cache.WeightedSize(), "negative entries are weightless")
}

func TestGramHintsAbsentWhenNoSubstring(t *testing.T) {
	t.Parallel()

	// The common case: no condition asks for substring pruning, so no extraction happens at all.
	assert.Nil(t, buildGramHints([]fetch.Condition{{Column: "body", Tokens: [][]byte{[]byte("info")}}}))
	assert.Nil(t, buildGramHints(nil))

	hints := buildGramHints([]fetch.Condition{
		{Column: "body", Tokens: [][]byte{[]byte("info")}},
		{Column: "body", Substrings: [][]byte{[]byte("checkout-service")}},
	})
	require.Len(t, hints, 2)
	assert.Empty(t, hints[0], "a condition without a substring hint contributes no grams")
	assert.NotEmpty(t, hints[1])
}

func TestGramSidecarFormat(t *testing.T) {
	t.Parallel()

	data := buildColumnGrams(colOf([]byte("checkout-service handler=CreateOrder")))
	require.NotNil(t, data)
	require.GreaterOrEqual(t, len(data), 3)

	assert.Equal(t, gramFormatVersion, data[0])
	assert.Equal(t, byte(gramMinLen), data[1])
	assert.Equal(t, byte(gramMaxLen), data[2])

	t.Run("bounds mismatch is ignored, not misread", func(t *testing.T) {
		t.Parallel()

		// A sidecar built with different bounds holds a different gram set; probing it would prune
		// parts that match, so it must read back as a format mismatch (⇒ absent, ⇒ scan).
		for _, at := range []int{1, 2} {
			bad := append([]byte(nil), data...)
			bad[at]++

			_, err := decodeGramFilter(bad)
			require.ErrorIs(t, err, errGramFormat)
		}
	})

	t.Run("future version is ignored", func(t *testing.T) {
		t.Parallel()

		bad := append([]byte(nil), data...)
		bad[0] = gramFormatVersion + 1

		_, err := decodeGramFilter(bad)
		require.ErrorIs(t, err, errGramFormat)
	})

	t.Run("truncated header errors", func(t *testing.T) {
		t.Parallel()

		_, err := decodeGramFilter(data[:2])
		require.Error(t, err)
	})

	t.Run("corrupt filter errors", func(t *testing.T) {
		t.Parallel()

		bad := append([]byte(nil), data...)
		bad[len(bad)-1]++ // break the filter's trailing CRC

		_, err := decodeGramFilter(bad)
		require.Error(t, err)
	})
}

func TestBuildColumnGramsEmpty(t *testing.T) {
	t.Parallel()

	assert.Nil(t, buildColumnGrams(&byteCol{}), "a column with no rows has no sidecar")
}

// TestGramBuildDedupIsBitIdentical pins the property the row dedup relies on: a repeated value
// re-derives grams the filter already holds, so walking every row and walking only first
// occurrences must produce the same bytes.
func TestGramBuildDedupIsBitIdentical(t *testing.T) {
	t.Parallel()

	var deduped, every byteCol

	for i := range 200 {
		cell := []byte("checkout-service handler=CreateOrder user=" + strconv.Itoa(i%4))
		deduped.appendCell(cell)
		every.appendCell(cell)
	}

	var bb bloomBuilder

	withDedup := bb.buildGrams(&deduped)

	// Same build with the dedup disabled: size and fill over every row.
	var plain bloomBuilder

	plain.rows = nil
	plain.distinct.Reset()
	plain.forEachGram(&every, plain.distinct.Add)

	f := bloom.New(plain.distinct.Estimate(), gramFalsePositiveRate)
	plain.forEachGram(&every, f.Add)

	assert.Equal(t, encodeGramFilter(f), withDedup)
}

// FuzzGramFilterNoFalseNegatives is the safety property the whole scheme rests on: if a value in the
// column contains the literal, the part must never be pruned. It exercises the real build and the
// real query path against arbitrary bytes.
func FuzzGramFilterNoFalseNegatives(f *testing.F) {
	f.Add([]byte("checkout-service handler=CreateOrder status=OK"), 5, 20)
	f.Add([]byte("trace deadbeefcafebabe0123456789abcdef"), 6, 32)
	f.Add([]byte("\x00\x00\x00\x00\x00\x00\x00\x00"), 0, 8)
	f.Add([]byte("ababababababababab"), 2, 9)

	f.Fuzz(func(t *testing.T, value []byte, start, length int) {
		if len(value) == 0 {
			return
		}

		// Any substring of the value, normalized into range.
		start = ((start % len(value)) + len(value)) % len(value)

		length = ((length % (len(value) - start)) + (len(value) - start)) % (len(value) - start)
		lit := value[start : start+length]

		if len(lit) == 0 {
			return
		}

		p, be := gramPart(t, "body", colOf(value))

		if !mayMatchSubstring(p, be, "body", lit) {
			t.Fatalf("pruned a part containing the literal: value=%q lit=%q", value, lit)
		}
	})
}

// FuzzGramHintsAreColumnGrams pins the build/query agreement one level down: every gram the query
// side probes for a literal must be a gram the build side would emit for a value containing it.
func FuzzGramHintsAreColumnGrams(f *testing.F) {
	f.Add([]byte("prefix-"), []byte("checkout-service"), []byte("-suffix"))
	f.Add([]byte(""), []byte("deadbeefcafebabe"), []byte(""))
	f.Add([]byte("\xff"), []byte("aaaaaaaaaa"), []byte("\x00"))

	f.Fuzz(func(t *testing.T, prefix, lit, suffix []byte) {
		if len(lit) < gramMinLen {
			return
		}

		value := append(append(append([]byte(nil), prefix...), lit...), suffix...)

		var bb bloomBuilder

		have := map[string]struct{}{}
		bb.forEachGram(colOf(value), func(g []byte) { have[string(g)] = struct{}{} })

		var ext sparsegram.Extractor

		ext.MinLen, ext.MaxLen = gramMinLen, gramMaxLen

		for _, g := range sparsegram.Covering(ext.Grams(nil, lit)) {
			if _, ok := have[string(lit[g.Start:g.End])]; !ok {
				t.Fatalf("query gram %q of %q is not a gram of %q", lit[g.Start:g.End], lit, value)
			}
		}
	})
}
