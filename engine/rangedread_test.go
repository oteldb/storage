package engine_test

import (
	"context"
	"fmt"
	"maps"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/backend"
	"github.com/oteldb/storage/engine"
	"github.com/oteldb/storage/query/fetch"
)

// countingBackend records the object bytes a fetch actually pulls, whichever way it pulls them.
type countingBackend struct {
	backend.Backend

	bytes atomic.Int64

	mu     sync.Mutex
	perKey map[string]int64
}

func newCountingBackend() *countingBackend {
	return &countingBackend{Backend: backend.Memory(), perKey: map[string]int64{}}
}

func (b *countingBackend) Read(ctx context.Context, key string) ([]byte, error) {
	v, err := b.Backend.Read(ctx, key)
	b.note(key, len(v))

	return v, err
}

// ReadView is what the part reader takes, so it must be counted too — and forwarded, or the
// no-copy capability disappears under this wrapper.
func (b *countingBackend) ReadView(ctx context.Context, key string) ([]byte, error) {
	v, err := backend.ReadView(ctx, b.Backend, key)
	b.note(key, len(v))

	return v, err
}

func (b *countingBackend) ReadAt(ctx context.Context, key string, off, n int64) ([]byte, error) {
	v, err := backend.ReadAt(ctx, b.Backend, key, off, n)
	b.note(key, len(v))

	return v, err
}

func (b *countingBackend) note(key string, n int) {
	b.bytes.Add(int64(n))
	b.mu.Lock()
	b.perKey[key] += int64(n)
	b.mu.Unlock()
}

func (b *countingBackend) read() int64 { return b.bytes.Load() }

func (b *countingBackend) reset() {
	b.bytes.Store(0)
	b.mu.Lock()
	clear(b.perKey)
	b.mu.Unlock()
}

// report renders the per-key totals, so a failure says which object was read rather than only how
// much.
func (b *countingBackend) report() string {
	b.mu.Lock()
	defer b.mu.Unlock()

	var sb strings.Builder
	for _, k := range slices.Sorted(maps.Keys(b.perKey)) {
		fmt.Fprintf(&sb, "\n  %-44s %8d", k, b.perKey[k])
	}

	return sb.String()
}

// TestSelectiveFetchReadsAFractionOfTheColumn is the property #303 is about: a selector matching a
// few of many series must not pay for the whole column. The columns are one object each, so before
// ranged reads the answer was "all of it" no matter how few series matched.
func TestSelectiveFetchReadsAFractionOfTheColumn(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	b := newCountingBackend()

	// The block-sliced read path needs the decode cache; without it the engine falls back to decoding
	// whole columns and the column object is read whole by design.
	e := engine.New(engine.Config{Backend: b, Prefix: "t/metrics", DecodeCacheBytes: 1 << 20})

	// Sized so a column spans many compression frames: the frame is the smallest unit a ranged read
	// can fetch, so on a part small enough to fit in one frame there is nothing to save and the
	// measurement would say more about the test than the code.
	const (
		series     = 1000
		samples    = 500
		sampleStep = 15_000
	)

	for s := range series {
		ser := mkSeries("job", "api", "inst", instLabel(s))
		for i := range samples {
			mustAppend(t, e, ser, int64(i)*sampleStep, float64(s)+float64(i)/8)
		}
	}

	require.NoError(t, e.Flush(ctx))

	columnBytes := partColumnBytes(t, ctx, b.Backend, "t/metrics")
	require.Positive(t, columnBytes)

	b.reset()

	// One series of 400, over a window covering a fraction of its samples.
	got := fetchAll(t, e, fetch.Request{
		Start: 0, End: 20 * sampleStep,
		Matchers: []fetch.Matcher{eqMatcher("inst", instLabel(7))},
	})
	require.Len(t, got, 1)
	require.NotEmpty(t, got[0].Values)

	read := b.read()
	t.Logf("selective fetch read %d of %d column bytes (%.1f%%)%s",
		read, columnBytes, 100*float64(read)/float64(columnBytes), b.report())

	// The floor is the compression frame: it is the smallest unit a ranged read can fetch, so a
	// single-series fetch pays one frame per column however few rows it wants. What matters is that
	// the figure is a fraction of the column rather than all of it — and, more sharply, that it does
	// not grow with the column (see TestSelectiveFetchCostDoesNotGrowWithThePart).
	assert.Less(t, read, columnBytes/4,
		"a fetch matching 1 of %d series must not read the whole column", series)
}

// TestSelectiveFetchCostDoesNotGrowWithThePart is the invariant behind #303, stated without a magic
// ratio: what a selective fetch reads is set by the rows it wants, not by the part they sit in. With
// whole-column reads the two were the same number, which is what made part size a memory bound.
func TestSelectiveFetchCostDoesNotGrowWithThePart(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	const (
		samples    = 500
		sampleStep = 15_000
	)

	// Read cost for one series out of a part built from `series` series.
	//nolint:contextcheck // fetchAll owns its context, as every fetch helper in this package does
	measure := func(t *testing.T, series int) (read, columns int64) {
		t.Helper()

		b := newCountingBackend()
		e := engine.New(engine.Config{Backend: b, Prefix: "t/metrics", DecodeCacheBytes: 1 << 20})

		for s := range series {
			ser := mkSeries("job", "api", "inst", instLabel(s))
			for i := range samples {
				mustAppend(t, e, ser, int64(i)*sampleStep, float64(s)+float64(i)/8)
			}
		}

		require.NoError(t, e.Flush(ctx))

		columns = partColumnBytes(t, ctx, b.Backend, "t/metrics")
		b.reset()

		got := fetchAll(t, e, fetch.Request{
			Start: 0, End: 20 * sampleStep,
			Matchers: []fetch.Matcher{eqMatcher("inst", instLabel(7))},
		})
		require.Len(t, got, 1)

		return b.read(), columns
	}

	smallRead, smallColumns := measure(t, 250)
	largeRead, largeColumns := measure(t, 1000)

	t.Logf("250 series: read %d of %d column bytes", smallRead, smallColumns)
	t.Logf("1000 series: read %d of %d column bytes", largeRead, largeColumns)

	require.Greater(t, largeColumns, 3*smallColumns, "the larger part must really be larger")

	// Four times the part, but the same series and the same window: the read must stay in the same
	// range rather than track the part.
	assert.Less(t, largeRead, 2*smallRead,
		"read cost must be set by the rows fetched, not by the part holding them")
}

// TestSelectiveFetchIsCorrectOverRangedReads guards the obvious hazard of fetching frames
// individually: the samples must be exactly what a whole-column read would have produced.
func TestSelectiveFetchIsCorrectOverRangedReads(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	const (
		series     = 40
		samples    = 300
		sampleStep = 15_000
	)

	want := make(map[string][]float64, series)

	build := func(b backend.Backend, cacheBytes int64) *engine.Engine {
		e := engine.New(engine.Config{Backend: b, Prefix: "t/metrics", DecodeCacheBytes: cacheBytes})
		for s := range series {
			ser := mkSeries("job", "api", "inst", instLabel(s))
			for i := range samples {
				mustAppend(t, e, ser, int64(i)*sampleStep, float64(s*1000)+float64(i)/16)
			}
		}

		require.NoError(t, e.Flush(ctx))

		return e
	}

	// The whole-column path (no decode cache ⇒ no block slicing) is the reference.
	ref := build(backend.Memory(), 0)
	ranged := build(newCountingBackend(), 1<<20)

	for s := range series {
		lbl := instLabel(s)
		req := fetch.Request{
			Start: 0, End: int64(samples) * sampleStep,
			Matchers: []fetch.Matcher{eqMatcher("inst", lbl)},
		}

		refGot := fetchAll(t, ref, req)
		require.Len(t, refGot, 1, "series %s", lbl)
		want[lbl] = refGot[0].Values

		got := fetchAll(t, ranged, req)
		require.Len(t, got, 1, "series %s", lbl)
		assert.Equal(t, want[lbl], got[0].Values, "series %s", lbl)
		assert.Equal(t, refGot[0].Timestamps, got[0].Timestamps, "series %s", lbl)
	}
}

// partColumnBytes totals the bytes of every part column object under prefix — what a fetch used to
// read regardless of how few series it matched.
func partColumnBytes(t *testing.T, ctx context.Context, b backend.Backend, prefix string) int64 {
	t.Helper()

	keys, err := b.List(ctx, prefix)
	require.NoError(t, err)

	var total int64

	for _, k := range keys {
		if !isColumnKey(k) {
			continue
		}

		n, err := backend.SizeOf(ctx, b, k)
		require.NoError(t, err)

		total += n
	}

	return total
}

// isColumnKey reports whether key names a part's column object ("{part}/c/{i}").
func isColumnKey(key string) bool {
	for i := len(key) - 1; i > 0; i-- {
		if key[i] == '/' {
			return i >= 2 && key[i-2:i] == "/c"
		}
	}

	return false
}

func instLabel(i int) string {
	const digits = "0123456789abcdefghijklmnopqrstuvwxyz"

	return string([]byte{digits[i/36%36], digits[i%36]})
}
