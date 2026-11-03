package engine

import (
	"strconv"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/backend"
	"github.com/oteldb/storage/backend/backendtest"
	backendfile "github.com/oteldb/storage/backend/file"
	"github.com/oteldb/storage/block"
	"github.com/oteldb/storage/encoding/chunk"
	"github.com/oteldb/storage/query/fetch"
	"github.com/oteldb/storage/signal"
)

func TestSeriesChunkAdmissionBeforeMetadata(t *testing.T) {
	t.Parallel()
	files, err := backendfile.New(t.TempDir())
	require.NoError(t, err)
	counter := backendtest.NewSizedByteCounter(files)
	budget := NewDecodeBudget(128)
	e := New(Config{Backend: counter, Prefix: "admission-chunks", MetricSeriesChunks: true, MetricBlockRows: 4, DecodeCacheBytes: 1 << 20, DecodeBudget: budget})
	for i := range 20 {
		_, err := e.Append(apiSeries(), int64(i+1), float64(i))
		require.NoError(t, err)
	}
	require.NoError(t, e.Flush(t.Context()))
	_, err = budget.acquire(t.Context(), 128)
	require.NoError(t, err)
	defer budget.release(128)
	counter.Reset()
	it, err := e.TryFetch(t.Context(), fetch.Request{Start: 0, End: 100})
	require.ErrorIs(t, err, fetch.ErrDecodeBusy)
	require.Nil(t, it)
	require.NotContains(t, counter.Report(), "/c/")
	require.NotContains(t, counter.Report(), "/marks")
	require.Zero(t, budget.waiting())
}

func TestSeriesChunkIgnoresLegacyMarks(t *testing.T) {
	t.Parallel()
	be := backend.Memory()
	e := New(Config{Backend: be, Prefix: "bad-marks-chunks", MetricSeriesChunks: true, MetricBlockRows: 4, DecodeCacheBytes: 1 << 20})
	for i := range 10 {
		_, err := e.Append(apiSeries(), int64(i+1), float64(i))
		require.NoError(t, err)
	}
	require.NoError(t, e.Flush(t.Context()))
	p := e.parts[0]
	marks := block.Marks{GranuleSize: 4, Granules: []block.Granule{{FirstRow: 0, MinKey: 1000, MaxKey: 1001}, {FirstRow: 3, MinKey: 1000, MaxKey: 1001}, {FirstRow: 8, MinKey: 1000, MaxKey: 1001}}}

	require.NoError(t, be.Write(t.Context(), p.prefix+"/marks", marks.Encode(nil)))
	e.blockCache.evictPrefix(p.prefix)
	it, err := e.Fetch(t.Context(), fetch.Request{Start: 1, End: 10})
	require.NoError(t, err)
	got, err := fetch.Drain(t.Context(), it)
	require.NoError(t, err)
	require.Len(t, got, 1)
	require.Len(t, got[0].Values, 10)
	require.Nil(t, p.granules, "variable marks are not pinned on the part")
}

func TestSeriesChunkWarmCacheChecksColumnGeometry(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	be := backend.Memory()
	e := New(Config{Backend: be, Prefix: "geometry-chunks", MetricSeriesChunks: true, MetricBlockRows: 4, DecodeCacheBytes: 1 << 20})
	for i := range 10 {
		_, err := e.Append(apiSeries(), int64(i+1), float64(i))
		require.NoError(t, err)
	}
	require.NoError(t, e.Flush(ctx))
	p := e.parts[0]
	w := block.NewStreamWriterTo(ctx, be, "different-geometry", block.WithIndependentGroups(), block.WithGranuleSize(4))
	require.NoError(t, w.AddColumn(block.Column{Name: colValue, Kind: block.KindFloat64, Codec: chunk.CodecFloatGroups, Block: true}))
	for _, values := range [][]float64{{0, 1, 2}, {3, 4, 5, 6}, {7, 8, 9}} {
		require.NoError(t, w.AppendFloat64Group(0, values, true))
		require.NoError(t, w.EndGroup())
	}
	require.NoError(t, block.WriteStreamPart(ctx, be, "different-geometry", w))
	other, err := block.OpenPart(ctx, be, "different-geometry")
	require.NoError(t, err)
	r := e.newSeriesBlockReader(p)
	require.NotNil(t, r)
	defer r.releasePins()
	require.NoError(t, r.ensureGeometry(ctx))
	r.metadata[colValID], err = other.ColumnBlocks(ctx, colValue)
	require.NoError(t, err)
	entry := e.blockCache.insert(&blockEntry{key: blockKey{prefix: p.prefix, col: colValID, blk: 0}, f64: []float64{0, 1, 2, 3}, bytes: 32})
	e.blockCache.release(entry)
	_, _, err = r.floatBlock(ctx, &r.valDec, colValue, colValID, 0)
	require.ErrorIs(t, err, block.ErrCorrupt)
}

func TestSeriesChunkReadAmplification(t *testing.T) {
	t.Parallel()
	results := map[bool]int64{}
	for _, independent := range []bool{false, true} {
		files, err := backendfile.New(t.TempDir())
		require.NoError(t, err)
		counter := backendtest.NewSizedByteCounter(files)
		e := New(Config{Backend: counter, Prefix: "read-chunks", MetricFloatGroups: true, MetricSeriesChunks: independent, DecodeCacheBytes: 1 << 20})
		hostsByID := map[signal.SeriesID]string{}
		for s := range 64 {
			series := apiSeries()
			series.Attributes = signal.NewAttributes(append(series.Attributes, signal.KeyValue{Key: []byte("host"), Value: signal.StringValue([]byte(strconv.Itoa(s)))})...)
			hostsByID[series.Hash()] = strconv.Itoa(s)
			for i := range 60 {
				_, err := e.Append(series, int64(i+1), float64(s*1000+i))
				require.NoError(t, err)
			}
		}
		require.NoError(t, e.Flush(t.Context()))
		// Prime only the query-owned directory and marks; the following series' data stays cold.
		query := func(host string) {
			it, err := e.Fetch(t.Context(), fetch.Request{Start: 1, End: 60, Matchers: []fetch.Matcher{{Name: []byte("host"), Match: func(v signal.Value) bool { return string(v.Str()) == host }}}})
			require.NoError(t, err)
			got, err := fetch.Drain(t.Context(), it)
			require.NoError(t, err)
			require.Len(t, got, 1)
			require.Len(t, got[0].Values, 60)
		}
		require.NotNil(t, e.parts[0].index.paged)
		query(hostsByID[e.parts[0].index.paged.first])
		counter.Reset()
		query(hostsByID[e.parts[0].index.paged.last])
		var bytes int64
		for line := range strings.SplitSeq(counter.Report(), "\n") {
			if strings.Contains(line, "/c/") {
				fields := strings.Fields(line)
				n, err := strconv.ParseInt(fields[len(fields)-1], 10, 64)
				require.NoError(t, err)
				bytes += n
			}
		}
		results[independent] = bytes
		t.Logf("independent=%t sample-column backend bytes=%d report=%s", independent, bytes, counter.Report())
	}
	require.Less(t, results[true], results[false])
}

func TestSeriesChunkCacheChargesCapacityAndEntries(t *testing.T) {
	t.Parallel()
	e := New(Config{Backend: backend.Memory(), Prefix: "small-chunks", MetricSeriesChunks: true, DecodeCacheBytes: 1 << 20})
	e.blockCache.i64Free.put(make([]int64, 1024))
	e.blockCache.f64Free.put(make([]float64, 1024))
	for i := range 64 {
		s := apiSeries()
		s.Attributes = signal.NewAttributes(append(s.Attributes, signal.KeyValue{Key: []byte("host"), Value: signal.StringValue([]byte(strconv.Itoa(i)))})...)
		_, err := e.Append(s, int64(i+1), float64(i))
		require.NoError(t, err)
	}
	require.NoError(t, e.Flush(t.Context()))
	it, err := e.Fetch(t.Context(), fetch.Request{Start: 0, End: 100})
	require.NoError(t, err)
	got, err := fetch.Drain(t.Context(), it)
	require.NoError(t, err)
	require.Len(t, got, 64)
	var entries int
	var charged int64
	for i := range e.blockCache.shards {
		shard := &e.blockCache.shards[i]
		shard.mu.Lock()
		for _, cached := range shard.items {
			for key, entry := range cached {
				if key.blk < 0 || key.col == colMarksID {
					continue
				}
				require.Equal(t, 1, cap(entry.i64)+cap(entry.f64))
				require.GreaterOrEqual(t, entry.bytes, variableChunkEntryOverhead+8)
				entries++
				charged += entry.bytes
			}
		}
		shard.mu.Unlock()
	}
	require.Equal(t, 128, entries)
	require.GreaterOrEqual(t, e.blockCache.bytes.Load(), charged)
	require.LessOrEqual(t, e.blockCache.bytes.Load(), e.blockCache.maxBytes)
}
