package engine

import (
	"context"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"runtime"
	"runtime/debug"
	"runtime/pprof"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/go-faster/errors"

	"github.com/oteldb/storage/backend"
	backendfile "github.com/oteldb/storage/backend/file"
	"github.com/oteldb/storage/internal/timebucket"
	"github.com/oteldb/storage/query/fetch"
	"github.com/oteldb/storage/signal"
	"github.com/oteldb/storage/wal"
)

const (
	cpuFixtureBase    = int64(1_700_006_400) * int64(time.Second)
	cpuFixtureCadence = 15 * int64(time.Second)
)

var cpuFixtureModes = [...]string{"idle", "user", "system", "iowait", "irq", "softirq", "nice", "steal"}

type cpuFixtureOptions struct {
	hosts, generations, points int
	flushMemoryBytes           int64
	readCache                  bool
	disk, sync, reopen         bool
	separate                   bool
}

// Hosts defaults to 10,000 (80,000 series), capped at 80,000 (640,000 series).
// CPU_GENERATIONS/CPU_POINTS default to four generations with sixteen 15-second samples each.
func cpuBenchOptions(b *testing.B) cpuFixtureOptions {
	b.Helper()
	return cpuFixtureOptions{
		hosts:       cpuBenchInt(b, "OTELDB_BENCH_CPU_HOSTS", 10000, 80000),
		generations: cpuBenchInt(b, "OTELDB_BENCH_CPU_GENERATIONS", 4, 4),
		points:      cpuBenchInt(b, "OTELDB_BENCH_CPU_POINTS", 16, 1024),
		disk:        os.Getenv("OTELDB_BENCH_DISK_SERIES") == "true",
		readCache:   os.Getenv("OTELDB_BENCH_CPU_READ_CACHE") == "true",
		sync:        os.Getenv("OTELDB_BENCH_WAL_SYNC") != "false",
		reopen:      os.Getenv("OTELDB_BENCH_CPU_REOPEN") == "true",
	}
}

func cpuBenchInt(b *testing.B, key string, fallback, maximum int) int {
	b.Helper()
	raw := os.Getenv(key)
	if raw == "" {
		return fallback
	}
	n, err := strconv.Atoi(raw)
	if err != nil || n < 1 || n > maximum {
		b.Fatalf("%s must be in [1,%d], got %q", key, maximum, raw)
	}
	return n
}

type cpuFixture struct {
	engine                *Engine
	counter               *cpuFixtureFile
	options               cpuFixtureOptions
	appendTime, flushTime time.Duration
	root                  string
}

func cpuHost(host int) string { return fmt.Sprintf("node-%05d:9100", host) }

func cpuMetricSeries(i int) signal.Series {
	host := []byte(cpuHost(i / len(cpuFixtureModes)))
	return signal.Series{
		Resource: signal.Resource{Attributes: signal.NewAttributes(
			signal.KeyValue{Key: []byte("host.name"), Value: signal.StringValue(host)},
			signal.KeyValue{Key: []byte("service.name"), Value: signal.StringValue([]byte("node-exporter"))},
			signal.KeyValue{Key: []byte("service.namespace"), Value: signal.StringValue([]byte("monitoring"))},
			signal.KeyValue{Key: []byte("os.type"), Value: signal.StringValue([]byte("linux"))},
		)},
		Scope: signal.Scope{Name: []byte("prometheus/node-exporter"), Version: []byte("1.9.0")},
		Attributes: signal.NewAttributes(
			signal.KeyValue{Key: []byte("__name__"), Value: signal.StringValue([]byte("node_cpu_seconds_total"))},
			signal.KeyValue{Key: []byte("instance"), Value: signal.StringValue(host)},
			signal.KeyValue{Key: []byte("cpu"), Value: signal.StringValue([]byte("0"))},
			signal.KeyValue{Key: []byte("mode"), Value: signal.StringValue([]byte(cpuFixtureModes[i%len(cpuFixtureModes)]))},
		),
	}
}

func newCPUFixture(tb testing.TB, ctx context.Context, options cpuFixtureOptions) *cpuFixture {
	tb.Helper()
	f := &cpuFixture{root: tb.TempDir(), options: options}
	files, err := backendfile.New(filepath.Join(f.root, "parts"))
	if err != nil {
		tb.Fatal(err)
	}
	f.counter = &cpuFixtureFile{File: files}
	w, err := wal.Create(filepath.Join(f.root, "wal"), 0)
	if err != nil {
		tb.Fatal(err)
	}
	w.SetSync(options.sync)
	var source backend.Backend = f.counter
	if options.readCache {
		source = backend.Cached(source, 256<<20)
	}
	flushBytes := options.flushMemoryBytes
	if flushBytes == 0 {
		flushBytes = 64 << 20
	}
	f.engine = New(Config{
		Backend: source, WAL: w, Prefix: "cpu", DiskSeries: options.disk, SeparateSeries: options.separate,
		MetricSeriesChunks: true, MetricBlockRows: 1024, AggregateStats: true,
		DecodeCacheBytes: 16 << 20, DecodeMemoryBytes: 256 << 20, FlushMemoryBytes: flushBytes,
	})
	tb.Cleanup(func() {
		if f.engine != nil {
			_ = f.engine.Close(context.WithoutCancel(ctx))
		}
	})
	return f
}

// buildCPUFixture retains no input identity vector or source query after ingestion.
func buildCPUFixture(b *testing.B, options cpuFixtureOptions) *cpuFixture {
	b.Helper()
	f := newCPUFixture(b, b.Context(), options)
	for generation := range options.generations {
		f.appendGeneration(b, generation)
		start := time.Now()
		if err := f.engine.Flush(b.Context()); err != nil {
			b.Fatal(err)
		}
		f.flushTime += time.Since(start)
	}
	if options.reopen {
		if err := f.engine.Close(b.Context()); err != nil {
			b.Fatal(err)
		}
		cfg := f.engine.cfg
		cfg.WAL = nil
		f.engine = New(cfg)
		if err := f.engine.LoadPartsReadOnly(b.Context()); err != nil {
			b.Fatal(err)
		}
	}
	if got := f.engine.Stats().Series; got != int64(options.hosts*len(cpuFixtureModes)) {
		b.Fatalf("fixture series=%d, want %d", got, options.hosts*len(cpuFixtureModes))
	}
	return f
}

func (f *cpuFixture) appendGeneration(b *testing.B, generation int) {
	b.Helper()
	const batchRows = 4096
	seriesPerBatch := max(1, batchRows/f.options.points)
	identities := make([]signal.Series, seriesPerBatch)
	ids := make([]signal.SeriesID, seriesPerBatch*f.options.points)
	ts, values := make([]int64, len(ids)), make([]float64, len(ids))
	totalSeries := f.options.hosts * len(cpuFixtureModes)
	for offset := 0; offset < totalSeries; offset += seriesPerBatch {
		count := min(seriesPerBatch, totalSeries-offset)
		for i := range count {
			identities[i] = cpuMetricSeries(offset + i)
			id := identities[i].Hash()
			for point := range f.options.points {
				j := i*f.options.points + point
				ids[j] = id
				ts[j] = cpuFixtureBase + int64(generation*f.options.points+point)*cpuFixtureCadence
				values[j] = float64(offset+i+generation*f.options.points+point) / 1000
			}
		}
		n := count * f.options.points
		start := time.Now()
		result, err := f.engine.AppendBatch(ids[:n], ts[:n], values[:n], nil,
			func(i int) signal.Series { return identities[i/f.options.points] }, AppendLimits{})
		f.appendTime += time.Since(start)
		if err != nil || result.Accepted != n {
			b.Fatalf("append accepted=%d, want %d: %v", result.Accepted, n, err)
		}
	}
}

func (f *cpuFixture) request(host int) fetch.Request {
	hostEqual := &fetch.EqualMatcher{Name: "host.name", Value: cpuHost(host)}
	nameEqual := &fetch.EqualMatcher{Name: "__name__", Value: "node_cpu_seconds_total"}
	return fetch.Request{
		Start: cpuFixtureBase,
		End:   cpuFixtureBase + int64(f.options.generations*f.options.points-1)*cpuFixtureCadence, Recycle: true,
		Matchers: []fetch.Matcher{
			{Name: []byte(hostEqual.Name), Match: hostEqual.Predicate(), Spec: hostEqual},
			{Name: []byte(nameEqual.Name), Match: nameEqual.Predicate(), Spec: nameEqual},
		},
	}
}

func (f *cpuFixture) fetchHost(ctx context.Context, host int) error {
	it, err := f.engine.Fetch(ctx, f.request(host))
	if err != nil {
		return err
	}
	defer func() { _ = it.Close() }()
	series, rows := 0, 0
	for {
		batch, err := it.Next(ctx)
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return err
		}
		series++
		rows += len(batch.Timestamps)
		batch.Release()
	}
	if series != len(cpuFixtureModes) || rows != len(cpuFixtureModes)*f.options.generations*f.options.points {
		return errors.Errorf("fixture fetch series=%d rows=%d", series, rows)
	}
	return nil
}

func (f *cpuFixture) countHost(ctx context.Context, host int) error {
	points := f.options.generations * f.options.points
	steps := min(points, 16)
	grid := fetch.WindowGrid{
		Start: cpuFixtureBase + int64(points-steps)*cpuFixtureCadence,
		Step:  cpuFixtureCadence, Width: 300 * int64(time.Second), NumSteps: steps,
	}
	groups, err := f.engine.CountGrid(ctx, f.request(host), grid, []byte("mode"))
	if err != nil {
		return err
	}
	if len(groups) != len(cpuFixtureModes) {
		return errors.Errorf("fixture grid groups=%d", len(groups))
	}
	for _, values := range groups {
		for _, n := range values {
			if n != 1 {
				return errors.Errorf("fixture grid count=%d", n)
			}
		}
	}
	return nil
}

func (f *cpuFixture) countAll(ctx context.Context, _ int) error {
	r := f.request(0)
	r.Matchers = nil
	count, err := f.engine.Count(ctx, r)
	if err != nil {
		return err
	}
	if count != f.options.hosts*len(cpuFixtureModes) {
		return errors.Errorf("fixture count=%d", count)
	}
	return nil
}

func (f *cpuFixture) prepareMerge(b *testing.B) int {
	b.Helper()
	points := f.options.generations * f.options.points
	if int64(points)*cpuFixtureCadence < int64(time.Hour) {
		return 1
	}
	// Seal older compaction buckets without adding the marker to the CPU query range.
	last := cpuFixtureBase + int64(points-1)*cpuFixtureCadence
	marker := signal.Series{Attributes: signal.NewAttributes(signal.KeyValue{
		Key: []byte("__name__"), Value: signal.StringValue([]byte("benchmark_clock")),
	})}
	accepted, err := f.engine.Append(marker, timebucket.End(last, timebucket.Top())+1, 0)
	if err != nil || !accepted {
		b.Fatalf("append bucket marker: accepted=%t: %v", accepted, err)
	}
	if err := f.engine.Flush(b.Context()); err != nil {
		b.Fatal(err)
	}
	return 2
}

// BenchmarkDiskSeriesCPUQuery uses one fixture per process mode. Select a single workload when profiling.
// Set OTELDB_BENCH_DISK_SERIES=true for disk indexes; use a disk-backed TMPDIR for file/WAL timing.
// OTELDB_BENCH_CPU_READ_CACHE=true enables a shared 256 MiB backend cache; read-B/op counts file reads.
func BenchmarkDiskSeriesCPUQuery(b *testing.B) {
	options := cpuBenchOptions(b)
	merged := os.Getenv("OTELDB_BENCH_CPU_QUERY_MERGE") == "true"
	if merged {
		options.flushMemoryBytes = 2 << 30
	}
	f := buildCPUFixture(b, options)
	if merged {
		expectedParts := f.prepareMerge(b)
		if err := f.engine.MergeWith(b.Context(), MergeOptions{Force: true}); err != nil {
			b.Fatal(err)
		}
		if f.engine.PartCount() != expectedParts {
			b.Fatalf("merged query fixture has %d parts, want %d", f.engine.PartCount(), expectedParts)
		}
	}
	for _, workload := range []string{"fetch-random-host", "fetch-repeated-host", "grid-random-host", "grid-repeated-host", "count-all"} {
		b.Run(workload, func(b *testing.B) {
			query := f.fetchHost
			if strings.HasPrefix(workload, "grid") {
				query = f.countHost
			}
			if workload == "count-all" {
				query = f.countAll
			}
			if err := query(b.Context(), 0); err != nil {
				b.Fatal(err)
			}
			finishProfile := cpuQueryProfile(b)
			defer finishProfile()
			f.counter.Reset()
			b.ReportAllocs()
			var next uint64
			for b.Loop() {
				host := 0
				if strings.Contains(workload, "random") {
					next = next*6364136223846793005 + 1442695040888963407
					host = int(next % uint64(options.hosts))
				}
				if err := query(b.Context(), host); err != nil {
					b.Fatal(err)
				}
			}
			b.ReportMetric(float64(f.counter.Bytes())/float64(b.N), "read-B/op")
		})
	}
}

// BenchmarkDiskSeriesCPUIngest measures the complete end-to-end fixture, including input generation.
// Its ns/B/allocs include setup; use BenchmarkDiskSeriesCPUStorage for isolated storage phases.
// Fixed -benchtime=1x avoids retaining several fixtures during benchmark calibration.
func BenchmarkDiskSeriesCPUIngest(b *testing.B) {
	options := cpuBenchOptions(b)
	b.ReportAllocs()
	var appendTime, flushTime time.Duration
	for b.Loop() {
		f := buildCPUFixture(b, options)
		appendTime += f.appendTime
		flushTime += f.flushTime
		b.StopTimer()
		if err := f.engine.Close(b.Context()); err != nil {
			b.Fatal(err)
		}
		f.engine = nil
		f.counter = nil
		if err := os.RemoveAll(f.root); err != nil {
			b.Fatal(err)
		}
		b.StartTimer()
	}
	samples := float64(options.hosts * len(cpuFixtureModes) * options.generations * options.points * b.N)
	b.ReportMetric(samples/appendTime.Seconds(), "append-samples/s")
	b.ReportMetric(samples/flushTime.Seconds(), "flush-samples/s")
	b.ReportMetric(appendTime.Seconds()/float64(b.N), "append-s/op")
	b.ReportMetric(flushTime.Seconds()/float64(b.N), "flush-s/op")
}

// BenchmarkDiskSeriesCPURetained measures live heap after GC, without retaining fixture input arrays.
// Run each mode separately with -benchtime=1x -count=1. RSS is Linux-only, after forced heap scavenging.
// Compare custom memory counters; ns/op includes fixture construction and GC.
func BenchmarkDiskSeriesCPURetained(b *testing.B) {
	options := cpuBenchOptions(b)
	b.ReportAllocs()
	var previous *cpuFixture
	for b.Loop() {
		if previous != nil {
			if err := previous.engine.Close(b.Context()); err != nil {
				b.Fatal(err)
			}
			previous.engine = nil
			previous.counter = nil
			if err := os.RemoveAll(previous.root); err != nil {
				b.Fatal(err)
			}
		}
		debug.FreeOSMemory()
		var before, after runtime.MemStats
		runtime.ReadMemStats(&before)
		f := buildCPUFixture(b, options)
		debug.FreeOSMemory()
		runtime.ReadMemStats(&after)
		stats := f.engine.Stats()
		b.ReportMetric(float64(int64(after.HeapAlloc)-int64(before.HeapAlloc)), "heap-delta-B")
		b.ReportMetric(float64(after.HeapAlloc), "heap-B")
		b.ReportMetric(float64(int64(after.HeapObjects)-int64(before.HeapObjects)), "heap-delta-objects")
		b.ReportMetric(float64(stats.IdentityBytes), "identity-B")
		b.ReportMetric(f.appendTime.Seconds(), "fixture-append-s")
		b.ReportMetric(f.flushTime.Seconds(), "fixture-flush-s")
		b.ReportMetric(float64(cpuProcessRSS()), "rss-B")
		b.ReportMetric(float64(len(f.engine.parts)), "parts")
		f.reportDiskBytes(b)
		runtime.KeepAlive(f)
		previous = f
	}
}

func cpuProcessRSS() int64 {
	if runtime.GOOS != "linux" {
		return 0
	}
	data, err := os.ReadFile("/proc/self/statm")
	if err != nil {
		return 0
	}
	fields := strings.Fields(string(data))
	if len(fields) < 2 {
		return 0
	}
	pages, err := strconv.ParseInt(fields[1], 10, 64)
	if err != nil {
		return 0
	}
	return pages * int64(os.Getpagesize())
}

// cpuQueryProfile excludes ingestion/setup and supplies allocation snapshots for pprof -base.
func cpuQueryProfile(b *testing.B) func() {
	b.Helper()
	return cpuPhaseProfile(b, os.Getenv("OTELDB_BENCH_CPU_PROFILE"))
}

func cpuPhaseProfile(b *testing.B, prefix string) func() {
	b.Helper()
	if prefix == "" {
		return func() {}
	}
	write := func(suffix, profile string) {
		file, err := os.Create(prefix + suffix)
		if err != nil {
			b.Fatal(err)
		}
		writeErr := pprof.Lookup(profile).WriteTo(file, 0)
		closeErr := file.Close()
		if writeErr != nil {
			b.Fatal(writeErr)
		}
		if closeErr != nil {
			b.Fatal(closeErr)
		}
	}
	// Allocation profiles can lag by two GC cycles.
	runtime.GC()
	runtime.GC()
	write(".allocs.before", "allocs")
	file, err := os.Create(prefix + ".cpu")
	if err != nil {
		b.Fatal(err)
	}
	if err := pprof.StartCPUProfile(file); err != nil {
		_ = file.Close()
		b.Fatal(err)
	}
	return func() {
		pprof.StopCPUProfile()
		if err := file.Close(); err != nil {
			b.Fatal(err)
		}
		runtime.GC()
		runtime.GC()
		write(".allocs.after", "allocs")
		write(".heap.after", "heap")
	}
}

func (f *cpuFixture) reportDiskBytes(b *testing.B) {
	b.Helper()
	var total, index, identity int64
	if err := filepath.WalkDir(filepath.Join(f.root, "parts"), func(_ string, entry fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if entry.IsDir() {
			return nil
		}
		info, err := entry.Info()
		if err != nil {
			return err
		}
		total += info.Size()
		switch entry.Name() {
		case "series-index":
			index += info.Size()
		case "identity":
			identity += info.Size()
		}
		return nil
	}); err != nil {
		b.Fatal(err)
	}
	b.ReportMetric(float64(total), "parts-disk-B")
	b.ReportMetric(float64(index), "index-disk-B")
	b.ReportMetric(float64(identity), "identity-disk-B")
}
