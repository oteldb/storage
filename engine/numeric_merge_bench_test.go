package engine

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/json"
	"io/fs"
	"math"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	backendfile "github.com/oteldb/storage/backend/file"
	"github.com/oteldb/storage/internal/memlimit"
	"github.com/oteldb/storage/signal"
)

const (
	numericMergeInputs  = 32
	numericMergeGrant   = 1344 << 20
	numericMergePrefix  = "numeric-merge"
	numericMergeSeedCap = 2 << 30
)

// BenchmarkNumericSampleMerge times one numeric maintenance turn over a fresh copy of a native file fixture.
// Use a disk-backed TMPDIR. An optional OTELDB_BENCH_NUMERIC_MERGE_SEED directory retains the input across runs.
func BenchmarkNumericSampleMerge(b *testing.B) {
	for _, size := range []struct {
		name  string
		rows  int
		shape string
	}{
		{"small", 4096, "mixed"},
		{"medium", 32768, "mixed"},
		{"full", 366080, "mixed"},
		{"small-dense-overlap", 4096, "dense-overlap"},
		{"singleton-1", 4096, "singleton-1"},
		{"singleton-4", 4096, "singleton-4"},
		{"singleton-32", 4096, "singleton-32"},
		{"short-fragments", 4096, "short-fragments"},
	} {
		b.Run(size.name, func(b *testing.B) {
			if size.name == "full" && os.Getenv("OTELDB_BENCH_NUMERIC_MERGE_FULL") != "true" {
				b.Skip("set OTELDB_BENCH_NUMERIC_MERGE_FULL=true for the 10k-scale fixture")
			}
			benchmarkNumericMerge(b, size.name, size.rows, size.shape)
		})
	}
}

type numericMergeSeed struct {
	RowsPerPart int                `json:"rowsPerPart"`
	Samples     numericMergeDigest `json:"samples"`
}

type numericMergeDigest struct {
	Rows int64     `json:"rows"`
	Sum  [4]uint64 `json:"sum"`
}

func (d *numericMergeDigest) add(id signal.SeriesID, ts int64, value float64) {
	var row [32]byte
	binary.LittleEndian.PutUint64(row[0:8], id.Hi)
	binary.LittleEndian.PutUint64(row[8:16], id.Lo)
	binary.LittleEndian.PutUint64(row[16:24], uint64(ts))
	binary.LittleEndian.PutUint64(row[24:32], math.Float64bits(value))
	hash := sha256.Sum256(row[:])
	// The aggregate is independent of how the same rows are partitioned across outputs.
	for i := range d.Sum {
		d.Sum[i] += binary.LittleEndian.Uint64(hash[i*8:])
	}
	d.Rows++
}

func benchmarkNumericMerge(b *testing.B, name string, rows int, shape string) {
	b.Helper()
	scratch := b.TempDir()
	seed, expected := prepareNumericMergeSeed(b, scratch, name, rows, shape)
	working := filepath.Join(scratch, "working")
	profile := os.Getenv("OTELDB_BENCH_CPU_PROFILE")
	var inBytes, outBytes, partBytes, valueBytes, readBytes, retired, peak, outputSeries int64
	series := rows
	if shape == "dense-overlap" || shape == "short-fragments" {
		series /= 2
	}
	var profiled bool
	b.ReportAllocs()
	for b.Loop() {
		b.StopTimer()
		require.NoError(b, os.CopyFS(working, os.DirFS(seed)))
		e, counter := newNumericMergeEngine(b, working)
		require.NoError(b, e.LoadParts(b.Context()))
		inputs := e.Parts()
		require.Len(b, inputs, numericMergeInputs)
		for _, input := range inputs {
			require.EqualValues(b, rows, input.Rows)
			require.Equal(b, series, input.Series)
			inBytes += input.SizeBytes
		}
		counter.Reset()
		stopProfile := func() {}
		if !profiled && profile != "" {
			stopProfile = cpuPhaseProfile(b, profile+".merge")
			profiled = true
		}
		peak += runNumericMerge(b, e)
		stopProfile()
		readBytes += counter.Bytes()
		outputs := e.Parts()
		for _, input := range inputs {
			for _, output := range outputs {
				require.NotEqual(b, input.ID, output.ID)
			}
		}
		for _, output := range outputs {
			outBytes += output.SizeBytes
			outputSeries += int64(output.Series)
		}
		payload, values := numericMergeOutputBytes(b, e)
		partBytes += payload
		valueBytes += values
		retired += int64(len(inputs) - len(outputs))
		require.NoError(b, e.Close(b.Context()))
		verifyNumericMerge(b, working, expected.Samples)
		require.NoError(b, os.RemoveAll(working))
		b.StartTimer()
	}
	operations := float64(b.N)
	seconds := b.Elapsed().Seconds()
	b.ReportMetric(float64(inBytes)/operations, "input-B/op")
	b.ReportMetric(float64(outBytes)/operations, "output-B/op")
	b.ReportMetric(float64(partBytes)/operations, "part-B/op")
	b.ReportMetric(float64(valueBytes)/operations, "value-B/op")
	b.ReportMetric(float64(readBytes)/operations, "backend-read-B/op")
	b.ReportMetric(float64(peak)/operations, "modeled-peak-B/op")
	b.ReportMetric(float64(numericMergeInputs)-float64(retired)/operations, "output-parts/op")
	b.ReportMetric(float64(retired)/seconds, "net-parts/s")
	b.ReportMetric(float64(inBytes)/1e6/seconds, "input-MB/s")
	b.ReportMetric(float64(expected.Samples.Rows)*operations/seconds, "samples/s")
	b.ReportMetric(float64(expected.Samples.Rows)*operations/float64(outputSeries), "samples/series")
}

func runNumericMerge(b *testing.B, e *Engine) int64 {
	b.Helper()
	var peak int64
	mergeResidentObserver = func(bytes, _, _ int64) { peak = bytes }
	defer func() { mergeResidentObserver = nil }()
	b.StartTimer()
	changed, err := e.MergeSamplesStep(b.Context(), MergeOptions{Background: true})
	b.StopTimer()
	require.NoError(b, err)
	require.True(b, changed)
	plan := e.LastMergePlan()
	require.Equal(b, "completed", plan.Outcome)
	require.Equal(b, numericMergeInputs, plan.Selected)
	require.EqualValues(b, numericMergeGrant, plan.AllowanceBytes)
	return peak
}

func newNumericMergeEngine(b *testing.B, dir string) (*Engine, *cpuFixtureFile) {
	b.Helper()
	files, err := backendfile.New(filepath.Join(dir, "parts"))
	require.NoError(b, err)
	counter := &cpuFixtureFile{File: files}
	pool := memlimit.NewPool(numericMergeGrant)
	e := New(Config{
		Backend: counter, Prefix: numericMergePrefix,
		DiskSeries: true, SeparateSeries: true, MetricSeriesChunks: true,
		ResidentHeadBytes: 2 << 30, FlushMemoryBytes: 448 << 20, MergeMemoryBytes: numericMergeGrant,
		FlushAdmission: pool.Acquire,
		MergeAdmission: func(ctx context.Context, bytes int64, wait bool) (func(), bool, error) {
			if wait {
				release, err := pool.Acquire(ctx, bytes)
				return release, err == nil, err
			}
			release, ok := pool.TryAcquire(bytes)
			return release, ok, nil
		},
	})
	return e, counter
}

func prepareNumericMergeSeed(b *testing.B, scratch, name string, rows int, shape string) (string, numericMergeSeed) {
	b.Helper()
	seed := filepath.Join(scratch, "seed")
	if dir := os.Getenv("OTELDB_BENCH_NUMERIC_MERGE_SEED"); dir != "" {
		require.NoError(b, os.MkdirAll(dir, 0o755))
		seed = filepath.Join(dir, name)
	}
	var stats numericMergeSeed
	err := os.Mkdir(seed, 0o755)
	if os.IsExist(err) {
		data, err := os.ReadFile(filepath.Join(seed, "fixture.json"))
		require.NoError(b, err)
		require.NoError(b, json.Unmarshal(data, &stats))
		require.Equal(b, rows, stats.RowsPerPart)
		require.EqualValues(b, rows*numericMergeInputs, stats.Samples.Rows)
		checkNumericMergeSeedSize(b, seed)
		return seed, stats
	}
	require.NoError(b, err)
	complete := false
	defer func() {
		if !complete {
			_ = os.RemoveAll(seed)
		}
	}()
	e, _ := newNumericMergeEngine(b, seed)
	digest := buildNumericMergeSeed(b, e, rows, seed, shape)
	require.NoError(b, e.Close(b.Context()))
	stats = numericMergeSeed{RowsPerPart: rows, Samples: digest}
	data, err := json.Marshal(stats)
	require.NoError(b, err)
	require.NoError(b, os.WriteFile(filepath.Join(seed, "fixture.json"), data, 0o644))
	complete = true
	return seed, stats
}

func buildNumericMergeSeed(b *testing.B, e *Engine, rows int, dir, shape string) numericMergeDigest {
	b.Helper()
	const batchRows = 4096
	var digest numericMergeDigest
	identities := make([]signal.Series, batchRows)
	ids := make([]signal.SeriesID, batchRows)
	ts, values := make([]int64, batchRows), make([]float64, batchRows)
	overlap := rows / 8
	for part := range numericMergeInputs {
		for first := 0; first < rows; first += batchRows {
			n := min(batchRows, rows-first)
			for i := range n {
				offset := first + i
				series := offset
				scrape := part
				switch shape {
				case "mixed":
					series = 4*overlap + part*(rows-overlap) + offset - overlap
					if offset < overlap {
						series = (part%4)*overlap + offset
					}
				case "singleton-1":
					series += part * rows
				case "singleton-4":
					series += (part / 4) * rows
				case "dense-overlap":
					series = offset / 2
					scrape += (offset % 2) * numericMergeInputs
				case "short-fragments":
					series = offset / 2
					scrape = part*2 + offset%2
				}
				identities[i] = cpuMetricSeries(series)
				ids[i] = identities[i].Hash()
				ts[i] = cpuFixtureBase + int64(scrape)*cpuFixtureCadence + int64(series%1000)
				values[i] = float64(series%100003+part*7) / 100
				if series%2 != 0 {
					values[i] = float64((series*19+part*37)%17401) / 100
				}
				if (shape == "dense-overlap" || shape == "short-fragments") && offset%2 != 0 {
					values[i] += 0.5
				}
				digest.add(ids[i], ts[i], values[i])
			}
			result, err := e.AppendBatch(ids[:n], ts[:n], values[:n], nil,
				func(i int) signal.Series { return identities[i] }, AppendLimits{})
			require.NoError(b, err)
			require.Equal(b, n, result.Accepted)
		}
		require.NoError(b, e.Flush(b.Context()))
		require.Equal(b, part+1, e.PartCount())
		checkNumericMergeSeedSize(b, dir)
	}
	return digest
}

func numericMergeOutputBytes(b *testing.B, e *Engine) (partBytes, valueBytes int64) {
	b.Helper()
	outputs, err := e.PartsDetailed(b.Context())
	require.NoError(b, err)
	for _, output := range outputs {
		partBytes += output.Bytes
		for _, column := range output.Columns {
			if column.Name == colValue {
				valueBytes += column.Bytes
			}
		}
	}
	return partBytes, valueBytes
}

func checkNumericMergeSeedSize(b *testing.B, dir string) {
	b.Helper()
	var size int64
	require.NoError(b, filepath.WalkDir(dir, func(_ string, entry fs.DirEntry, err error) error {
		if err != nil || entry.IsDir() {
			return err
		}
		info, err := entry.Info()
		if err == nil {
			size += info.Size()
		}
		return err
	}))
	require.LessOrEqual(b, size, int64(numericMergeSeedCap), "fixture byte limit")
}

func verifyNumericMerge(b *testing.B, dir string, expected numericMergeDigest) {
	b.Helper()
	e, _ := newNumericMergeEngine(b, dir)
	ctx := b.Context()
	defer func() { require.NoError(b, e.Close(ctx)) }()
	require.NoError(b, e.LoadPartsReadOnly(ctx))
	var got numericMergeDigest
	for _, part := range e.parts {
		verifyNumericMergePart(b, part, &got)
	}
	require.Equal(b, expected, got)
}

func verifyNumericMergePart(b *testing.B, part *part, digest *numericMergeDigest) {
	b.Helper()
	ctx := b.Context()
	index, err := part.index.idSource(ctx)
	require.NoError(b, err)
	stream, err := newPartStream(ctx, part, defaultMergeReadWindow)
	require.NoError(b, err)
	defer stream.close()
	var scratch rangeBuf
	for i := range index.Len() {
		ts, values, _, err := stream.decodeRange(index.RangeAt(i), &scratch)
		require.NoError(b, err)
		id := index.At(i)
		for j, stamp := range ts {
			digest.add(id, stamp, values[j])
		}
	}
}
