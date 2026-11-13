package engine

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/base64"
	"encoding/json"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"testing"

	"github.com/go-faster/errors"

	backendfile "github.com/oteldb/storage/backend/file"
	"github.com/oteldb/storage/index/series"
	"github.com/oteldb/storage/signal"
	"github.com/oteldb/storage/wal"
)

func capturedEngineInput(b *testing.B) [][]signal.Series {
	b.Helper()
	raw, err := os.ReadFile(filepath.Join("..", "index", "series", "testdata", "node-exporter-series.json.gz"))
	if err != nil {
		b.Fatal(err)
	}
	z, err := gzip.NewReader(bytes.NewReader(raw))
	if err != nil {
		b.Fatal(err)
	}
	defer func() { _ = z.Close() }()
	var fixture struct {
		Series []string `json:"series"`
	}
	if err := json.NewDecoder(z).Decode(&fixture); err != nil {
		b.Fatal(err)
	}
	groups := make([][]signal.Series, 8)
	for host := range 64 {
		instance := signal.StringValue([]byte("node-" + strconv.Itoa(host) + ":9100"))
		for _, encoded := range fixture.Series {
			data, err := base64.StdEncoding.DecodeString(encoded)
			if err != nil {
				b.Fatal(err)
			}
			s, _, err := signal.DecodeSeries(data)
			if err != nil {
				b.Fatal(err)
			}
			for i := range s.Attributes {
				if bytes.Equal(s.Attributes[i].Key, []byte("instance")) {
					s.Attributes[i].Value = instance
				}
			}
			h := s.Hash().Lo % 8
			groups[h] = append(groups[h], s)
		}
	}
	return groups
}

// BenchmarkCapturedExporterEngine includes WAL, bounded head rotation and native-file flushes.
// Input decoding/hashing and engine creation are excluded; each pass writes two scrape generations.
func BenchmarkCapturedExporterEngine(b *testing.B) {
	groups := capturedEngineInput(b)
	for _, producers := range []int{1, 2} {
		for _, enabled := range []bool{false, true} {
			b.Run("producers="+strconv.Itoa(producers)+"/templates="+strconv.FormatBool(enabled), func(b *testing.B) {
				var pool *series.AttributeTemplates
				if enabled {
					pool = series.NewAttributeTemplates(8<<20, []byte("instance"), []byte("cpu"), []byte("mode"), []byte("device"), []byte("mountpoint"), []byte("quantile"))
				}
				ids := make([][]signal.SeriesID, len(groups))
				for h := range groups {
					for i := range groups[h] {
						ids[h] = append(ids[h], groups[h][i].Hash())
					}
				}
				var parts int
				b.ReportAllocs()
				for b.Loop() {
					b.StopTimer()
					root := b.TempDir()
					engines := make([]*Engine, len(groups))
					b.Cleanup(func() {
						for _, e := range engines {
							if e != nil {
								_ = e.Close(context.WithoutCancel(b.Context()))
							}
						}
					})
					for h := range engines {
						files, err := backendfile.New(filepath.Join(root, strconv.Itoa(h), "parts"))
						if err != nil {
							b.Fatal(err)
						}
						journal, err := wal.Create(filepath.Join(root, strconv.Itoa(h), "wal"), 0)
						if err != nil {
							b.Fatal(err)
						}
						journal.SetSync(true)
						engines[h] = New(Config{Backend: files, WAL: journal, Prefix: "exporter", DiskSeries: true, SeparateSeries: true,
							MetricSeriesChunks: true, ResidentHeadBytes: 512 << 10, SeriesCacheBytes: 8 << 20,
							FlushMemoryBytes: 64 << 20, DecodeMemoryBytes: 128 << 20, SeriesTemplates: pool})
					}
					b.StartTimer()
					errs := make(chan error, len(engines)*producers)
					var wg sync.WaitGroup
					for h := range engines {
						for producer := range producers {
							start, end := len(groups[h])*producer/producers, len(groups[h])*(producer+1)/producers
							wg.Go(func() {
								errs <- appendCapturedGenerations(b.Context(), engines[h], groups[h][start:end], ids[h][start:end])
							})
						}
					}
					wg.Wait()
					b.StopTimer()
					close(errs)
					for err := range errs {
						if err != nil {
							b.Fatal(err)
						}
					}
					for h, e := range engines {
						if e.SeriesCount() != len(groups[h]) {
							b.Fatal("active cardinality changed")
						}
						parts += e.PartCount()
						if err := e.Close(b.Context()); err != nil {
							b.Fatal(err)
						}
					}
					if err := os.RemoveAll(root); err != nil {
						b.Fatal(err)
					}
					b.StartTimer()
				}
				b.ReportMetric(float64(parts)/float64(b.N), "parts/op")
				b.ReportMetric(float64(pool.SizeBytes()), "pool-B")
			})
		}
	}
}

func appendCapturedGenerations(ctx context.Context, e *Engine, input []signal.Series, ids []signal.SeriesID) error {
	for generation := range 2 {
		for start := 0; start < len(input); start += 4096 {
			end := min(start+4096, len(input))
			ts, values := make([]int64, end-start), make([]float64, end-start)
			for i := range ts {
				ts[i], values[i] = 1700000000000000000+int64(generation)*15000000000, 1
			}
			result, err := e.AppendBatch(ids[start:end], ts, values, nil, func(i int) signal.Series { return input[start+i] }, AppendLimits{})
			if err != nil {
				return err
			}
			if result.Accepted != end-start {
				return errors.New("captured exporter sample rejected")
			}
		}
		if err := e.Flush(ctx); err != nil {
			return err
		}
	}
	return nil
}
