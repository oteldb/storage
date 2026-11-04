package recordengine

import (
	"bytes"
	"io/fs"
	"strconv"
	"testing"

	"github.com/oteldb/storage/internal/vfs"
	"github.com/oteldb/storage/internal/vfs/faultfs"
	"github.com/oteldb/storage/signal"
	"github.com/oteldb/storage/wal"
)

// The WAL still frames, checksums and writes each batch; discard its file bytes to keep the CPU
// benchmark's resident footprint bounded independently of b.N. BenchmarkAppendBatchWAL uses real files.
type appendBenchFS struct{ vfs.FS }

type appendBenchFile struct{ vfs.File }

func (f appendBenchFS) OpenFile(name string, flag int, mode fs.FileMode) (vfs.File, error) {
	file, err := f.FS.OpenFile(name, flag, mode)
	if err != nil {
		return nil, err
	}
	return appendBenchFile{file}, nil
}

func (appendBenchFile) Write(p []byte) (int, error) { return len(p), nil }

func BenchmarkAppendBatchColumns(b *testing.B) {
	cases := []struct {
		name string
		rows int
		wide bool
	}{
		{"1", 1, false}, {"16", 16, false}, {"32", 32, false}, {"64", 64, false},
		{"1000", 1000, false}, {"4096", 4096, false}, {"4096-wide-attrs", 4096, true},
	}
	for _, tc := range cases {
		for _, logged := range []bool{false, true} {
			b.Run(tc.name+"/wal="+strconv.FormatBool(logged), func(b *testing.B) {
				cfg := Config{Schema: headTestSchema}
				batch := payloadBatch(tc.rows)
				if tc.wide {
					cfg.Schema, batch = wideAppendBenchBatch(tc.rows)
				}
				if logged {
					w, err := wal.CreateFS(appendBenchFS{faultfs.New()}, 0)
					if err != nil {
						b.Fatal(err)
					}
					b.Cleanup(func() { _ = w.Close() })
					cfg.WAL = w
				}
				e := New(cfg)
				if _, err := e.AppendBatch(batch, AppendLimits{}); err != nil {
					b.Fatal(err)
				}
				c := e.head.records[batch.Stream]
				b.SetBytes(batch.ByteSize())
				b.ReportAllocs()
				for b.Loop() {
					c.prepare(cfg.Schema, tc.rows, c.sel)
					e.head.bytes = 0
					if _, err := e.AppendBatch(batch, AppendLimits{}); err != nil {
						b.Fatal(err)
					}
				}
			})
		}
	}
}

func wideAppendBenchBatch(rows int) (*Schema, *Batch) {
	columns := make([]Column, 0, 9)
	for i := range 4 {
		columns = append(columns, Column{Name: "int" + strconv.Itoa(i), Kind: KindInt64})
	}
	for i := range 5 {
		columns = append(columns, Column{Name: "bytes" + strconv.Itoa(i), Kind: KindBytes})
	}
	schema := NewSchema(columns...)
	times := make([]int64, rows)
	batch := testBatchColumns(schema, times)
	attrs := make(signal.Attributes, 32)
	for i := range attrs {
		attrs[i] = signal.KeyValue{
			Key: []byte("attribute" + strconv.Itoa(i)), Value: signal.StringValue(bytes.Repeat([]byte("v"), 32)),
		}
	}
	blob := attrs.AppendHashInput(nil)
	for i := range times {
		times[i] = int64(i)
		batch.Bytes[0][i] = []byte("INFO")
		batch.Bytes[1][i] = []byte("GET /api/v1/thing status=200 handler=list done")
		batch.Bytes[2][i] = []byte("0123456789abcdef")
		batch.Bytes[3][i] = []byte("01234567")
		batch.Bytes[4][i] = blob
	}
	return schema, batch
}
