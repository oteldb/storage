package engine

import (
	"strconv"
	"testing"

	"github.com/oteldb/storage/backend/backendtest"
	backendfile "github.com/oteldb/storage/backend/file"
	"github.com/oteldb/storage/block"
)

func BenchmarkMetadataCacheUnderSampleChurn(b *testing.B) {
	const chunks = 10_000
	ctx := b.Context()
	files, err := backendfile.New(b.TempDir())
	if err != nil {
		b.Fatal(err)
	}
	be := backendtest.NewSizedByteCounter(files)
	writer := block.NewStreamWriterTo(ctx, be, "part", block.WithIndependentGroups(), block.WithSizingStats())
	if err := writer.AddColumn(block.Column{Name: colTs, Kind: block.KindInt64, Block: true}); err != nil {
		b.Fatal(err)
	}
	for i := range chunks {
		values := [2]int64{int64(i), int64(i + 1)}
		if err := writer.AppendInt64(0, values[:]); err != nil {
			b.Fatal(err)
		}
		if err := writer.EndGroup(); err != nil {
			b.Fatal(err)
		}
	}
	if err := block.WriteStreamPart(ctx, be, "part", writer); err != nil {
		b.Fatal(err)
	}
	reader, err := block.OpenPart(ctx, be, "part")
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() { _ = reader.Close() })
	directory, err := reader.ColumnBlocks(ctx, colTs)
	if err != nil {
		b.Fatal(err)
	}
	metadataBytes := directory.MetadataBytes()
	e := &Engine{blockCache: newBlockCache(metadataBytes * 3)}
	p := &part{prefix: "part", reader: reader}
	be.Reset()
	b.ReportAllocs()
	for b.Loop() {
		entry, err := e.partBlockMetadata(ctx, p, colTs, colTsID)
		if err != nil {
			b.Fatal(err)
		}
		e.blockCache.release(entry)
		for i := range 8 {
			// Charge a sample scan without measuring sample decoding or buffer construction.
			sample := e.blockCache.insert(&blockEntry{key: blockKey{prefix: "scan", col: colTsID, blk: i}, bytes: metadataBytes})
			e.blockCache.release(sample)
		}
	}
	b.ReportMetric(float64(be.Reads())/float64(b.N), "reads/op")
	b.ReportMetric(float64(be.Bytes())/float64(b.N), "readB/op")
}

func BenchmarkMetadataCacheWorkingSet(b *testing.B) {
	for _, parts := range []int{8, 16} {
		b.Run(strconv.Itoa(parts)+"_parts", func(b *testing.B) {
			for _, fit := range []bool{true, false} {
				name := "over_budget"
				if fit {
					name = "fits_budget"
				}
				b.Run(name, func(b *testing.B) {
					be, inputs, metadataBytes := metadataCacheParts(b, parts, 10_000)
					budget := metadataBytes / 2
					if fit {
						budget = metadataBytes * 3
					}
					e := &Engine{blockCache: newBlockCache(budget)}
					be.Reset()
					b.ReportAllocs()
					for b.Loop() {
						for _, p := range inputs {
							entry, err := e.partBlockMetadata(b.Context(), p, colTs, colTsID)
							if err != nil {
								b.Fatal(err)
							}
							e.blockCache.release(entry)
							for i := range 8 {
								// Model sample-cache pollution without timing sample decoding.
								sample := e.blockCache.insert(&blockEntry{
									key: blockKey{prefix: "scan", col: colTsID, blk: i}, bytes: budget / 4,
								})
								e.blockCache.release(sample)
							}
						}
					}
					b.ReportMetric(float64(metadataBytes), "directory-working-set-B")
					b.ReportMetric(float64(budget), "cache-budget-B")
					b.ReportMetric(float64(be.Reads())/float64(b.N*parts), "reads/part")
					b.ReportMetric(float64(be.Bytes())/float64(b.N*parts), "readB/part")
				})
			}
		})
	}
}

func metadataCacheParts(b *testing.B, count, chunks int) (backendtest.SizedByteCounter, []*part, int64) {
	b.Helper()
	files, err := backendfile.New(b.TempDir())
	if err != nil {
		b.Fatal(err)
	}
	be := backendtest.NewSizedByteCounter(files)
	parts := make([]*part, 0, count)
	var bytes int64
	for i := range count {
		prefix := "part-" + strconv.Itoa(i)
		writer := block.NewStreamWriterTo(b.Context(), be, prefix, block.WithIndependentGroups(), block.WithSizingStats())
		if err := writer.AddColumn(block.Column{Name: colTs, Kind: block.KindInt64, Block: true}); err != nil {
			writer.Abort()
			b.Fatal(err)
		}
		for j := range chunks {
			values := [2]int64{int64(i*chunks + j), int64(i*chunks + j + 1)}
			if err := writer.AppendInt64(0, values[:]); err != nil {
				writer.Abort()
				b.Fatal(err)
			}
			if err := writer.EndGroup(); err != nil {
				writer.Abort()
				b.Fatal(err)
			}
		}
		err := block.WriteStreamPart(b.Context(), be, prefix, writer)
		writer.Abort()
		if err != nil {
			b.Fatal(err)
		}
		reader, err := block.OpenPart(b.Context(), be, prefix)
		if err != nil {
			b.Fatal(err)
		}
		b.Cleanup(func() { _ = reader.Close() })
		directory, err := reader.ColumnBlocks(b.Context(), colTs)
		if err != nil {
			b.Fatal(err)
		}
		bytes += directory.MetadataBytes()
		parts = append(parts, &part{prefix: prefix, reader: reader})
	}
	return be, parts, bytes
}
