package engine

import (
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
