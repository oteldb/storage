package block

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/oteldb/storage/backend/file"
	"github.com/oteldb/storage/encoding/chunk"
	"github.com/oteldb/storage/encoding/compress"
)

// benchFrames are one column's compression frames and their decompressed sizes.
type benchFrames struct {
	name  string
	comp  *compress.Compressor
	raw   [][]byte
	sizes []int
}

func framesOf(b *testing.B, name string, desc ColumnDesc, obj []byte, comp *compress.Compressor) benchFrames {
	b.Helper()

	dir, err := newColumnReader(desc, obj, comp, 0).blockDir()
	if err != nil {
		b.Fatal(err)
	}

	out := benchFrames{name: name, comp: comp}

	for f := range len(dir.frameOff) - 1 {
		raw, err := dir.frame(f)
		if err != nil {
			b.Fatal(err)
		}

		out.raw = append(out.raw, raw)
		out.sizes = append(out.sizes, int(dir.frameRaw[f]))
	}

	return out
}

// realFrames loads every zstd framed column of the largest part under $OTELDB_BENCH_PARTS, a
// directory of parts as the file backend lays them out.
func realFrames(b *testing.B) []benchFrames {
	b.Helper()

	root := os.Getenv("OTELDB_BENCH_PARTS")
	if root == "" {
		return nil
	}

	ctx := context.Background()

	fb, err := file.New(root)
	if err != nil {
		b.Fatal(err)
	}

	manifests, err := filepath.Glob(filepath.Join(root, "*", "manifest"))
	if err != nil || len(manifests) == 0 {
		b.Fatalf("no parts under %s: %v", root, err)
	}

	var (
		best     *PartReader
		bestSize int64
	)

	for _, m := range manifests {
		r, err := OpenPart(ctx, fb, filepath.Base(filepath.Dir(m)))
		if err != nil {
			b.Fatal(err)
		}

		if r.Manifest().DiskBytes > bestSize {
			best, bestSize = r, r.Manifest().DiskBytes
		}
	}

	var out []benchFrames

	for _, name := range best.ColumnNames() {
		desc, _ := best.ColumnDescByName(name)
		if !desc.Framed || desc.Compress != compress.AlgorithmZSTD {
			continue
		}

		col, err := best.Column(ctx, name)
		if err != nil {
			b.Fatal(err)
		}

		object := col.object
		if desc.SharedDict && !desc.TrailerDict {
			if _, err := col.sharedEntries(); err != nil {
				b.Fatal(err)
			}

			object = col.sharedRest
			desc.SharedDict = false
		}

		out = append(out, framesOf(b, "real/"+name, desc, object, col.comp))
	}

	return out
}

// BenchmarkFrameDecompress compares the bounded frame decode against the unbounded one, each into a
// reused buffer: the synthetic low-cardinality id frames of [BenchmarkBytesColumnDecode], and the
// zstd columns of a real part when OTELDB_BENCH_PARTS names a directory of them.
func BenchmarkFrameDecompress(b *testing.B) {
	vals := benchVals(1<<17, 32)
	desc, obj, err := buildColumn(Column{Name: "c", Kind: KindBytes, Codec: chunk.CodecDict, Bytes: vals, Block: true},
		zstdComp(), 8192, defaultCompressBlockBytes)
	if err != nil {
		b.Fatal(err)
	}

	sets := append([]benchFrames{framesOf(b, "lowcard", desc, obj, zstdComp())}, realFrames(b)...)

	for _, set := range sets {
		var total int64
		for _, n := range set.sizes {
			total += int64(n)
		}

		b.Run(set.name+"/dec=unbounded", func(b *testing.B) {
			b.SetBytes(total)
			b.ReportAllocs()

			var buf []byte

			for b.Loop() {
				for _, raw := range set.raw {
					if buf, err = set.comp.Decompress(buf[:0], raw); err != nil {
						b.Fatal(err)
					}
				}
			}
		})

		b.Run(set.name+"/dec=bounded", func(b *testing.B) {
			b.SetBytes(total)
			b.ReportAllocs()

			var buf []byte

			for b.Loop() {
				for i, raw := range set.raw {
					if buf, err = set.comp.DecompressLimit(buf, raw, set.sizes[i]); err != nil {
						b.Fatal(err)
					}
				}
			}
		})
	}
}
