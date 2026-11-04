package block

import (
	"encoding/binary"
	"strconv"
	"testing"

	"github.com/oteldb/storage/encoding/compress"
)

func BenchmarkChunkDirectoryDecode(b *testing.B) {
	desc, object, limits := compactDirectoryFixture(b, compress.AlgorithmNone, true)
	end := len(object) - footerLenBytes
	start := end - int(desc.Sizing.DirLen)
	packed := object[start:end]
	comp := compress.NewCompressor(compress.AlgorithmLZ4, 0)
	n, k := binary.Uvarint(packed)
	raw, err := comp.DecompressLimit(nil, packed[k:len(packed)-dirCRCBytes], int(n))
	if err != nil {
		b.Fatal(err)
	}
	for _, compact := range []bool{false, true} {
		b.Run("compact="+strconv.FormatBool(compact), func(b *testing.B) {
			desc := desc
			dir := raw
			if compact {
				dir = packed
			}
			desc.CompactDir, desc.Sizing.DirLen = compact, int64(len(dir))
			chk := newDirCheck(desc, limits)
			chk.dirComp = comp
			b.SetBytes(int64(len(raw)))
			b.ReportAllocs()
			for b.Loop() {
				d, err := parseDetachedDir(dir, int64(start), chk)
				if err != nil {
					b.Fatal(err)
				}
				if d.granules != 128 {
					b.Fatal("wrong granule count")
				}
			}
		})
	}
}

func BenchmarkChunkDirectoryFinishEstimate(b *testing.B) {
	for _, chunks := range []int{1024, 65536} {
		b.Run(strconv.Itoa(chunks), func(b *testing.B) {
			comp := compress.NewCompressor(compress.AlgorithmNone, 0)
			c := streamColumn{variable: true, comp: comp, blk: &blockAccum{
				comp: comp, dirComp: compress.NewCompressor(compress.AlgorithmLZ4, 0),
				frameGranules: make([]int, chunks), gLens: make([]int, chunks),
			}}
			b.ReportAllocs()
			for b.Loop() {
				_, transient := c.finishBytes()
				if transient == 0 {
					b.Fatal("missing directory estimate")
				}
			}
		})
	}
}
