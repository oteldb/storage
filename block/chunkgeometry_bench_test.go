package block

import (
	"encoding/binary"
	"hash/crc32"
	"strconv"
	"testing"

	"github.com/oteldb/storage/encoding/compress"
)

func chunkGeometryFixture(tb testing.TB, chunks int, mixed bool) ([]byte, int64, dirCheck) {
	tb.Helper()
	a := blockAccum{variable: true, timeBounds: true, frameCount: chunks,
		gLens: make([]int, chunks), chunkRows: make([]int, chunks),
		minKeys: make([]int64, chunks), maxKeys: make([]int64, chunks)}
	const base = int64(1700000000000000000)
	var rows int
	maxTime := base
	for i := range chunks {
		n := 1
		if mixed {
			n += i % 16
		}
		a.gLens[i], a.chunkRows[i] = 64, n
		a.minKeys[i] = base + int64(i%32)*15000000000
		a.maxKeys[i] = a.minKeys[i] + int64(n-1)*15000000000
		maxTime = max(maxTime, a.maxKeys[i])
		rows += n
		a.frameDir = appendFrameMetadata(a.frameDir, 1, 32, uint32(i))
	}
	raw := a.encodeDir(1024)
	comp := compress.NewCompressor(compress.AlgorithmLZ4, 0)
	packed := binary.AppendUvarint(nil, uint64(len(raw)))
	packed = comp.Compress(packed, raw)
	packed = binary.LittleEndian.AppendUint32(packed, crc32.Checksum(packed, castagnoli))
	desc := ColumnDesc{Kind: KindInt64, Blocked: true, Framed: true, Checked: true,
		MaxBlockRows: 1024, TimeBounds: true, CompactDir: true, HasSizing: true,
		MinInt64: base, MaxInt64: maxTime, Sizing: ColumnSizing{NumGranules: int64(chunks),
			NumFrames: int64(chunks), MaxFrameBytes: 32, MaxFrameRaw: 64, MaxGranuleRaw: 64,
			StreamRaw: int64(chunks) * 64, DirLen: int64(len(packed))}}
	chk := newDirCheck(desc, decodeLimits{rows: rows, frames: unlimited})
	chk.dirComp = comp
	return packed, int64(chunks) * 32, chk
}

func BenchmarkSampleDirectory(b *testing.B) {
	for _, chunks := range []int{65536, 1048576} {
		for _, mixed := range []bool{false, true} {
			b.Run(strconv.Itoa(chunks)+"/mixed="+strconv.FormatBool(mixed), func(b *testing.B) {
				raw, frameBytes, chk := chunkGeometryFixture(b, chunks, mixed)
				b.Run("open", func(b *testing.B) {
					b.SetBytes(int64(chk.rows) * 8)
					b.ReportAllocs()
					var resident int64
					for b.Loop() {
						d, err := parseDetachedDir(raw, frameBytes, chk)
						if err != nil {
							b.Fatal(err)
						}
						resident = d.residentBytes()
					}
					b.ReportMetric(float64(resident), "resident-B/op")
				})
				b.Run("lookup", func(b *testing.B) {
					dir, err := parseDetachedDir(raw, frameBytes, chk)
					if err != nil {
						b.Fatal(err)
					}
					d := Decoder{rows: chk.rows, streams: newBlockStreams(dir, chk.dirComp)}
					b.SetBytes(32)
					b.ReportAllocs()
					var at uint32
					for b.Loop() {
						at = at*1664525 + 1013904223
						index := int(at) % chunks
						lo, hi := d.BlockSpan(index)
						minT, maxT, ok := d.BlockTimeBounds(index)
						if lo >= hi || minT > maxT || !ok {
							b.Fatal("invalid geometry")
						}
					}
				})
			})
		}
	}
}
