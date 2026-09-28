package bucketindex_test

import (
	"fmt"
	"testing"

	"github.com/oteldb/storage/backend/bucketindex"
)

// benchIndex is a shard's index at a realistic live part count: mostly merged parts over contiguous
// runs, a few flushes, and some tombstones.
func benchIndex() *bucketindex.Index {
	ix := &bucketindex.Index{Generation: bucketindex.Generation{Term: 1 << 20, Counter: 1 << 10}}

	next := uint64(1)
	for i := range 100 {
		width := uint64(1)
		if i%4 != 0 {
			width = 16
		}

		nums := make([]uint64, 0, width)
		for range width {
			nums = append(nums, next)
			next++
		}

		ix.Add(bucketindex.Entry{
			Prefix:  fmt.Sprintf("default/metrics/01M3MM88TN9KYPAV6QS8F%05d", i),
			MinTime: int64(i) << 30, MaxTime: int64(i+1) << 30,
			Blocks: bucketindex.Blocks(nums...), Level: uint32(i % 3),
		})
	}

	for i := range 512 {
		ix.Tombstone(bucketindex.Removal{Prefix: fmt.Sprintf("default/metrics/dead%05d", i)})
	}

	return ix
}

func BenchmarkIndexEncode(b *testing.B) {
	ix := benchIndex()
	buf := ix.AppendBinary(nil)

	b.SetBytes(int64(len(buf)))
	b.ReportAllocs()

	for b.Loop() {
		buf = ix.AppendBinary(buf[:0])
	}
}

func BenchmarkIndexDecode(b *testing.B) {
	data := benchIndex().AppendBinary(nil)

	b.SetBytes(int64(len(data)))
	b.ReportAllocs()

	for b.Loop() {
		if _, err := bucketindex.Decode(data); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkTrimWants is the identity relation a commit runs per want over the live set.
func BenchmarkTrimWants(b *testing.B) {
	ix := benchIndex()

	wants := make([]bucketindex.Want, 0, 16)
	for i := range 16 {
		wants = append(wants, bucketindex.Want{Prefix: fmt.Sprintf("lost%02d", i), Blocks: bucketindex.Blocks(uint64(2000 + i))})
	}

	b.ReportAllocs()

	for b.Loop() {
		bucketindex.TrimWants(append([]bucketindex.Want(nil), wants...), ix.Entries)
	}
}
