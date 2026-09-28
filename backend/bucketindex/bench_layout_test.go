package bucketindex_test

import (
	"testing"

	"github.com/oteldb/storage/backend/bucketindex"
)

// benchIndexWithLineage is [benchIndex] as a metric shard's index is: every part records its rollup
// layout, and the catalog holds a long history of split groups.
func benchIndexWithLineage() *bucketindex.Index {
	ix := benchIndex()

	for i := range ix.Entries {
		ix.Entries[i].Rollup = &bucketindex.Rollup{Tiers: []bucketindex.RollupTier{
			{Before: 1 << 50, Interval: 60e9, Agg: 1},
			{Before: 1 << 49, Interval: 3600e9, Agg: 1},
		}}
	}

	for i := range uint64(256) {
		ix.Catalog = append(ix.Catalog, bucketindex.Claim{
			Blocks: bucketindex.Blocks(4*i+1, 4*i+2), Group: bucketindex.Blocks(4096+2*i, 4097+2*i),
		})
	}

	ix.Catalog = bucketindex.MergeLineage(ix.Catalog)

	return ix
}

func BenchmarkIndexEncodeWithLineage(b *testing.B) {
	ix := benchIndexWithLineage()
	buf := ix.AppendBinary(nil)

	b.SetBytes(int64(len(buf)))
	b.ReportAllocs()

	for b.Loop() {
		buf = ix.AppendBinary(buf[:0])
	}
}

func BenchmarkIndexDecodeWithLineage(b *testing.B) {
	data := benchIndexWithLineage().AppendBinary(nil)

	b.SetBytes(int64(len(data)))
	b.ReportAllocs()

	for b.Loop() {
		if _, err := bucketindex.Decode(data); err != nil {
			b.Fatal(err)
		}
	}
}
