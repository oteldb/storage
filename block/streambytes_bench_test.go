package block

import (
	"context"
	"encoding/binary"
	"fmt"
	"testing"

	"github.com/oteldb/storage/backend/backendtest"
	"github.com/oteldb/storage/encoding/chunk"
)

// benchShapes are [BenchmarkBuildColumnBytesForm]'s two populations: attribute-like values every
// granule joins D with, and body-like values every granule declines.
var benchShapes = []struct {
	name string
	cell func(i int) []byte
}{
	{"shared", func(i int) []byte {
		return fmt.Appendf(nil, "svc=api pod=worker-%03d level=info handler=/v1/resource", i%512)
	}},
	{"selfencoded", func(i int) []byte {
		return fmt.Appendf(nil, "request %d completed in %dms for tenant %d", i, i%977, i*7)
	}},
}

// BenchmarkStreamWriterBytes encodes one streamed bytes column, the merge's writer, from copied
// values and through a binding over the source's dictionary, which hashes a value once per granule
// instead of once per row. Throughput is over the logical bytes, comparable with
// [BenchmarkBuildColumnBytesForm].
func BenchmarkStreamWriterBytes(b *testing.B) {
	const (
		rows  = 1 << 16
		batch = 1024
	)

	for _, shape := range benchShapes {
		cells := make([][]byte, rows)
		for i := range cells {
			cells[i] = shape.cell(i)
		}

		var logical int64
		for _, v := range cells {
			logical += int64(len(v))
		}

		entries, ids := splitBytesForm(cells)
		dc := &chunk.DictColumn{Entries: entries, IDWidth: 2}

		for _, id := range ids {
			dc.IDs = binary.BigEndian.AppendUint16(dc.IDs, uint16(id))
		}

		for _, feed := range []bytesFeed{feedValues, feedBind} {
			b.Run(shape.name+"/"+feed.String(), func(b *testing.B) {
				b.SetBytes(logical)
				b.ReportAllocs()

				for b.Loop() {
					w := NewStreamWriterTo(context.Background(), backendtest.NewStreamingMemory(), "p", WithGranuleSize(8192))
					if err := w.AddColumn(Column{Name: "c", Kind: KindBytes, Block: true}); err != nil {
						b.Fatal(err)
					}

					var (
						bnd *Binding
						gen DictGen
						err error
					)

					if feed == feedBind {
						if bnd, err = w.Binding(0); err != nil {
							b.Fatal(err)
						}

						gen = NewDictGen()
						if err := bnd.BindStable(entries, gen); err != nil {
							b.Fatal(err)
						}
					}

					for lo := 0; lo < rows; lo += batch {
						if feed == feedBind {
							err = bnd.AppendDict(OwnedGranule(dc, gen), lo, lo+batch, nil)
						} else {
							err = w.AppendBytes(0, cells[lo:lo+batch])
						}

						if err != nil {
							b.Fatal(err)
						}
					}

					if _, err := w.build(); err != nil {
						b.Fatal(err)
					}
				}
			})
		}
	}
}

// BenchmarkStreamWriterToBytesResident reports a streamed bytes column's resident footprint at the
// end of parts of growing size: flat in rows, where a buffered writer's tracks the part.
func BenchmarkStreamWriterToBytesResident(b *testing.B) {
	const batch = 4096

	for _, shape := range benchShapes {
		for _, rows := range []int{1 << 16, 1 << 18, 1 << 20} {
			b.Run(fmt.Sprintf("%s/rows=%d", shape.name, rows), func(b *testing.B) {
				vals := make([][]byte, batch)

				var resident int64

				b.ReportAllocs()

				for b.Loop() {
					w := NewStreamWriterTo(context.Background(), backendtest.NewStreamingMemory(), "p")
					if err := w.AddColumn(Column{Name: "c", Kind: KindBytes, Codec: chunk.CodecDict, Block: true}); err != nil {
						b.Fatal(err)
					}

					for lo := 0; lo < rows; lo += batch {
						for i := range vals {
							vals[i] = shape.cell(lo + i)
						}

						if err := w.AppendBytes(0, vals); err != nil {
							b.Fatal(err)
						}
					}

					resident = w.ResidentBytes()

					w.Abort()
				}

				b.ReportMetric(float64(resident), "resident-B")
			})
		}
	}
}
