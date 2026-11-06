package engine

import (
	"slices"
	"strconv"
	"testing"

	"github.com/oteldb/storage/backend/file"
	"github.com/oteldb/storage/index/series"
	"github.com/oteldb/storage/signal"
)

func BenchmarkIdentityObjectWrite(b *testing.B) {
	const count = 65_536
	index := series.New()
	refs := make([]uint32, count)
	var logical int64
	for i := range count {
		s := signal.Series{Attributes: signal.NewAttributes(
			signal.KeyValue{Key: []byte("__name__"), Value: signal.StringValue([]byte("node_cpu_seconds_total"))},
			signal.KeyValue{Key: []byte("instance"), Value: signal.StringValue([]byte("host-" + strconv.Itoa(i)))},
			signal.KeyValue{Key: []byte("cpu"), Value: signal.IntValue(int64(i % 32))},
		)}
		logical += int64(16 + len(s.AppendHashInput(nil)))
		id := index.Add(s)
		refs[i], _ = index.Ref(id)
	}
	view := index.View()
	slices.SortFunc(refs, func(a, c uint32) int { return view.ID(int(a)).Compare(view.ID(int(c))) })
	entries := identitySnapshot{view: view, refs: refs}
	for _, stream := range []bool{false, true} {
		name := "slice"
		if stream {
			name = "writer"
		}
		b.Run(name, func(b *testing.B) {
			files, err := file.New(b.TempDir())
			if err != nil {
				b.Fatal(err)
			}
			part := partStreamWriter{e: New(Config{Backend: files}), prefix: "part", streamIdentities: stream}
			b.SetBytes(logical)
			b.ReportAllocs()
			for b.Loop() {
				if err := part.writeIdentity(b.Context(), entries); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
