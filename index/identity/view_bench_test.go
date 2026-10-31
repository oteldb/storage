package identity_test

import (
	"fmt"
	"slices"
	"strconv"
	"testing"

	"github.com/oteldb/storage/index/identity"
	"github.com/oteldb/storage/index/series"
	"github.com/oteldb/storage/signal"
)

func encodingView(n int, shape string) (series.View, []uint32, int64) {
	index := series.New()
	refs := make([]uint32, n)
	var logical int64
	for i := range n {
		s := signal.Series{
			Resource: signal.Resource{Attributes: signal.NewAttributes(kv("service.name", str("node")))},
			Scope:    signal.Scope{Name: []byte("collector"), Version: []byte("1")},
			Attributes: signal.NewAttributes(
				kv("__name__", str("node_cpu_seconds_total")),
				kv("instance", str("host-"+strconv.Itoa(i/64))),
				kv("cpu", signal.IntValue(int64(i%64))),
				kv("mode", str("idle")),
			),
		}
		switch shape {
		case "contexts":
			s.Resource.Attributes = signal.NewAttributes(kv("instance", str(strconv.Itoa(i))))
		case "nested":
			s.Attributes = append(s.Attributes, kv("nested", signal.MapValue(
				kv("array", signal.SliceValue(signal.IntValue(int64(i)), str("value"), signal.BoolValue(true))),
				kv("bytes", signal.BytesValue([]byte{0, 1, 2, 3})),
			)))
			s.Attributes = signal.NewAttributes(s.Attributes...)
		}
		index.Add(s)
		refs[i] = uint32(i)
		logical += int64(len(s.AppendHashInput(nil)))
	}
	view := index.View()
	slices.SortFunc(refs, func(a, b uint32) int { return view.ID(int(a)).Compare(view.ID(int(b))) })
	return view, refs, logical
}

func BenchmarkEncodeRegistry(b *testing.B) {
	for _, shape := range []string{"flat", "contexts", "nested"} {
		for _, n := range []int{1024, 65536} {
			b.Run(fmt.Sprintf("%s/series=%d", shape, n), func(b *testing.B) {
				view, refs, logical := encodingView(n, shape)
				for _, path := range []string{"materialized", "encoded"} {
					b.Run(path, func(b *testing.B) {
						b.ReportAllocs()
						b.SetBytes(logical)
						for b.Loop() {
							if path == "encoded" {
								_ = identity.EncodeView(nil, view, refs)
							} else {
								_ = encodeMaterialized(nil, view, refs)
							}
						}
					})
				}
			})
		}
	}
}
