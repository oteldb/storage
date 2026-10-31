package identity_test

import (
	"math"
	"slices"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/index/identity"
	"github.com/oteldb/storage/index/series"
	"github.com/oteldb/storage/signal"
)

func encodeMaterialized(dst []byte, view series.View, refs []uint32) []byte {
	return identity.EncodeSorted(dst, len(refs), func(i int, dst signal.Attributes) series.Entry {
		ref := int(refs[i])
		return series.Entry{ID: view.ID(ref), Series: view.At(ref, dst)}
	})
}

func TestEncodeViewMatchesMaterialized(t *testing.T) {
	t.Parallel()
	for _, tt := range []struct {
		name   string
		series []signal.Series
	}{
		{name: "empty"},
		{name: "no attributes", series: []signal.Series{{}}},
		{name: "shared", series: []signal.Series{mkEntry(0).Series, mkEntry(1).Series, mkEntry(2).Series}},
		{name: "all kinds", series: []signal.Series{{
			Resource: signal.Resource{SchemaURL: []byte{byte(signal.KindStr), 0}, Attributes: signal.NewAttributes(
				kv("resource", signal.MapValue(kv("nested", signal.IntValue(-1)))))},
			Scope: signal.Scope{Name: []byte("same"), Version: []byte("same"), SchemaURL: []byte("schema"),
				Attributes: signal.NewAttributes(kv("scope", signal.SliceValue(signal.BoolValue(false))))},
			Attributes: signal.NewAttributes(
				kv("empty", signal.EmptyValue()), kv("str", str("")), kv("nul", str("\x00\xff")),
				kv("int", signal.IntValue(math.MinInt64)), kv("max", signal.IntValue(math.MaxInt64)),
				kv("false", signal.BoolValue(false)), kv("true", signal.BoolValue(true)),
				kv("zero", signal.DoubleValue(math.Copysign(0, -1))),
				kv("nan", signal.DoubleValue(math.Float64frombits(0x7ff8000000001234))),
				kv("inf", signal.DoubleValue(math.Inf(1))), kv("bytes", signal.BytesValue([]byte{0, 255})),
				kv("array", signal.SliceValue(str("x"), signal.MapValue(kv("k", signal.BoolValue(true))))),
				kv("map", signal.MapValue(kv("z", signal.SliceValue()), kv("a", signal.MapValue()))),
				kv("duplicate", str("first")), kv("duplicate", signal.IntValue(2)),
			),
		}}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			index := series.New()
			for _, s := range tt.series {
				index.Add(s)
			}
			view := index.View()
			refs := make([]uint32, view.Len())
			for i := range refs {
				refs[i] = uint32(i)
			}
			slices.SortFunc(refs, func(a, b uint32) int { return view.ID(int(a)).Compare(view.ID(int(b))) })
			if len(refs) > 0 {
				refs = append(refs[:1], refs...)
			}
			prefix := []byte("caller-prefix")
			want := encodeMaterialized(slices.Clone(prefix), view, refs)
			got := identity.EncodeView(slices.Clone(prefix), view, refs)
			require.Equal(t, want, got, "part-local symbol order, sections and CRCs must be identical")
			decoded := collect(t, got[len(prefix):])
			require.Len(t, decoded, view.Len())
			for i := range view.Len() {
				require.True(t, decoded[view.ID(i)].Equal(view.At(i, nil)))
			}
			index.Add(mkEntry(100).Series)
			require.Equal(t, got, identity.EncodeView(slices.Clone(prefix), view, refs), "later registration cannot change a captured view")
		})
	}
}

func FuzzEncodeViewParity(f *testing.F) {
	f.Add("key", "value", int64(1), uint64(0x7ff8000000001234))
	f.Add("", "\x00\xff", int64(math.MinInt64), uint64(1<<63))
	f.Fuzz(func(t *testing.T, key, value string, number int64, bits uint64) {
		index := series.New()
		for i := range 3 {
			index.Add(signal.Series{
				Resource: signal.Resource{SchemaURL: []byte(value), Attributes: signal.NewAttributes(kv(key, str(value)))},
				Scope:    signal.Scope{Name: []byte(key), Attributes: signal.NewAttributes(kv("n", signal.IntValue(int64(i))))},
				Attributes: signal.NewAttributes(
					kv(key, signal.MapValue(kv("array", signal.SliceValue(str(value), signal.IntValue(number))))),
					kv("float", signal.DoubleValue(math.Float64frombits(bits))), kv("bytes", signal.BytesValue([]byte(value))),
				),
			})
		}
		view := index.View()
		refs := []uint32{0, 1, 2}
		slices.SortFunc(refs, func(a, b uint32) int { return view.ID(int(a)).Compare(view.ID(int(b))) })
		want := encodeMaterialized(nil, view, refs)
		got := identity.EncodeView(nil, view, refs)
		require.Equal(t, want, got)
		require.NoError(t, identity.Decode(got, func(id signal.SeriesID, s signal.Series) error {
			require.Equal(t, id, s.Hash())
			return nil
		}))
	})
}

func TestEncodeViewSubsetDuringRegistration(t *testing.T) {
	t.Parallel()
	index := series.New()
	for i := range 128 {
		index.Add(mkEntry(i).Series)
	}
	index.Add(signal.Series{Attributes: signal.NewAttributes(kv("unselected", str(strings.Repeat("large", 1<<16))))})
	view := index.View()
	refs := []uint32{7, 19, 55}
	slices.SortFunc(refs, func(a, b uint32) int { return view.ID(int(a)).Compare(view.ID(int(b))) })
	want := encodeMaterialized(nil, view, refs)
	done := make(chan struct{})
	go func() {
		defer close(done)
		for i := 128; i < 2048; i++ {
			index.Add(mkEntry(i).Series)
		}
	}()
	t.Cleanup(func() { <-done })
	for range 20 {
		got := identity.EncodeView(nil, view, refs)
		require.Equal(t, want, got)
		require.Less(t, len(got), 4096, "unselected registry symbols must not enter the part")
	}
}
