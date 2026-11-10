package identity_test

import (
	"bytes"
	"io"
	"math"
	"slices"
	"strconv"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/index/identity"
	"github.com/oteldb/storage/index/series"
	"github.com/oteldb/storage/signal"
)

func viewWriteSeries(i int, value signal.Value) signal.Series {
	return signal.Series{
		Resource: signal.Resource{
			SchemaURL: []byte("resource-schema"),
			Attributes: signal.NewAttributes(kv("service.name", str("api")),
				kv("host.id", signal.IntValue(int64(i/8)))),
		},
		Scope: signal.Scope{
			Name: []byte("scope"), Version: []byte("v1"), SchemaURL: []byte("scope-schema"),
			Attributes: signal.NewAttributes(kv("scope.attr", signal.MapValue(kv("active", signal.BoolValue(true))))),
		},
		Attributes: signal.NewAttributes(kv("__name__", str("node_cpu_seconds_total")),
			kv("instance", str("node-"+strconv.Itoa(i))), kv("cpu", signal.IntValue(0)),
			kv("mode", str("user")), kv("value", value)),
	}
}

func writeViewFixture(values []signal.Value) (series.View, []int) {
	index := series.New()
	for i, value := range values {
		index.Add(viewWriteSeries(i, value))
	}
	view := index.View()
	refs := make([]int, view.Len())
	for i := range refs {
		refs[i] = i
	}
	slices.SortFunc(refs, func(a, b int) int { return view.ID(a).Compare(view.ID(b)) })
	return view, refs
}

func viewWriteValues() []signal.Value {
	return []signal.Value{
		signal.EmptyValue(), str(""), str("a\x00b"), signal.BytesValue([]byte{0, 255}),
		signal.BoolValue(false), signal.BoolValue(true), signal.IntValue(math.MinInt64),
		signal.IntValue(math.MaxInt64), signal.DoubleValue(5), signal.DoubleValue(math.Copysign(0, -1)),
		signal.DoubleValue(math.Float64frombits(0x7ff8000000000001)), signal.DoubleValue(math.Inf(1)),
		signal.DoubleValue(math.Inf(-1)), signal.SliceValue(), signal.MapValue(),
		signal.SliceValue(signal.IntValue(1), str("x"), signal.MapValue(kv("nested", signal.BoolValue(false)))),
		signal.MapValue(kv("array", signal.SliceValue(signal.DoubleValue(-3.25), signal.EmptyValue())),
			kv("bytes", signal.BytesValue([]byte{1, 0, 2}))),
	}
}

func TestWriteViewSortedMatchesMaterialized(t *testing.T) {
	t.Parallel()
	view, refs := writeViewFixture(viewWriteValues())
	for _, count := range []int{0, 1, len(refs)} {
		t.Run(strconv.Itoa(count), func(t *testing.T) {
			t.Parallel()
			var materialized, direct bytes.Buffer
			_, err := identity.WriteSorted(&materialized, count, func(i int, dst signal.Attributes) series.Entry {
				ref := refs[i]
				return series.Entry{ID: view.ID(ref), Series: view.At(ref, dst)}
			})
			require.NoError(t, err)
			written, err := identity.WriteViewSorted(&direct, count, view, func(i int) int { return refs[i] })
			require.NoError(t, err)
			require.Equal(t, int64(direct.Len()), written)
			require.Equal(t, materialized.Bytes(), direct.Bytes())
			decoded := 0
			require.NoError(t, identity.Decode(direct.Bytes(), func(id signal.SeriesID, got signal.Series) error {
				ref := refs[decoded]
				require.Equal(t, view.ID(ref), id)
				require.Equal(t, view.At(ref, nil).AppendHashInput(nil), got.AppendHashInput(nil))
				decoded++
				return nil
			}))
			require.Equal(t, count, decoded)
		})
	}
}

func TestWriteViewSortedIdentityReferences(t *testing.T) {
	t.Parallel()
	original, refs := writeViewFixture(viewWriteValues())
	index := series.New()
	for _, ref := range refs {
		index.Add(original.At(ref, nil))
	}
	view := index.View()
	var materialized, direct bytes.Buffer
	_, err := identity.WriteSorted(&materialized, view.Len(), func(i int, dst signal.Attributes) series.Entry {
		return series.Entry{ID: view.ID(i), Series: view.At(i, dst)}
	})
	require.NoError(t, err)
	_, err = identity.WriteViewSorted(&direct, view.Len(), view, nil)
	require.NoError(t, err)
	require.Equal(t, materialized.Bytes(), direct.Bytes())
}

func TestWriteViewSortedEmptyAttributes(t *testing.T) {
	t.Parallel()
	for _, s := range []signal.Series{{}, {Resource: signal.Resource{SchemaURL: []byte("r")},
		Scope: signal.Scope{Name: []byte("n"), Version: []byte("v"), SchemaURL: []byte("s")}}} {
		index := series.New()
		id := index.Add(s)
		var materialized, direct bytes.Buffer
		_, err := identity.WriteSorted(&materialized, 1, func(int, signal.Attributes) series.Entry {
			return series.Entry{ID: id, Series: s}
		})
		require.NoError(t, err)
		_, err = identity.WriteViewSorted(&direct, 1, index.View(), nil)
		require.NoError(t, err)
		require.Equal(t, materialized.Bytes(), direct.Bytes())
	}
}

func TestWriteViewSortedErrors(t *testing.T) {
	t.Parallel()
	view, refs := writeViewFixture([]signal.Value{str("x")})
	var valid bytes.Buffer
	_, err := identity.WriteViewSorted(&valid, 1, view, nil)
	require.NoError(t, err)
	for _, cause := range []error{nil, io.ErrClosedPipe} {
		for limit := range valid.Len() {
			w := &limitedIdentityWriter{remaining: limit, err: cause}
			n, err := identity.WriteViewSorted(w, 1, view, func(i int) int { return refs[i] })
			want := cause
			if want == nil {
				want = io.ErrShortWrite
			}
			require.ErrorIs(t, err, want)
			require.Equal(t, int64(limit), n)
			require.True(t, bytes.Equal(valid.Bytes()[:limit], w.Bytes()))
		}
	}
	for _, invalid := range []int{-1, 100} {
		n, err := identity.WriteViewSorted(invalidIdentityWriter(invalid), 0, view, nil)
		require.ErrorIs(t, err, io.ErrShortWrite)
		require.Zero(t, n)
	}
	for _, count := range []int{-1, 2} {
		n, err := identity.WriteViewSorted(io.Discard, count, view, nil)
		require.Error(t, err)
		require.Zero(t, n)
	}
	for _, ref := range []int{-1, view.Len()} {
		var out bytes.Buffer
		n, err := identity.WriteViewSorted(&out, 1, view, func(int) int { return ref })
		require.Error(t, err)
		require.Equal(t, int64(out.Len()), n)
	}
}

func FuzzWriteViewSortedMatchesMaterialized(f *testing.F) {
	f.Add("name", "value", int64(7), uint64(0x8000000000000000))
	f.Add("", "", int64(0), uint64(0x7ff8000000000001))
	f.Add("a\x00b", "\xff", int64(-1), uint64(0x7ff0000000000000))
	f.Fuzz(func(t *testing.T, key, value string, number int64, bits uint64) {
		s := signal.Series{Attributes: signal.NewAttributes(kv(key, str(value)),
			kv(key+"_int", signal.IntValue(number)), kv(key+"_nested", signal.MapValue(
				kv("f", signal.DoubleValue(math.Float64frombits(bits))),
				kv("s", signal.SliceValue(str(value), signal.BytesValue([]byte(value)))))))}
		index := series.New()
		index.Add(s)
		view := index.View()
		var materialized, direct bytes.Buffer
		_, err := identity.WriteSorted(&materialized, 1, func(i int, dst signal.Attributes) series.Entry {
			return series.Entry{ID: view.ID(i), Series: view.At(i, dst)}
		})
		require.NoError(t, err)
		_, err = identity.WriteViewSorted(&direct, 1, view, nil)
		require.NoError(t, err)
		require.Equal(t, materialized.Bytes(), direct.Bytes())
		require.NoError(t, identity.Decode(direct.Bytes(), func(id signal.SeriesID, got signal.Series) error {
			require.Equal(t, s.Hash(), id)
			require.Equal(t, s.AppendHashInput(nil), got.AppendHashInput(nil))
			return nil
		}))
	})
}

func BenchmarkIdentityViewDestination(b *testing.B) {
	for _, nested := range []bool{false, true} {
		name := "scalar"
		if nested {
			name = "nested"
		}
		b.Run(name, func(b *testing.B) {
			const count = 4096
			values := make([]signal.Value, count)
			for i := range values {
				values[i] = signal.IntValue(int64(i))
				if nested {
					values[i] = signal.MapValue(kv("node", str("node-"+strconv.Itoa(i))),
						kv("values", signal.SliceValue(signal.IntValue(int64(i)), str("shared"))))
				}
			}
			view, refs := writeViewFixture(values)
			entry := func(i int, attrs signal.Attributes) series.Entry {
				ref := refs[i]
				return series.Entry{ID: view.ID(ref), Series: view.At(ref, attrs)}
			}
			b.Run("materialized", func(b *testing.B) {
				b.ReportAllocs()
				for b.Loop() {
					if _, err := identity.WriteSorted(io.Discard, count, entry); err != nil {
						b.Fatal(err)
					}
				}
			})
			b.Run("compact", func(b *testing.B) {
				b.ReportAllocs()
				for b.Loop() {
					if _, err := identity.WriteViewSorted(io.Discard, count, view, func(i int) int { return refs[i] }); err != nil {
						b.Fatal(err)
					}
				}
			})
		})
	}
}
