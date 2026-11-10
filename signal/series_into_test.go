package signal

import (
	"bytes"
	"math"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func seriesIntoCases() []struct {
	name     string
	identity Series
} {
	return []struct {
		name     string
		identity Series
	}{
		{name: "resource-scope-point", identity: sampleSeries()},
		{name: "nested-typed", identity: Series{
			Resource: Resource{Attributes: NewAttributes(kv("map", MapValue(kv("bytes", BytesValue([]byte("nested-bytes"))))))},
			Scope:    Scope{Attributes: NewAttributes(kv("slice", SliceValue(IntValue(5), sv("5"), DoubleValue(5))))},
			Attributes: NewAttributes(kv("empty", EmptyValue()), kv("bool", BoolValue(true)),
				kv("nan", DoubleValue(math.Float64frombits(0x7ff8000000000042))),
				kv("zero", DoubleValue(math.Float64frombits(1<<63)))),
		}},
		{name: "point-only", identity: Series{Attributes: NewAttributes(kv("one", IntValue(1)))}},
		{name: "empty"},
	}
}

//nolint:paralleltest,tparallel // Subtests intentionally reuse one destination sequentially.
func TestDecodeSeriesInto(t *testing.T) {
	t.Parallel()

	var dst Series
	cases := seriesIntoCases()
	for i := range cases {
		tc := &cases[i]
		t.Run(tc.name, func(t *testing.T) {
			raw := tc.identity.AppendHashInput(nil)
			fresh, wantN, err := DecodeSeries(raw)
			require.NoError(t, err)
			n, err := DecodeSeriesInto(&dst, append(bytes.Clone(raw), 255))
			require.NoError(t, err)
			assert.Equal(t, wantN, n)
			assert.Equal(t, raw, dst.AppendHashInput(nil))
			assert.Equal(t, fresh.AppendHashInput(nil), dst.AppendHashInput(nil))
			assert.Equal(t, tc.identity.Hash(), dst.Hash())
		})
	}
}

func TestDecodeSeriesIntoReusesAndClearsHeaders(t *testing.T) {
	t.Parallel()

	dst := Series{
		Resource:   Resource{Attributes: make(Attributes, 0, 8)},
		Scope:      Scope{Attributes: make(Attributes, 0, 8)},
		Attributes: make(Attributes, 0, 8),
	}
	r, s, a := dst.Resource.Attributes[:cap(dst.Resource.Attributes)], dst.Scope.Attributes[:cap(dst.Scope.Attributes)], dst.Attributes[:cap(dst.Attributes)]
	cases := seriesIntoCases()
	for i := range cases {
		tc := &cases[i]
		n, err := DecodeSeriesInto(&dst, tc.identity.AppendHashInput(nil))
		require.NoError(t, err)
		require.Positive(t, n)
		require.Same(t, &r[0], &dst.Resource.Attributes[:cap(dst.Resource.Attributes)][0])
		require.Same(t, &s[0], &dst.Scope.Attributes[:cap(dst.Scope.Attributes)][0])
		require.Same(t, &a[0], &dst.Attributes[:cap(dst.Attributes)][0])
		assert.Equal(t, make(Attributes, cap(r)-len(dst.Resource.Attributes)), r[len(dst.Resource.Attributes):])
		assert.Equal(t, make(Attributes, cap(s)-len(dst.Scope.Attributes)), s[len(dst.Scope.Attributes):])
		assert.Equal(t, make(Attributes, cap(a)-len(dst.Attributes)), a[len(dst.Attributes):])
	}
}

//nolint:paralleltest,tparallel // Subtests intentionally reuse one destination sequentially.
func TestDecodeSeriesIntoErrorReset(t *testing.T) {
	t.Parallel()

	var dst Series
	cases := seriesIntoCases()
	for i := range cases {
		tc := &cases[i]
		t.Run(tc.name, func(t *testing.T) {
			raw := tc.identity.AppendHashInput(nil)
			for end := range raw {
				_, err := DecodeSeriesInto(&dst, sampleSeries().AppendHashInput(nil))
				require.NoError(t, err)
				n, err := DecodeSeriesInto(&dst, raw[:end])
				require.ErrorIs(t, err, ErrMalformed, "prefix %d", end)
				require.Zero(t, n)
				require.Nil(t, dst.Resource.SchemaURL)
				require.Nil(t, dst.Scope.Name)
				require.Nil(t, dst.Scope.Version)
				require.Nil(t, dst.Scope.SchemaURL)
				for _, attrs := range []Attributes{dst.Resource.Attributes, dst.Scope.Attributes, dst.Attributes} {
					require.Empty(t, attrs)
					for _, attr := range attrs[:cap(attrs)] {
						require.Equal(t, KeyValue{}, attr)
					}
				}
			}
		})
	}
}

func TestDecodeSeriesIntoBorrowsBytes(t *testing.T) {
	t.Parallel()

	var dst Series
	raw := sampleSeries().AppendHashInput(nil)
	_, err := DecodeSeriesInto(&dst, raw)
	require.NoError(t, err)
	fields := [][]byte{dst.Resource.SchemaURL, dst.Scope.Name, dst.Scope.Version, dst.Scope.SchemaURL}
	for _, attrs := range []Attributes{dst.Resource.Attributes, dst.Scope.Attributes, dst.Attributes} {
		for _, attr := range attrs {
			fields = append(fields, attr.Key, attr.Value.Str())
		}
	}
	for _, field := range fields {
		require.NotEmpty(t, field)
		at := bytes.Index(raw, field)
		require.NotEqual(t, -1, at)
		require.Same(t, &raw[at], &field[0])
	}
}

func FuzzDecodeSeriesInto(f *testing.F) {
	cases := seriesIntoCases()
	for i := range cases {
		tc := &cases[i]
		raw := tc.identity.AppendHashInput(nil)
		f.Add(raw)
		f.Add(raw[:len(raw)-1])
	}
	f.Add([]byte{})
	f.Add([]byte{0x80, 0, 0, 0, 0, 0, 0, 0})
	f.Fuzz(func(t *testing.T, raw []byte) {
		dst := sampleSeries()
		fresh, wantN, wantErr := DecodeSeries(raw)
		n, err := DecodeSeriesInto(&dst, raw)
		if (err == nil) != (wantErr == nil) || n != wantN {
			t.Fatalf("fresh (%d, %v), reuse (%d, %v)", wantN, wantErr, n, err)
		}
		if err != nil {
			if !dst.Equal(Series{}) {
				t.Fatal("failed decode retained identity")
			}
			return
		}
		if !bytes.Equal(fresh.AppendHashInput(nil), dst.AppendHashInput(nil)) {
			t.Fatal("fresh and reused canonical identities differ")
		}
		if _, err := DecodeSeriesInto(&dst, Series{}.AppendHashInput(nil)); err != nil || !dst.Equal(Series{}) {
			t.Fatalf("empty decode after reuse: %v", err)
		}
	})
}

func aliasedSeriesInto(mode byte) Series {
	attrs := make(Attributes, 8)
	r, s, a := attrs[:2], attrs[:1], attrs[:1]
	switch mode % 3 {
	case 1:
		r, s, a = attrs[:2:4], attrs[1:3:6], attrs[2:4:8]
	case 2:
		r, s, a = attrs[:0:4], attrs[1:1:6], attrs[2:2:8]
	}
	return Series{Resource: Resource{Attributes: r}, Scope: Scope{Attributes: s}, Attributes: a}
}

func TestDecodeSeriesIntoAliasedHeaders(t *testing.T) {
	t.Parallel()

	for mode, name := range []string{"same-start", "partial-overlap", "empty-with-capacity"} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			cases := seriesIntoCases()
			for i := range cases {
				tc := &cases[i]
				dst := aliasedSeriesInto(byte(mode))
				resource := &dst.Resource.Attributes[:cap(dst.Resource.Attributes)][0]
				raw := tc.identity.AppendHashInput(nil)
				n, err := DecodeSeriesInto(&dst, raw)
				require.NoError(t, err)
				require.Equal(t, len(raw), n)
				require.Equal(t, raw, dst.AppendHashInput(nil))
				require.Same(t, resource, &dst.Resource.Attributes[:cap(dst.Resource.Attributes)][0])
				for end := range raw {
					dst = aliasedSeriesInto(byte(mode))
					n, err = DecodeSeriesInto(&dst, raw[:end])
					require.ErrorIs(t, err, ErrMalformed)
					require.Zero(t, n)
					require.True(t, dst.Equal(Series{}))
					for _, attrs := range []Attributes{dst.Resource.Attributes, dst.Scope.Attributes, dst.Attributes} {
						for _, attr := range attrs[:cap(attrs)] {
							require.Equal(t, KeyValue{}, attr)
						}
					}
				}
			}
		})
	}
}

func FuzzDecodeSeriesIntoAliased(f *testing.F) {
	cases := seriesIntoCases()
	for i := range cases {
		tc := &cases[i]
		for mode := range byte(3) {
			f.Add(tc.identity.AppendHashInput(nil), mode)
		}
	}
	f.Fuzz(func(t *testing.T, raw []byte, mode byte) {
		dst := aliasedSeriesInto(mode)
		fresh, wantN, wantErr := DecodeSeries(raw)
		n, err := DecodeSeriesInto(&dst, raw)
		if (err == nil) != (wantErr == nil) || n != wantN {
			t.Fatalf("fresh (%d, %v), aliased (%d, %v)", wantN, wantErr, n, err)
		}
		if err != nil {
			if !dst.Equal(Series{}) {
				t.Fatal("failed decode retained aliased identity")
			}
			return
		}
		if !bytes.Equal(fresh.AppendHashInput(nil), dst.AppendHashInput(nil)) {
			t.Fatal("fresh and aliased canonical identities differ")
		}
	})
}
