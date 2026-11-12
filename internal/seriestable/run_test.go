package seriestable

import (
	"bytes"
	"context"
	"encoding/binary"
	"hash/crc32"
	"slices"
	"strconv"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/backend"
	"github.com/oteldb/storage/backend/backendtest"
	backendfile "github.com/oteldb/storage/backend/file"
	"github.com/oteldb/storage/block"
	"github.com/oteldb/storage/index/series"
	"github.com/oteldb/storage/internal/seriesindex"
	"github.com/oteldb/storage/query/fetch"
	"github.com/oteldb/storage/readbudget"
	"github.com/oteldb/storage/signal"
)

func descriptors(n int) []signal.Series {
	out := make([]signal.Series, n)
	for i := range out {
		out[i] = signal.Series{
			Resource: signal.Resource{Attributes: signal.NewAttributes(signal.KeyValue{Key: []byte("resource"), Value: signal.BytesValue([]byte{0, 255})})},
			Scope:    signal.Scope{Name: []byte("scope"), Version: []byte("v1")},
			Attributes: signal.NewAttributes(signal.KeyValue{Key: []byte("host"), Value: signal.StringValue([]byte(strconv.Itoa(i)))},
				signal.KeyValue{Key: []byte("typed"), Value: signal.IntValue(int64(i))}),
		}
	}
	slices.SortFunc(out, func(a, b signal.Series) int { return a.Hash().Compare(b.Hash()) })
	return out
}

func buildRun(tb testing.TB, be backend.Backend, prefix string, data []signal.Series) *Run {
	tb.Helper()
	var b Builder
	for i := range data {
		s := &data[i]
		require.NoError(tb, b.Add(s.Hash(), *s))
	}
	r, err := Write(tb.Context(), be, prefix, &b)
	require.NoError(tb, err)
	tb.Cleanup(func() { require.NoError(tb, r.Close()) })
	return r
}

func TestRoundTrip(t *testing.T) {
	t.Parallel()
	for _, files := range []bool{false, true} {
		t.Run(strconv.FormatBool(files), func(t *testing.T) {
			t.Parallel()
			be := backend.Memory()
			if files {
				f, err := backendfile.New(t.TempDir())
				require.NoError(t, err)
				be = f
			}
			data := descriptors(8)
			r := buildRun(t, be, "s-one", data)
			require.EqualValues(t, len(data), r.Count())
			require.Equal(t, []string{IDColumn}, r.part.ColumnNames())
			require.Positive(t, r.MaxPageBytes())
			r2, err := Open(t.Context(), be, "s-one", r.Digest())
			require.NoError(t, err)
			defer func() { require.NoError(t, r2.Close()) }()
			reader, err := r2.NewReader(t.Context())
			require.NoError(t, err)
			defer func() { require.NoError(t, reader.Close()) }()
			for _, s := range data {
				raw, found, err := reader.LookupRaw(s.Hash())
				require.NoError(t, err)
				require.True(t, found)
				require.Equal(t, s.AppendHashInput(nil), raw)
			}
			_, found, err := reader.LookupRaw(signal.SeriesID{})
			require.NoError(t, err)
			require.False(t, found)
			it, err := r2.Iterate(t.Context())
			require.NoError(t, err)
			defer func() { require.NoError(t, it.Close()) }()
			for _, s := range data {
				require.True(t, it.Next())
				require.Equal(t, s.Hash(), it.ID())
				require.Equal(t, s.AppendHashInput(nil), it.Raw())
			}
			require.False(t, it.Next())
			require.NoError(t, it.Err())
			selected, err := reader.Resolve([]fetch.Matcher{{Name: []byte("typed"), Match: func(v signal.Value) bool { return v.Equal(signal.IntValue(3)) }}})
			require.NoError(t, err)
			defer func() { require.NoError(t, selected.Close()) }()
			require.True(t, selected.Next())
			require.False(t, selected.Next())
			require.NoError(t, selected.Err())
		})
	}
}

func TestBuilderOwnership(t *testing.T) {
	t.Parallel()
	data := descriptors(1)
	want := bytes.Clone(data[0].AppendHashInput(nil))
	id := data[0].Hash()
	var b Builder
	require.NoError(t, b.Add(id, data[0]))
	data[0].Scope.Name[0] ^= 1
	data[0].Resource.Attributes[0].Value.Bytes()[0] ^= 1
	r, err := Write(t.Context(), backend.Memory(), "owned", &b)
	require.NoError(t, err)
	defer func() { require.NoError(t, r.Close()) }()
	rd, err := r.NewReader(t.Context())
	require.NoError(t, err)
	defer func() { require.NoError(t, rd.Close()) }()
	raw, found, err := rd.LookupRaw(id)
	require.NoError(t, err)
	require.True(t, found)
	require.Equal(t, want, raw)
	require.Positive(t, b.Bytes())
	require.Positive(t, b.EncodedBound())
}

func TestCloseAndAdmission(t *testing.T) {
	t.Parallel()
	be := backend.Memory()
	r := buildRun(t, be, "closed", descriptors(2))
	budget := readbudget.New(32 << 20)
	ctx := readbudget.With(t.Context(), budget)
	rd, err := r.NewReader(ctx)
	require.NoError(t, err)
	require.Less(t, budget.Remaining(), int64(32<<20))
	require.NoError(t, r.Close())
	_, err = r.NewReader(ctx)
	require.ErrorIs(t, err, ErrClosed)
	_, found, err := rd.LookupRaw(descriptors(2)[0].Hash())
	require.NoError(t, err)
	require.True(t, found)
	require.NoError(t, rd.Close())
	require.NoError(t, rd.Close())
	require.EqualValues(t, 32<<20, budget.Remaining())
	_, _, err = rd.LookupRaw(signal.SeriesID{})
	require.ErrorIs(t, err, ErrClosed)
	counter := backendtest.NewByteCounter(be)
	_, err = Open(readbudget.With(t.Context(), readbudget.New(1)), counter, "closed")
	require.ErrorIs(t, err, readbudget.ErrExceeded)
	require.Zero(t, counter.Reads())
	_, err = Open(t.Context(), backendtest.WithoutCapabilities(be), "closed")
	require.ErrorIs(t, err, readbudget.ErrExceeded)
}

func TestRejectInvalidRun(t *testing.T) {
	t.Parallel()
	for _, mode := range []string{"index missing", "column missing", "wrong digest", "index mismatch", "column crc", "ID mismatch", "sample metadata", "numeric schema"} {
		t.Run(mode, func(t *testing.T) {
			t.Parallel()
			be := backend.Memory()
			data := descriptors(3)
			r := buildRun(t, be, "bad", data)
			digest := r.Digest()
			switch mode {
			case "index missing":
				require.NoError(t, be.Delete(t.Context(), "bad/series-index"))
			case "column missing":
				require.NoError(t, be.Delete(t.Context(), "bad/c/0"))
			case "wrong digest":
				digest[0] ^= 1
			case "index mismatch":
				buildRun(t, be, "other", data)
				raw, err := be.Read(t.Context(), "other/series-index")
				require.NoError(t, err)
				require.NoError(t, be.Write(t.Context(), "bad/series-index", raw))
			case "column crc", "ID mismatch":
				raw, err := be.Read(t.Context(), "bad/c/0")
				require.NoError(t, err)
				raw[3] ^= 1
				if mode == "ID mismatch" {
					binary.LittleEndian.PutUint32(raw[len(raw)-4:], crc32.Checksum(raw[:len(raw)-4], crc32.MakeTable(crc32.Castagnoli)))
				}
				require.NoError(t, be.Write(t.Context(), "bad/c/0", raw))
			case "sample metadata":
				var b seriesindex.Builder
				for _, s := range data {
					require.NoError(t, b.Add(s.Hash(), s, seriesindex.Metadata{Covered: 123}))
				}
				b.BindParent(digest)
				require.NoError(t, b.WriteObject(t.Context(), be, "bad/series-index"))
			case "numeric schema":
				w := block.NewPartWriter()
				require.NoError(t, w.AddColumn(block.Column{Name: "ts", Kind: block.KindInt64, Int64: []int64{1, 2, 3}}))
				require.NoError(t, block.WritePart(t.Context(), be, "bad", w))
			}
			got, err := Open(t.Context(), be, "bad", digest)
			require.Error(t, err)
			require.Nil(t, got)
		})
	}
}

type checkedRanges struct {
	*backendtest.ByteCounter

	maxIDRead int64
}

func (b *checkedRanges) ReadAt(ctx context.Context, key string, offset, n int64) ([]byte, error) {
	if key == "bounded/c/0" {
		b.maxIDRead = max(b.maxIDRead, n)
	}
	return b.ByteCounter.ReadAt(ctx, key, offset, n)
}

func TestBoundedIDValidation(t *testing.T) {
	t.Parallel()
	be := backend.Memory()
	buildRun(t, be, "bounded", descriptors(5000))
	counter := &checkedRanges{ByteCounter: backendtest.NewByteCounter(be)}
	r, err := Open(readbudget.With(t.Context(), readbudget.New(64<<20)), counter, "bounded")
	require.NoError(t, err)
	defer func() { require.NoError(t, r.Close()) }()
	require.Positive(t, counter.maxIDRead)
	require.LessOrEqual(t, counter.maxIDRead, int64(validationBuffer))
}

func TestBuilderValidation(t *testing.T) {
	t.Parallel()
	be := backend.Memory()
	_, err := Write(t.Context(), be, "empty", nil)
	require.Error(t, err)
	var b Builder
	_, err = Write(t.Context(), be, "empty", &b)
	require.Error(t, err)
	s := descriptors(1)[0]
	require.Error(t, b.Add(signal.SeriesID{}, s))
	registry := series.New()
	id := registry.Add(s)
	require.Error(t, b.AddView(registry.View(), -1))
	require.Error(t, b.AddView(registry.View(), 1))
	require.NoError(t, b.AddView(registry.View(), 0))
	require.Error(t, b.Add(id, s))
	r, err := Write(t.Context(), be, "view", &b)
	require.NoError(t, err)
	require.NoError(t, r.Close())
}

func TestOperationFailures(t *testing.T) {
	t.Parallel()
	be := backend.Memory()
	r := buildRun(t, be, "operations", descriptors(1))
	canceled, cancel := context.WithCancel(t.Context())
	cancel()
	_, err := Open(canceled, be, "operations")
	require.ErrorIs(t, err, context.Canceled)
	_, err = r.NewReader(canceled)
	require.ErrorIs(t, err, context.Canceled)
	_, err = r.Resolve(canceled, nil)
	require.ErrorIs(t, err, context.Canceled)
	_, err = Open(t.Context(), be, "operations", r.Digest(), r.Digest())
	require.Error(t, err)
	_, err = Open(t.Context(), be, "absent")
	require.ErrorIs(t, err, backend.ErrNotExist)
	tiny := readbudget.With(t.Context(), readbudget.New(1))
	_, err = r.NewReader(tiny)
	require.ErrorIs(t, err, readbudget.ErrExceeded)
	_, err = r.Resolve(tiny, nil)
	require.ErrorIs(t, err, readbudget.ErrExceeded)
	_, err = r.Resolve(t.Context(), []fetch.Matcher{{Name: []byte("host")}})
	require.Error(t, err)
	ctx, stop := context.WithCancel(t.Context())
	rd, err := r.NewReader(ctx)
	require.NoError(t, err)
	it, err := r.Iterate(ctx)
	require.NoError(t, err)
	stop()
	_, _, err = rd.LookupRaw(signal.SeriesID{})
	require.ErrorIs(t, err, context.Canceled)
	require.False(t, it.Next())
	require.ErrorIs(t, it.Err(), context.Canceled)
	require.NoError(t, it.Close())
	require.NoError(t, it.Close())
	require.False(t, it.Next())
	require.NoError(t, rd.Close())
	_, err = rd.Resolve(nil)
	require.ErrorIs(t, err, ErrClosed)
	active, err := r.Iterate(t.Context())
	require.NoError(t, err)
	require.NoError(t, r.Close())
	require.True(t, active.Next())
	require.NoError(t, active.Close())
	_, err = r.Resolve(t.Context(), nil)
	require.ErrorIs(t, err, ErrClosed)
	var nilRun *Run
	var nilReader *Reader
	var nilIterator *Iterator
	require.NoError(t, nilRun.Close())
	require.NoError(t, nilReader.Close())
	require.NoError(t, nilIterator.Close())
}

func TestReadRejectsChangedMetadata(t *testing.T) {
	t.Parallel()
	for _, corrupt := range []bool{false, true} {
		t.Run(strconv.FormatBool(corrupt), func(t *testing.T) {
			t.Parallel()
			be := backend.Memory()
			data := descriptors(1)
			r := buildRun(t, be, "changed", data)
			var b seriesindex.Builder
			require.NoError(t, b.Add(data[0].Hash(), data[0], seriesindex.Metadata{Covered: 12}))
			b.BindParent(r.Digest())
			require.NoError(t, b.WriteObject(t.Context(), be, "changed/series-index"))
			if corrupt {
				raw, err := be.Read(t.Context(), "changed/series-index")
				require.NoError(t, err)
				raw[1] ^= 255
				require.NoError(t, be.Write(t.Context(), "changed/series-index", raw))
			}
			rd, err := r.NewReader(t.Context())
			require.NoError(t, err)
			defer func() { require.NoError(t, rd.Close()) }()
			_, _, err = rd.LookupRaw(data[0].Hash())
			require.Error(t, err)
			it, err := r.Iterate(t.Context())
			require.NoError(t, err)
			defer func() { require.NoError(t, it.Close()) }()
			require.False(t, it.Next())
			require.Error(t, it.Err())
		})
	}
}

func TestIDFramingFailures(t *testing.T) {
	t.Parallel()
	for _, mode := range []string{"flag", "count", "checksum", "short", "manifest checksum", "manifest size"} {
		t.Run(mode, func(t *testing.T) {
			t.Parallel()
			be := backend.Memory()
			buildRun(t, be, "framing", descriptors(1))
			key := "framing/c/0"
			if mode == "manifest checksum" || mode == "manifest size" {
				key = "framing/manifest"
			}
			raw, err := be.Read(t.Context(), key)
			require.NoError(t, err)
			switch mode {
			case "flag":
				raw[0] = 255
			case "count":
				raw[1] = 2
			case "checksum", "manifest checksum":
				raw[len(raw)-1] ^= 1
			case "short":
				raw = raw[:len(raw)-1]
			case "manifest size":
				raw = make([]byte, 4097)
			}
			require.NoError(t, be.Write(t.Context(), key, raw))
			_, err = Open(t.Context(), be, "framing")
			require.Error(t, err)
		})
	}
}

type failingBackend struct {
	*backendtest.ByteCounter

	fail string
}

func (b *failingBackend) Write(ctx context.Context, key string, data []byte) error {
	if key == b.fail {
		return context.Canceled
	}
	return b.ByteCounter.Write(ctx, key, data)
}

func (b *failingBackend) WriteDeferred(ctx context.Context, key string, data []byte) error {
	return b.Write(ctx, key, data)
}

func (b *failingBackend) CreateObjectDeferred(ctx context.Context, key string) (backend.ObjectWriter, error) {
	return backend.CreateObject(ctx, b, key)
}

func (b *failingBackend) DeleteDeferred(ctx context.Context, key string) error {
	return b.Delete(ctx, key)
}

func (b *failingBackend) SyncPrefix(context.Context, string) error {
	if b.fail == "sync" {
		return context.Canceled
	}
	return nil
}

func (b *failingBackend) ReadAt(ctx context.Context, key string, off, n int64) ([]byte, error) {
	if "read:"+key == b.fail {
		return nil, context.Canceled
	}
	return b.ByteCounter.ReadAt(ctx, key, off, n)
}

func TestWriteAndReadFailures(t *testing.T) {
	t.Parallel()
	for _, failure := range []string{"failed/c/0", "failed/manifest", "failed/series-index", "sync", "read:failed/c/0"} {
		t.Run(failure, func(t *testing.T) {
			t.Parallel()
			be := &failingBackend{ByteCounter: backendtest.NewByteCounter(backend.Memory()), fail: failure}
			var b Builder
			s := descriptors(1)[0]
			require.NoError(t, b.Add(s.Hash(), s))
			r, err := Write(t.Context(), be, "failed", &b)
			require.ErrorIs(t, err, context.Canceled)
			require.Nil(t, r)
			_, err = be.Read(t.Context(), "bucket-index.bin")
			require.ErrorIs(t, err, backend.ErrNotExist)
		})
	}
}

func FuzzOpenRun(f *testing.F) {
	be := backend.Memory()
	r := buildRun(f, be, "fuzz", descriptors(2))
	require.NoError(f, r.Close())
	keys, err := be.List(f.Context(), "fuzz/")
	require.NoError(f, err)
	base := make(map[string][]byte, len(keys))
	for _, key := range keys {
		raw, err := be.Read(f.Context(), key)
		require.NoError(f, err)
		base[key] = raw
	}
	for i, key := range []string{"manifest", "c/0", "series-index"} {
		f.Add(uint8(i), base["fuzz/"+key])
	}
	f.Fuzz(func(t *testing.T, field uint8, raw []byte) {
		if len(raw) > 64<<10 {
			t.Skip()
		}
		be := backend.Memory()
		for key, value := range base {
			if err := be.Write(t.Context(), key, value); err != nil {
				t.Fatal(err)
			}
		}
		key := []string{"manifest", "c/0", "series-index"}[field%3]
		if err := be.Write(t.Context(), "fuzz/"+key, raw); err != nil {
			t.Fatal(err)
		}
		opened, err := Open(readbudget.With(t.Context(), readbudget.New(32<<20)), be, "fuzz")
		if err == nil {
			if err := opened.Close(); err != nil {
				t.Fatal(err)
			}
		}
	})
}
