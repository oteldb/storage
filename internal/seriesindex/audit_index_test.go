package seriesindex

import (
	"bytes"
	"context"
	"encoding/binary"
	"io"
	"math/rand/v2"
	"slices"
	"strconv"
	"testing"

	"github.com/go-faster/errors"
	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/backend"
	"github.com/oteldb/storage/backend/faultbackend"
	"github.com/oteldb/storage/index/postings"
	"github.com/oteldb/storage/query/fetch"
	"github.com/oteldb/storage/signal"
)

func openTestIndex(tb testing.TB, data []byte) *Index {
	tb.Helper()
	b := backend.Memory()
	require.NoError(tb, b.Write(context.Background(), "index", data))
	r, err := Open(context.Background(), b, "index")
	require.NoError(tb, err)
	return r
}

type testRecord struct{ key, value []byte }

func rewriteIndex(tb testing.TB, data []byte, edit func(*testRecord)) []byte {
	tb.Helper()
	r := openTestIndex(tb, data)
	c := r.table.Cursor(context.Background())
	var records []testRecord
	for c.Next() {
		record := testRecord{bytes.Clone(c.Key()), bytes.Clone(c.Value())}
		edit(&record)
		if record.key != nil {
			records = append(records, record)
		}
	}
	require.NoError(tb, c.Err())
	slices.SortFunc(records, func(a, b testRecord) int { return bytes.Compare(a.key, b.key) })
	var out bytes.Buffer
	w := NewTableWriter(&out)
	w.seriesCount = r.count
	for _, rec := range records {
		require.NoError(tb, w.Append(rec.key, rec.value))
	}
	require.NoError(tb, w.Finish())
	return out.Bytes()
}

func TestIndexPublicReaders(t *testing.T) {
	t.Parallel()
	src := indexSeries()
	data, builder := buildIndex(t, src)
	r := openTestIndex(t, data)
	require.Equal(t, len(src), builder.Count())
	require.GreaterOrEqual(t, builder.EncodedBound(), int64(len(data)))
	var names [][]byte
	require.NoError(t, r.Names(t.Context(), func(name []byte) error { names = append(names, bytes.Clone(name)); return nil }))
	require.NotEmpty(t, names)
	for _, name := range names {
		var got [][]byte
		require.NoError(t, r.Values(t.Context(), name, func(v signal.Value) error { got = append(got, signal.AppendValue(nil, v)); return nil }))
		require.NotEmpty(t, got)
	}
	require.NoError(t, r.Values(t.Context(), []byte("missing"), func(signal.Value) error { t.Fatal("unexpected missing-label value"); return nil }))
	stop := errors.New("callback stopped")
	require.ErrorIs(t, r.Names(t.Context(), func([]byte) error { return stop }), stop)
	require.ErrorIs(t, r.Values(t.Context(), names[0], func(signal.Value) error { return stop }), stop)
	cursor := r.SeriesCursor(t.Context())
	refs, _ := r.Resolve(t.Context(), nil)
	i := 0
	require.NoError(t, r.Visit(t.Context(), refs, func(id signal.SeriesID, raw []byte, meta Metadata) error {
		require.Equal(t, src[i].Hash(), id)
		require.Equal(t, src[i].AppendHashInput(nil), raw)
		got, found, err := r.Metadata(t.Context(), id)
		require.NoError(t, err)
		require.True(t, found)
		require.Equal(t, meta, got)
		i++
		return nil
	}))
	require.Equal(t, len(src), i)
	for i, s := range src {
		id, raw, meta, err := cursor.At(uint32(i))
		require.NoError(t, err)
		require.Equal(t, s.Hash(), id)
		require.Equal(t, s.AppendHashInput(nil), raw)
		require.EqualValues(t, i+10, meta.Covered)
	}
	id, raw, meta, err := cursor.At(r.count)
	require.Zero(t, id)
	require.Empty(t, raw)
	require.Zero(t, meta)
	require.ErrorIs(t, err, ErrCorrupt)
	refs, _ = r.Resolve(t.Context(), nil)
	require.ErrorIs(t, r.Visit(t.Context(), refs, func(signal.SeriesID, []byte, Metadata) error { return stop }), stop)
	for _, id := range []signal.SeriesID{{Hi: 1}, {Lo: uint64(r.count)}} {
		require.ErrorIs(t, r.Visit(t.Context(), postings.FromSlice([]signal.SeriesID{id}), func(signal.SeriesID, []byte, Metadata) error { return nil }), ErrCorrupt)
	}
	require.ErrorIs(t, r.Visit(t.Context(), errorPostings{stop}, func(signal.SeriesID, []byte, Metadata) error { return nil }), stop)
	_, err = r.Resolve(t.Context(), []fetch.Matcher{{Name: []byte("host")}})
	require.Error(t, err)
	_, found, err := r.Metadata(t.Context(), signal.SeriesID{Hi: ^uint64(0)})
	require.NoError(t, err)
	require.False(t, found)
	ids := r.IDs(t.Context())
	require.True(t, ids.Seek(src[1].Hash()))
	require.Equal(t, src[1].Hash(), ids.At())
	require.True(t, ids.Seek(src[0].Hash()))
	require.True(t, ids.Seek(src[len(src)-1].Hash()))
	require.False(t, ids.Seek(signal.SeriesID{Hi: ^uint64(0)}))
}

type errorPostings struct{ err error }

func (errorPostings) Next() bool                { return false }
func (errorPostings) Seek(signal.SeriesID) bool { return false }
func (errorPostings) At() signal.SeriesID       { return signal.SeriesID{} }
func (p errorPostings) Err() error              { return p.err }

func TestIndexDescriptorCorruption(t *testing.T) {
	t.Parallel()
	src := indexSeries()
	data, _ := buildIndex(t, src)
	for _, mode := range []string{"short record", "bad flags", "short stats", "bad identity hash", "missing descriptor"} {
		t.Run(mode, func(t *testing.T) {
			t.Parallel()
			bad := rewriteIndex(t, data, func(rec *testRecord) {
				if !bytes.Equal(rec.key, recordKey(0)) {
					return
				}
				switch mode {
				case "short record":
					rec.value = []byte{1}
				case "bad flags":
					rec.value[24] = 2
				case "short stats":
					rec.value = rec.value[:26]
				case "bad identity hash":
					rec.value[len(rec.value)-1] ^= 1
				case "missing descriptor":
					rec.key = []byte("z-missing")
				}
			})
			r := openTestIndex(t, bad)
			_, _, _, err := r.Series(t.Context(), 0)
			require.ErrorIs(t, err, ErrCorrupt)
			_, _, _, err = r.Lookup(t.Context(), src[0].Hash())
			require.ErrorIs(t, err, ErrCorrupt)
			meta, found, err := r.Metadata(t.Context(), src[0].Hash())
			require.NoError(t, err, "inline metadata does not decode descriptor payloads")
			require.True(t, found)
			require.EqualValues(t, 10, meta.Covered)
			_, _, _, err = r.SeriesCursor(t.Context()).At(0)
			require.ErrorIs(t, err, ErrCorrupt)
			require.ErrorIs(t, r.Visit(t.Context(), postings.FromSlice([]signal.SeriesID{{}}), func(signal.SeriesID, []byte, Metadata) error { return nil }), ErrCorrupt)
		})
	}
	for _, mode := range []string{"short ref", "out of bounds", "mismatched identity"} {
		t.Run(mode, func(t *testing.T) {
			t.Parallel()
			bad := rewriteIndex(t, data, func(rec *testRecord) {
				if !bytes.Equal(rec.key, idKey(src[0].Hash())) {
					return
				}
				switch mode {
				case "short ref":
					rec.value = []byte{1}
				case "out of bounds":
					rec.value = binary.BigEndian.AppendUint32(nil, uint32(len(src)))
				case "mismatched identity":
					rec.value = binary.BigEndian.AppendUint32(nil, 1)
				}
			})
			r := openTestIndex(t, bad)
			_, _, _, err := r.Lookup(t.Context(), src[0].Hash())
			require.ErrorIs(t, err, ErrCorrupt)
			_, _, err = r.Metadata(t.Context(), src[0].Hash())
			require.ErrorIs(t, err, ErrCorrupt)
		})
	}
	r := openTestIndex(t, data)
	id, ident, meta, err := r.Series(t.Context(), r.count)
	require.Zero(t, id)
	require.Zero(t, ident)
	require.Zero(t, meta)
	require.ErrorIs(t, err, ErrCorrupt)
}

func TestBuilderFailureAndWriteObject(t *testing.T) {
	t.Parallel()
	src := indexSeries()
	b := &Builder{}
	b.UpdateMetadata(Metadata{})
	require.Error(t, b.Add(signal.SeriesID{}, src[len(src)-1], Metadata{}))
	require.Zero(t, b.Count())
	for _, s := range src {
		require.NoError(t, b.Add(s.Hash(), s, Metadata{}))
	}
	require.Error(t, b.Add(src[0].Hash(), src[0], Metadata{}))
	b.UpdateMetadata(Metadata{Covered: 999})
	be := backend.Memory()
	require.NoError(t, b.WriteObject(t.Context(), be, "index"))
	r, err := Open(t.Context(), be, "index")
	require.NoError(t, err)
	meta, found, err := r.Metadata(t.Context(), src[len(src)-1].Hash())
	require.NoError(t, err)
	require.True(t, found)
	require.EqualValues(t, 999, meta.Covered)
	fault := faultbackend.Wrap(be).Add(faultbackend.Rule{Kind: faultbackend.Write, Err: io.ErrClosedPipe})
	require.ErrorIs(t, b.WriteObject(t.Context(), fault, "bad"), io.ErrClosedPipe)
	require.ErrorIs(t, b.Write(shortWriter{}), io.ErrShortWrite)
}

func TestBuilderEncodedBoundSharedPayload(t *testing.T) {
	t.Parallel()
	rng := rand.New(rand.NewPCG(7, 11))
	shared := make([]byte, 1<<20)
	for i := range shared {
		shared[i] = byte(rng.Uint32())
	}
	identities := make([]signal.Series, 32)
	for i := range identities {
		identities[i] = signal.Series{Attributes: signal.NewAttributes(signal.KeyValue{Key: []byte("shared"), Value: signal.BytesValue(shared)}, signal.KeyValue{Key: []byte("id"), Value: signal.StringValue([]byte(strconv.Itoa(i)))})}
	}
	slices.SortFunc(identities, func(a, b signal.Series) int { return a.Hash().Compare(b.Hash()) })
	data, b := buildIndex(t, identities)
	require.Less(t, b.Bytes(), int64(len(data)))
	require.GreaterOrEqual(t, b.EncodedBound(), int64(len(data)))
}

func TestPostingsChunksTypedAndAbsent(t *testing.T) {
	t.Parallel()
	key := []byte{'b', 0, 255}
	src := make([]signal.Series, 3*postingChunk+17)
	for i := range src {
		attrs := []signal.KeyValue{{Key: []byte("id"), Value: signal.IntValue(int64(i))}, {Key: []byte("shared"), Value: signal.StringValue([]byte("common"))}}
		var value signal.Value
		switch i % 7 {
		case 1:
			value = signal.StringValue([]byte("1"))
		case 2:
			value = signal.IntValue(1)
		case 3:
			value = signal.BoolValue(true)
		case 4:
			value = signal.BytesValue([]byte{0, 255})
		case 5:
			value = signal.EmptyValue()
		case 6:
			value = signal.DoubleValue(1.5)
		}
		if i%7 != 0 {
			attrs = append(attrs, signal.KeyValue{Key: key, Value: value})
		}
		src[i] = signal.Series{Attributes: signal.NewAttributes(attrs...), Resource: signal.Resource{Attributes: signal.NewAttributes(signal.KeyValue{Key: []byte("shared"), Value: signal.StringValue([]byte("resource"))})}}
	}
	slices.SortFunc(src, func(a, b signal.Series) int { return a.Hash().Compare(b.Hash()) })
	data, _ := buildIndex(t, src)
	r := openTestIndex(t, data)
	for _, tc := range []struct {
		name     string
		matchers []fetch.Matcher
	}{
		{"many chunks", []fetch.Matcher{{Name: []byte("shared"), Match: func(v signal.Value) bool { return string(v.AppendText(nil)) == "common" }}}},
		{"source union", []fetch.Matcher{{Name: []byte("shared"), Spec: &fetch.EqualMatcher{Value: "resource"}, Match: func(v signal.Value) bool { return string(v.AppendText(nil)) == "resource" }}}},
		{"finite hint", []fetch.Matcher{{Name: []byte("shared"), StringValues: [][]byte{[]byte("common"), []byte("resource")}, Match: func(v signal.Value) bool { return string(v.AppendText(nil)) == "resource" }}}},
		{"absent or empty", []fetch.Matcher{{Name: key, Match: func(v signal.Value) bool { return v.Kind() == signal.KindEmpty }}}},
		{"bytes", []fetch.Matcher{{Name: key, Match: func(v signal.Value) bool { return v.Kind() == signal.KindBytes }}}},
		{"typed text", []fetch.Matcher{{Name: key, Spec: &fetch.EqualMatcher{Value: "1"}, Match: func(v signal.Value) bool { return string(v.AppendText(nil)) == "1" }}}},
		{"intersect", []fetch.Matcher{{Name: key, Match: func(v signal.Value) bool { return v.Kind() == signal.KindInt }}, {Name: []byte("shared"), Match: func(v signal.Value) bool { return string(v.AppendText(nil)) == "common" }}}},
		{"hint predicate rejection", []fetch.Matcher{{Name: []byte("shared"), StringValues: [][]byte{[]byte("common")}, Match: func(signal.Value) bool { return false }}}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			var want []signal.SeriesID
			for i, s := range src {
				if bruteMetricMatch(s, tc.matchers) {
					want = append(want, signal.SeriesID{Lo: uint64(i)})
				}
			}
			refs, err := r.Resolve(t.Context(), tc.matchers)
			require.NoError(t, err)
			got, err := postings.ToSlice(refs)
			require.NoError(t, err)
			require.Equal(t, want, got)
		})
	}
}

func TestPostingsStructuralValidation(t *testing.T) {
	t.Parallel()
	r := &Index{count: 4}
	for _, data := range [][]byte{nil, {0}, {255, 255}, {129, 8}, {1, 4}, {2, 1, 0}, {1, 0, 1}, {1, 0}} {
		require.ErrorIs(t, r.mark(data, 3, make([]uint64, 1), true), ErrCorrupt)
	}
	for _, p := range []postings.Postings{&refRange{n: 4, at: -1}, &refBitmap{words: []uint64{5}, n: 4, at: -1}} {
		require.True(t, p.Seek(signal.SeriesID{Lo: 2}))
		require.Equal(t, signal.SeriesID{Lo: 2}, p.At())
		require.True(t, p.Seek(signal.SeriesID{}))
		require.False(t, p.Seek(signal.SeriesID{Hi: 1}))
		require.False(t, p.Next())
		require.NoError(t, p.Err())
	}
	p := &refBitmap{words: []uint64{1}, n: 4, at: -1}
	require.True(t, p.Next())
	require.False(t, p.Seek(signal.SeriesID{Lo: 4}))
	require.False(t, p.Seek(signal.SeriesID{}))
	require.Nil(t, prefixEnd([]byte{255, 255}))
	require.Equal(t, []byte{2}, prefixEnd([]byte{1, 255}))
	for _, data := range [][]byte{{0}, {0, 1}, {1, 0}, {1}} {
		_, _, err := decodeEscaped(nil, data)
		require.ErrorIs(t, err, ErrCorrupt)
	}
	empty := &Builder{}
	var out bytes.Buffer
	require.NoError(t, empty.Write(&out))
	idx := openTestIndex(t, out.Bytes())
	for _, want := range []bool{false, true} {
		refs, err := idx.Resolve(t.Context(), []fetch.Matcher{{Name: []byte("missing"), Match: func(signal.Value) bool { return want }}})
		require.NoError(t, err)
		require.False(t, refs.Next())
	}
}

func TestIndexParentDigest(t *testing.T) {
	t.Parallel()
	b := &Builder{}
	require.Zero(t, openTestIndex(t, func() []byte { var data bytes.Buffer; require.NoError(t, b.Write(&data)); return data.Bytes() }()).ParentDigest())
	digest := [32]byte{1, 2, 3, 255}
	b.BindParent(digest)
	var data bytes.Buffer
	require.NoError(t, b.Write(&data))
	require.Equal(t, digest, openTestIndex(t, data.Bytes()).ParentDigest())
}

func TestIndexDictionaryCorruption(t *testing.T) {
	t.Parallel()
	data, _ := buildIndex(t, indexSeries())
	for _, mode := range []string{"label flags", "presence key", "presence ref", "typed escape", "typed value", "typed key", "value chunk"} {
		t.Run(mode, func(t *testing.T) {
			t.Parallel()
			bad := rewriteIndex(t, data, func(rec *testRecord) {
				old := bytes.Clone(rec.key)
				switch mode {
				case "label flags":
					if rec.key[0] == 'n' {
						rec.value = []byte{2}
					}
				case "presence key":
					if rec.key[0] == 'h' {
						rec.key = append(rec.key, 0)
					}
				case "presence ref":
					if rec.key[0] == 'h' {
						rec.value = []byte{0}
					}
				case "typed escape":
					if rec.key[0] == 'p' {
						rec.key = appendEscaped([]byte{'p'}, []byte("host"))
						rec.key = append(rec.key, 0, 1)
						rec.key = append(rec.key, old...)
					}
				case "typed value":
					if rec.key[0] == 'p' {
						rec.key = appendEscaped([]byte{'p'}, []byte("host"))
						rec.key = appendEscaped(rec.key, append([]byte{255}, old...))
						rec.key = binary.BigEndian.AppendUint32(rec.key, uint32(len(rec.value)))
					}
				case "typed key":
					if rec.key[0] == 'p' {
						rec.key = append(rec.key, 0)
					}
				case "value chunk":
					if rec.key[0] == 'p' {
						rec.value = []byte{0}
					}
				}
			})
			r := openTestIndex(t, bad)
			matcher := fetch.Matcher{Name: []byte("host"), Match: func(signal.Value) bool { return true }}
			_, err := r.Resolve(t.Context(), []fetch.Matcher{matcher})
			require.ErrorIs(t, err, ErrCorrupt)
		})
	}
	for _, mode := range []string{"name escape", "name suffix", "value escape", "value suffix", "value encoding"} {
		t.Run(mode, func(t *testing.T) {
			t.Parallel()
			changed := false
			bad := rewriteIndex(t, data, func(rec *testRecord) {
				if changed {
					return
				}
				switch mode {
				case "name escape":
					if rec.key[0] == 'n' {
						rec.key = []byte{'n', 0, 1}
						changed = true
					}
				case "name suffix":
					if rec.key[0] == 'n' {
						rec.key = append(rec.key, 1)
						changed = true
					}
				case "value escape":
					if bytes.HasPrefix(rec.key, appendEscaped([]byte{'p'}, []byte("host"))) {
						rec.key = append(appendEscaped([]byte{'p'}, []byte("host")), 0, 1)
						changed = true
					}
				case "value suffix":
					if bytes.HasPrefix(rec.key, appendEscaped([]byte{'p'}, []byte("host"))) {
						rec.key = append(rec.key, 0)
						changed = true
					}
				case "value encoding":
					if bytes.HasPrefix(rec.key, appendEscaped([]byte{'p'}, []byte("host"))) {
						rec.key = appendEscaped(appendEscaped([]byte{'p'}, []byte("host")), []byte{255})
						rec.key = binary.BigEndian.AppendUint32(rec.key, 0)
						changed = true
					}
				}
			})
			r := openTestIndex(t, bad)
			var err error
			if mode == "name escape" || mode == "name suffix" {
				err = r.Names(t.Context(), func([]byte) error { return nil })
			} else {
				err = r.Values(t.Context(), []byte("host"), func(signal.Value) error { return nil })
			}
			require.ErrorIs(t, err, ErrCorrupt)
		})
	}
}

func TestLazyMetadataPageCorruptionFailsReads(t *testing.T) {
	t.Parallel()
	src := make([]signal.Series, 32)
	for i := range src {
		payload := append(bytes.Repeat([]byte{byte(i + 1)}, 4096), []byte(strconv.Itoa(i))...)
		src[i] = signal.Series{Attributes: signal.NewAttributes(signal.KeyValue{Key: []byte("payload"), Value: signal.StringValue(payload)}, signal.KeyValue{Key: []byte("id"), Value: signal.IntValue(int64(i))})}
	}
	slices.SortFunc(src, func(a, b signal.Series) int { return a.Hash().Compare(b.Hash()) })
	data, _ := buildIndex(t, src)
	for _, kind := range []byte{'p', 's'} {
		t.Run(string(kind), func(t *testing.T) {
			t.Parallel()
			b := backend.Memory()
			require.NoError(t, b.Write(t.Context(), "index", data))
			r, err := Open(t.Context(), b, "index")
			require.NoError(t, err)
			directory := r.table.Cursor(t.Context())
			found := false
			var page pageInfo
			for i := range r.table.pages {
				page, err = directory.entry(i)
				require.NoError(t, err)
				if len(page.first) > 0 && page.first[0] == kind {
					found = true
					break
				}
			}
			require.True(t, found, "fixture needs a separate lazy metadata page")
			bad := bytes.Clone(data)
			bad[page.offset] ^= 1
			require.NoError(t, b.Write(t.Context(), "index", bad))
			r, err = Open(t.Context(), b, "index")
			require.NoError(t, err)
			ids, err := postings.ToSlice(r.IDs(t.Context()))
			require.NoError(t, err)
			require.Len(t, ids, len(src), "canonical-ID validation intentionally leaves unrelated pages lazy")
			if kind == 'p' {
				_, err = r.Resolve(t.Context(), []fetch.Matcher{{Name: []byte("payload"), Match: func(signal.Value) bool { return true }}})
				require.ErrorIs(t, err, ErrCorrupt)
				return
			}
			ref := binary.BigEndian.Uint32(page.first[1:])
			_, _, _, err = r.Lookup(t.Context(), src[ref].Hash())
			require.ErrorIs(t, err, ErrCorrupt)
		})
	}
}
