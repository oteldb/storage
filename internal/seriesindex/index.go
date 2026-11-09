// Package seriesindex stores immutable per-part identities and typed-label postings.
package seriesindex

import (
	"bytes"
	"context"
	"encoding/binary"
	"io"
	"math"
	"slices"

	"github.com/go-faster/errors"

	"github.com/oteldb/storage/backend"
	"github.com/oteldb/storage/index/series"
	"github.com/oteldb/storage/internal/memsize"
	"github.com/oteldb/storage/signal"
)

const postingChunk = 1024

// Stats is an optional raw-series aggregate; it does not describe sampled representatives.
type Stats struct {
	Count         int64
	Sum, Min, Max float64
}

// Metadata carries the newest covered timestamp and optional exact aggregate.
type Metadata struct {
	Covered  int64
	Stats    Stats
	HasStats bool
}

type record struct {
	id   signal.SeriesID
	meta Metadata
}

type namePosting struct {
	refs      []uint32
	nonString bool
}

type valuePosting struct {
	key  string
	refs []uint32
}

// Builder owns one output part's metadata. Callers include Bytes in their build admission.
type Builder struct {
	registry *series.Index
	records  []record
	names    map[string]*namePosting
	values   map[string]*valuePosting
	bytes    int64
}

// Bytes bounds retained metadata and sorting/directory scratch during writing.
func (b *Builder) Bytes() int64 {
	n := b.bytes
	if b.registry != nil {
		n += b.registry.SizeBytes()
	}
	return 3*n + 512<<10
}

// Count returns the number of appended identities.
func (b *Builder) Count() int { return len(b.records) }

// Entry materializes one identity, reusing the caller's attribute headers.
func (b *Builder) Entry(i int, attrs signal.Attributes) series.Entry {
	return series.Entry{ID: b.records[i].id, Series: b.registry.View().At(i, attrs)}
}

// Add appends a new canonical identity in strictly increasing ID order.
func (b *Builder) Add(id signal.SeriesID, s signal.Series, meta Metadata) error {
	if uint64(len(b.records)) >= math.MaxUint32 {
		return errors.New("seriesindex: too many series")
	}
	if len(b.records) > 0 && b.records[len(b.records)-1].id.Compare(id) >= 0 {
		return errors.New("seriesindex: series are not ordered")
	}
	if b.registry == nil {
		b.registry = series.New()
	}
	if b.registry.Add(s) != id {
		return errors.New("seriesindex: identity hash mismatch")
	}
	if b.names == nil {
		b.names, b.values = make(map[string]*namePosting), make(map[string]*valuePosting)
		b.bytes += 2 * memsize.MapBase
	}
	ref := uint32(len(b.records))
	prior := cap(b.records)
	b.records = append(b.records, record{id: id, meta: meta})
	b.bytes += int64(cap(b.records)-prior) * memsize.Of[record]()
	visitLabels(s, func(name []byte, value signal.Value) { b.addLabel(ref, name, value) })
	return nil
}

// UpdateMetadata replaces metadata for the last series, whose samples may span several runs.
func (b *Builder) UpdateMetadata(meta Metadata) {
	if len(b.records) > 0 {
		b.records[len(b.records)-1].meta = meta
	}
}

func visitLabels(s signal.Series, visit func([]byte, signal.Value)) {
	for _, kv := range s.Resource.Attributes {
		visit(kv.Key, kv.Value)
	}
	for _, kv := range s.Scope.Attributes {
		visit(kv.Key, kv.Value)
	}
	if len(s.Scope.Name) > 0 {
		visit([]byte(signal.LabelScopeName), signal.StringValue(s.Scope.Name))
	}
	if len(s.Scope.Version) > 0 {
		visit([]byte(signal.LabelScopeVersion), signal.StringValue(s.Scope.Version))
	}
	for _, kv := range s.Attributes {
		visit(kv.Key, kv.Value)
	}
}

func appendEscaped(dst, src []byte) []byte {
	for _, b := range src {
		dst = append(dst, b)
		if b == 0 {
			dst = append(dst, 255)
		}
	}
	return append(dst, 0, 0)
}

func decodeEscaped(dst, src []byte) ([]byte, int, error) {
	for i := 0; i < len(src); i++ {
		if src[i] != 0 {
			dst = append(dst, src[i])
			continue
		}
		if i+1 == len(src) {
			break
		}
		i++
		switch src[i] {
		case 0:
			return dst, i + 1, nil
		case 255:
			dst = append(dst, 0)
		default:
			return nil, 0, errors.Wrap(ErrCorrupt, "escaped label")
		}
	}
	return nil, 0, errors.Wrap(ErrCorrupt, "unterminated label")
}

func idKey(id signal.SeriesID) []byte {
	key := binary.BigEndian.AppendUint64([]byte{'i'}, id.Hi)
	return binary.BigEndian.AppendUint64(key, id.Lo)
}

func recordKey(ref uint32) []byte { return binary.BigEndian.AppendUint32([]byte{'s'}, ref) }

// Write emits one complete, range-readable index without taking ownership of dst.
func (b *Builder) Write(dst io.Writer) error {
	w := NewTableWriter(dst)
	w.seriesCount = uint32(len(b.records))
	var count [4]byte
	binary.BigEndian.PutUint32(count[:], uint32(len(b.records)))
	if err := w.Append([]byte{'c'}, count[:]); err != nil {
		return err
	}
	names := make([]string, 0, len(b.names))
	for name := range b.names {
		names = append(names, name)
	}
	slices.Sort(names)
	for _, name := range names {
		if err := b.writePostings(w, appendEscaped([]byte{'h'}, []byte(name)), b.names[name].refs); err != nil {
			return err
		}
	}
	for i, r := range b.records {
		binary.BigEndian.PutUint32(count[:], uint32(i))
		if err := w.Append(idKey(r.id), count[:]); err != nil {
			return err
		}
	}
	for _, name := range names {
		value := byte(0)
		if b.names[name].nonString {
			value = 1
		}
		if err := w.Append(appendEscaped([]byte{'n'}, []byte(name)), []byte{value}); err != nil {
			return err
		}
	}
	values := make([]string, 0, len(b.values))
	for key := range b.values {
		values = append(values, key)
	}
	slices.Sort(values)
	for _, key := range values {
		if err := b.writePostings(w, []byte(key), b.values[key].refs); err != nil {
			return err
		}
	}
	var value []byte
	var attrs signal.Attributes
	for i, r := range b.records {
		value = binary.BigEndian.AppendUint64(value[:0], r.id.Hi)
		value = binary.BigEndian.AppendUint64(value, r.id.Lo)
		value = binary.BigEndian.AppendUint64(value, uint64(r.meta.Covered))
		flag := byte(0)
		if r.meta.HasStats {
			flag = 1
		}
		value = append(value, flag)
		if r.meta.HasStats {
			value = binary.BigEndian.AppendUint64(value, uint64(r.meta.Stats.Count))
			for _, f := range []float64{r.meta.Stats.Sum, r.meta.Stats.Min, r.meta.Stats.Max} {
				value = binary.BigEndian.AppendUint64(value, math.Float64bits(f))
			}
		}
		entry := b.Entry(i, attrs)
		value = entry.Series.AppendHashInput(value)
		attrs = entry.Series.Attributes
		if err := w.Append(recordKey(uint32(i)), value); err != nil {
			return err
		}
	}
	return w.Finish()
}

// WriteObject publishes an immutable index object; its enclosing part still needs catalog publication.
func (b *Builder) WriteObject(ctx context.Context, dst backend.Backend, key string) error {
	w, err := backend.CreateObjectDeferred(ctx, dst, key)
	if err != nil {
		return err
	}
	defer w.Abort()
	if err := b.Write(w); err != nil {
		return err
	}
	return w.Commit(ctx)
}

// Index opens identities by canonical ID and local reference, retaining no per-series map.
type Index struct {
	table *Table
	count uint32
}

// Open validates the index header using bounded ranged reads.
func Open(ctx context.Context, b backend.Backend, key string) (*Index, error) {
	t, err := OpenTable(ctx, b, key)
	if err != nil {
		return nil, err
	}
	count := t.seriesCount
	if uint64(count)*2+1 > uint64(t.count) {
		return nil, errors.Wrap(ErrCorrupt, "series count bounds")
	}
	return &Index{table: t, count: count}, nil
}

// Count returns the number of distinct identities in this part.
func (r *Index) Count() uint32 { return r.count }

// MaxPageBytes bounds one cursor's encoded/decoded workspace.
func (r *Index) MaxPageBytes() int64 { return r.table.MaxPageBytes() }

// Lookup retrieves an owned identity and metadata by canonical ID.
func (r *Index) Lookup(ctx context.Context, id signal.SeriesID) (signal.Series, Metadata, bool, error) {
	c := r.table.Cursor(ctx)
	key := idKey(id)
	if !c.Seek(key) {
		return signal.Series{}, Metadata{}, false, c.Err()
	}
	if !bytes.Equal(c.Key(), key) {
		return signal.Series{}, Metadata{}, false, nil
	}
	if len(c.Value()) != 4 {
		return signal.Series{}, Metadata{}, false, errors.Wrap(ErrCorrupt, "reference")
	}
	ref := binary.BigEndian.Uint32(c.Value())
	got, s, meta, err := r.Series(ctx, ref)
	if err != nil {
		return signal.Series{}, Metadata{}, false, err
	}
	if got != id {
		return signal.Series{}, Metadata{}, false, errors.Wrap(ErrCorrupt, "reference identity mismatch")
	}
	return s, meta, true, nil
}

// Series returns an owned identity at a part-local reference.
func (r *Index) Series(ctx context.Context, ref uint32) (signal.SeriesID, signal.Series, Metadata, error) {
	if ref >= r.count {
		return signal.SeriesID{}, signal.Series{}, Metadata{}, errors.Wrap(ErrCorrupt, "reference bounds")
	}
	c := r.table.Cursor(ctx)
	key := recordKey(ref)
	if !c.Seek(key) {
		if c.Err() != nil {
			return signal.SeriesID{}, signal.Series{}, Metadata{}, c.Err()
		}
		return signal.SeriesID{}, signal.Series{}, Metadata{}, errors.Wrap(ErrCorrupt, "missing identity")
	}
	if !bytes.Equal(c.Key(), key) {
		return signal.SeriesID{}, signal.Series{}, Metadata{}, errors.Wrap(ErrCorrupt, "missing identity")
	}
	id, data, meta, err := decodeRecord(c.Value())
	if err != nil {
		return signal.SeriesID{}, signal.Series{}, Metadata{}, err
	}
	owned := bytes.Clone(data)
	s, n, err := signal.DecodeSeries(owned)
	if err != nil {
		return signal.SeriesID{}, signal.Series{}, Metadata{}, errors.Wrap(ErrCorrupt, "identity payload")
	}
	if n != len(owned) || signal.HashBytes(owned) != id {
		return signal.SeriesID{}, signal.Series{}, Metadata{}, errors.Wrap(ErrCorrupt, "identity hash")
	}
	return id, s, meta, nil
}

func decodeRecord(data []byte) (signal.SeriesID, []byte, Metadata, error) {
	if len(data) < 25 {
		return signal.SeriesID{}, nil, Metadata{}, errors.Wrap(ErrCorrupt, "short identity")
	}
	id := signal.SeriesID{Hi: binary.BigEndian.Uint64(data), Lo: binary.BigEndian.Uint64(data[8:])}
	meta := Metadata{Covered: int64(binary.BigEndian.Uint64(data[16:]))}
	flags := data[24]
	data = data[25:]
	if flags > 1 {
		return signal.SeriesID{}, nil, Metadata{}, errors.Wrap(ErrCorrupt, "identity flags")
	}
	if flags == 1 {
		if len(data) < 32 {
			return signal.SeriesID{}, nil, Metadata{}, errors.Wrap(ErrCorrupt, "short stats")
		}
		meta.HasStats = true
		meta.Stats = Stats{Count: int64(binary.BigEndian.Uint64(data)),
			Sum: math.Float64frombits(binary.BigEndian.Uint64(data[8:])),
			Min: math.Float64frombits(binary.BigEndian.Uint64(data[16:])),
			Max: math.Float64frombits(binary.BigEndian.Uint64(data[24:]))}
		data = data[32:]
	}
	return id, data, meta, nil
}

func (b *Builder) addLabel(ref uint32, name []byte, value signal.Value) {
	n := b.names[string(name)]
	if n == nil {
		n = &namePosting{}
		b.names[string(name)] = n
		b.bytes += int64(len(name)) + memsize.MapEntry[string, *namePosting]() + memsize.Of[namePosting]()
	}
	n.nonString = n.nonString || value.Kind() != signal.KindStr
	if len(n.refs) == 0 || n.refs[len(n.refs)-1] != ref {
		prior := cap(n.refs)
		n.refs = append(n.refs, ref)
		b.bytes += int64(cap(n.refs)-prior) * 4
	}
	key := appendEscaped([]byte{'p'}, name)
	key = appendEscaped(key, signal.AppendValue(nil, value))
	p := b.values[string(key)]
	if p == nil {
		p = &valuePosting{key: string(key)}
		b.values[p.key] = p
		b.bytes += int64(len(key)) + memsize.MapEntry[string, *valuePosting]() + memsize.Of[valuePosting]()
	}
	if len(p.refs) > 0 && p.refs[len(p.refs)-1] == ref {
		return
	}
	prior := cap(p.refs)
	p.refs = append(p.refs, ref)
	b.bytes += int64(cap(p.refs)-prior) * 4
}

func (b *Builder) writePostings(w *TableWriter, prefix []byte, refs []uint32) error {
	var key, value []byte
	for len(refs) > 0 {
		n := min(postingChunk, len(refs))
		key = append(key[:0], prefix...)
		key = binary.BigEndian.AppendUint32(key, refs[n-1])
		value = binary.AppendUvarint(value[:0], uint64(n))
		prior := uint32(0)
		for _, ref := range refs[:n] {
			value = binary.AppendUvarint(value, uint64(ref-prior))
			prior = ref
		}
		if err := w.Append(key, value); err != nil {
			return err
		}
		refs = refs[n:]
	}
	return nil
}
