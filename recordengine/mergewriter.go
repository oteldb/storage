package recordengine

import (
	"context"
	"slices"
	"unsafe"

	"github.com/go-faster/errors"

	"github.com/oteldb/storage/backend"
	"github.com/oteldb/storage/block"
	"github.com/oteldb/storage/encoding/chunk"
	"github.com/oteldb/storage/encoding/compress"
	"github.com/oteldb/storage/index/series"
	"github.com/oteldb/storage/internal/watermark"
)

// Column ordinals of a merged record part, fixed by the order [newRecordPartStreamWriter] declares
// them: the stream id, the timestamp, then the schema's int columns and its byte columns.
const (
	colStreamIdx = iota
	colTsIdx
	colSchemaIdx
)

// mergeGranuleRows is the granule a merge writes its output at, and so the step a merge checks an
// output part for sealing at: a part overshoots its bound by at most one granule. It is also the most
// rows a source run hands the merge at once.
const mergeGranuleRows = 8192

// recordPartStreamWriter writes one output part of a merge as its rows arrive: a
// [block.StreamWriter] whose compression frames go to the backend as they seal, plus the part's
// sidecars built from the same rows, O(distinct streams) for the stream-keyed ones and O(distinct
// tokens) for the blooms.
//
// Rows arrive in the part's (stream, ts) order, a run at a time; a stream may continue in the next
// part. Byte columns arrive as decoded source granules through per-source [block.Binding]s, so a row
// costs the writer a cached id rather than a hash of its value.
type recordPartStreamWriter struct {
	e      *Engine
	src    []*part
	w      *block.StreamWriter
	prefix string

	granule int
	rows    int
	decoded int64

	// cur is the stream being appended and curRows its rows so far: the stream id column is handed
	// one run per stream, so a run split across appends is one run in the column.
	cur     chunk.U128
	curRows int
	wmarks  []watermark.Entry
	minT    int64
	maxT    int64

	sinks []*columnSink
	// binds holds, per source and byte column, the binding to a table that outlives the source's
	// next decode and the one to a table that does not; each is made on the first rows it carries,
	// and bound counts them.
	binds []sourceBindings
	bound int

	// buffered is set over a backend that takes an object whole: every frame a column hands over
	// stays in RAM until the part commits.
	buffered bool

	// refs is the distinct side-store reference cells written, which bound the part's sidecars:
	// refCol is their byte column (-1 without a side store) and refBytes their resident size.
	refs     map[string]struct{}
	refCol   int
	refBytes int64
}

type sourceBindings struct {
	stable, framed       *block.Binding
	stableGen, framedGen block.DictGen
}

// newRecordPartStreamWriter starts an output part of a merge over src. comp, when non-nil, is the
// merge's compressor, which all its day writers share.
func newRecordPartStreamWriter(
	ctx context.Context, e *Engine, src []*part, comp *compress.Compressor,
) (*recordPartStreamWriter, error) {
	schema := e.cfg.Schema
	opts := []block.PartOption{
		block.WithSortKey(colTs), block.WithSizingStats(), block.WithGranuleSize(e.mergeGranule),
	}

	if e.cfg.MergeCompression != compress.AlgorithmNone {
		opts = append(opts, block.WithCompression(e.cfg.MergeCompression),
			block.WithCompressionLevel(e.cfg.MergeCompressionLevel))
	}

	if comp != nil {
		opts = append(opts, block.WithCompressors(comp))
	}

	w := &recordPartStreamWriter{
		e:        e,
		src:      src,
		prefix:   e.newPartPrefix(),
		granule:  e.mergeGranule,
		buffered: !backend.StreamsWrites(e.cfg.Backend),
		sinks:    make([]*columnSink, schema.numBytes()),
		binds:    make([]sourceBindings, len(src)*schema.numBytes()),
		refCol:   -1,
	}

	if e.cfg.SideStore != nil {
		ref, ok := schema.ref(e.cfg.SideStore.RefColumn())
		if !ok || ref.kind != KindBytes {
			return nil, errors.Errorf("side store reference column %q is not a byte column", e.cfg.SideStore.RefColumn())
		}

		w.refCol, w.refs = ref.idx, make(map[string]struct{})
	}
	w.w = block.NewStreamWriterTo(ctx, e.cfg.Backend, w.prefix, opts...)

	cols := make([]block.Column, 0, colSchemaIdx+schema.numInts()+schema.numBytes())
	cols = append(cols,
		block.Column{Name: colStream, Kind: block.KindInt128},
		block.Column{Name: colTs, Kind: block.KindInt64, Codec: chunk.CodecDoD, Block: true},
	)

	for k := range schema.intCols {
		col := schema.intColumn(k)
		cols = append(cols, block.Column{Name: col.Name, Kind: block.KindInt64, Codec: col.Codec, Block: true})
	}

	for k := range schema.byteCols {
		col := schema.byteColumn(k)
		cols = append(cols, block.Column{Name: col.Name, Kind: block.KindBytes, Codec: col.Codec, Block: true})
		w.sinks[k] = newColumnSink(schema, k)
	}

	for i := range cols {
		if err := w.w.AddColumn(cols[i]); err != nil {
			w.w.Abort()

			return nil, err
		}
	}

	return w, nil
}

// appendRows starts n = len(ts) rows of stream u, whose decoded size is bytes, by appending their
// timestamps; the other columns follow through appendInts and appendBytes.
func (w *recordPartStreamWriter) appendRows(u chunk.U128, ts []int64, bytes int64) error {
	if w.curRows == 0 || u != w.cur {
		if err := w.flushStream(); err != nil {
			return err
		}

		w.cur = u
		w.wmarks = append(w.wmarks, watermark.Entry{ID: u128ToID(u), Max: minInt64})
	}

	if w.rows == 0 {
		w.minT, w.maxT = maxInt64, minInt64
	}

	wm := &w.wmarks[len(w.wmarks)-1]
	for _, t := range ts {
		wm.Max = max(wm.Max, t)
		w.minT, w.maxT = min(w.minT, t), max(w.maxT, t)
	}

	w.curRows += len(ts)
	w.rows += len(ts)
	w.decoded += bytes

	return w.w.AppendInt64(colTsIdx, ts)
}

func (w *recordPartStreamWriter) flushStream() error {
	if w.curRows == 0 {
		return nil
	}

	n := w.curRows
	w.curRows = 0

	return w.w.AppendU128Run(colStreamIdx, w.cur, n)
}

func (w *recordPartStreamWriter) appendInts(k int, vals []int64) error {
	return w.w.AppendInt64(colSchemaIdx+k, vals)
}

// appendBytes appends rows [lo, hi) of g, a granule source si decoded, to byte column k. entries is
// the table g's ids index (nil when g holds a value per row), and stable says it stays intact for the
// merge rather than only until the source's next decode.
func (w *recordPartStreamWriter) appendBytes(
	si, k int, g block.DecodedGranule, entries [][]byte, stable bool, lo, hi int,
) error {
	b, err := w.binding(si, k, g.Table(), entries, stable)
	if err != nil {
		return err
	}

	if err := b.AppendDict(g, lo, hi, nil); err != nil {
		return err
	}

	if s := w.sinks[k]; s != nil {
		dc := g.Column()
		for r := lo; r < hi; r++ {
			s.add(dc.At(r))
		}
	}

	if k == w.refCol {
		w.addRefs(g.Column(), lo, hi)
	}

	return nil
}

func (w *recordPartStreamWriter) addRefs(dc chunk.DictColumn, lo, hi int) {
	const refEntryBytes = 48

	var last []byte

	for r := lo; r < hi; r++ {
		v := dc.At(r)
		if r > lo && slices.Equal(v, last) {
			continue
		}

		last = v

		if _, ok := w.refs[string(v)]; !ok {
			w.refs[string(v)] = struct{}{}
			w.refBytes += int64(len(v)) + refEntryBytes
		}
	}
}

func (w *recordPartStreamWriter) refCells(yield func([]byte) bool) {
	for ref := range w.refs {
		if !yield([]byte(ref)) {
			return
		}
	}
}

func (w *recordPartStreamWriter) binding(
	si, k int, gen block.DictGen, entries [][]byte, stable bool,
) (*block.Binding, error) {
	sb := &w.binds[si*len(w.sinks)+k]

	b, bound := &sb.framed, &sb.framedGen
	if stable {
		b, bound = &sb.stable, &sb.stableGen
	}

	if *b == nil {
		nb, err := w.w.Binding(colSchemaIdx + w.e.cfg.Schema.numInts() + k)
		if err != nil {
			return nil, err
		}

		*b = nb
		w.bound++
	}

	if *bound == gen {
		return *b, nil
	}

	bind := (*b).Bind
	if stable {
		bind = (*b).BindStable
	}

	if err := bind(entries, gen); err != nil {
		return nil, err
	}

	*bound = gen

	return *b, nil
}

// decodedBytes is the decoded size of the rows written so far: the part's [part.sizeBytes] once
// written, and what the merge cap is denominated in.
func (w *recordPartStreamWriter) decodedBytes() int64 { return w.decoded }

// residentBytes is everything the writer holds in RAM: the block writer's unsealed frames, directories,
// output dictionaries, staged granules and the per-entry caches of every source binding
// ([block.StreamWriter.ResidentBytes]), plus the sidecar state — a watermark per stream, and per bloom
// or attributes column its token hashes, value dedup and keys — and, over a backend that takes objects
// whole, the frames already handed over. It is what the router sheds writers on,
// so it must not undercount: the bindings alone are 12 B per entry of every source dictionary bound,
// for each open day.
func (w *recordPartStreamWriter) residentBytes() int64 {
	const (
		wmarkBytes   = 24
		bindingBytes = 128
	)

	n := w.w.ResidentBytes() + int64(cap(w.wmarks))*wmarkBytes
	if w.buffered {
		// A buffered object grows by append, so its capacity runs up to twice what it holds.
		n += 2 * w.w.EncodedBytes()
	}

	n += int64(cap(w.binds))*int64(unsafe.Sizeof(sourceBindings{})) + int64(w.bound)*bindingBytes

	for _, s := range w.sinks {
		n += s.residentBytes()
	}

	return n + w.refBytes
}

// abort releases the part's in-flight column objects; a no-op once the part is written.
func (w *recordPartStreamWriter) abort() { w.w.Abort() }

// finish writes the part and its sidecars in the order [writePart] does, opens it and stamps its
// time bounds. The router opens a writer only for rows it is about to append, so an empty one is a
// bug: it would burn a part id and leave an unreadable prefix.
func (w *recordPartStreamWriter) finish(ctx context.Context) (*part, error) {
	if w.rows == 0 {
		w.abort()

		return nil, errors.New("recordengine: finishing a merge output part with no rows")
	}

	e, b, prefix, schema := w.e, w.e.cfg.Backend, w.prefix, w.e.cfg.Schema

	if err := w.flushStream(); err != nil {
		w.abort()

		return nil, err
	}

	if err := block.WriteStreamPart(ctx, b, prefix, w.w); err != nil {
		return nil, errors.Wrapf(err, "write part %q", prefix)
	}

	if err := writeIdentity(ctx, b, prefix, w.identityEntries()); err != nil {
		return nil, err
	}

	for k, s := range w.sinks {
		if s == nil || s.bloom == nil {
			continue
		}

		name := schema.byteColumn(k).Name
		if err := backend.WriteDeferred(ctx, b, bloomKey(prefix, name), s.bloom.encode()); err != nil {
			return nil, errors.Wrapf(err, "write bloom %q", name)
		}
	}

	if err := backend.WriteDeferred(ctx, b, watermark.Key(prefix), watermark.Encode(nil, w.wmarks)); err != nil {
		return nil, errors.Wrapf(err, "write watermark sidecar %q", prefix)
	}

	if k, ok := schema.attrsByteCol(); ok {
		if keys := w.sinks[k].keys.sorted(); len(keys) > 0 {
			if err := backend.WriteDeferred(ctx, b, recordKeysKey(prefix), encodeRecordKeys(keys)); err != nil {
				return nil, errors.Wrap(err, "write record-keys footer")
			}
		}
	}

	p, err := openPart(ctx, b, schema, prefix, e.cfg.Obs.Corruption, e.readCompressors)
	if err != nil {
		return nil, err
	}

	p.minTime, p.maxTime = w.minT, w.maxT

	if e.cfg.SideStore != nil {
		if err := e.mergeSidecars(ctx, w.src, prefix, w.refCells); err != nil {
			return nil, err
		}
	}

	if err := backend.SyncPrefix(ctx, b, prefix); err != nil {
		return nil, errors.Wrapf(err, "sync part %q", prefix)
	}

	return p, nil
}

// identityEntries resolves the part's streams under one read lock.
func (w *recordPartStreamWriter) identityEntries() []series.Entry {
	e := w.e

	e.mu.RLock()
	defer e.mu.RUnlock()

	out := make([]series.Entry, 0, len(w.wmarks))

	for _, wm := range w.wmarks {
		if s, ok := e.head.series.Get(wm.ID); ok {
			out = append(out, series.Entry{ID: wm.ID, Series: s})
		}
	}

	return out
}
