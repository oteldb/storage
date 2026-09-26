package block

import (
	"bytes"
	"context"
	"math"

	"github.com/go-faster/errors"

	"github.com/oteldb/storage/encoding/chunk"
	"github.com/oteldb/storage/pool"
)

// errWriterFinished is returned by a bytes append or [Binding] call after the writer finished or
// aborted: its dictionary is gone, and bindings die with it.
var errWriterFinished = errors.New("block: stream writer column already finished")

// streamBytes is a [StreamWriter] bytes column's staging: one granule of raw values, or, for a
// dictionary column, the output granule G and the column dictionary D it joins at each seal.
type streamBytes struct {
	raw      bool
	obs      BytesObserver
	finished bool

	rawBlob []byte
	rawOffs []int32

	g      dictGranule
	d      *sharedDictBuilder
	darena byteArena
	// gOfD is G's gid of each D entry, valid while its stamp is G's generation: how a D value
	// reached through a cached id finds its place in G without hashing.
	gOfD []dictSlot
	ids  []int32

	bindings []*Binding
}

type dictSlot struct {
	stamp uint32
	gid   int32
}

// dictGranule is the output granule being staged: its distinct values in first-occurrence order
// (the order the chunk encoder assigns ids in), their counts and D ids, and each row's value.
type dictGranule struct {
	vals   [][]byte
	counts []uint64
	did    []int32
	rowGid []int32
	// index holds only values absent from D; a D value is found through gOfD.
	index     *pool.ByteIntMap
	arena     byteArena
	newCharge int64

	// gen stamps what is valid for this granule. It never takes 0, which is every fresh stamp;
	// epoch counts its wraps, so a binding's stamps from before one are cleared rather than trusted.
	gen   uint32
	epoch uint32
}

func newStreamBytes(raw bool, obs BytesObserver, dictCap int64) *streamBytes {
	bc := &streamBytes{raw: raw, obs: obs, rawOffs: []int32{0}}
	if !raw {
		bc.d = newSharedDictBuilder(dictCap, obs != nil)
		bc.d.index = pool.NewByteIntMap()
		bc.g.index = pool.NewByteIntMap()
		bc.g.gen = 1
	}

	return bc
}

func (g *dictGranule) add(v []byte, did int32) int32 {
	g.vals = append(g.vals, v)
	g.counts = append(g.counts, 0)
	g.did = append(g.did, did)

	return int32(len(g.vals) - 1)
}

func (g *dictGranule) addRow(gid int32) {
	g.rowGid = append(g.rowGid, gid)
	g.counts[gid]++
}

func (bc *streamBytes) resetGranule() {
	g := &bc.g

	clear(g.vals)
	g.vals, g.counts, g.did, g.rowGid = g.vals[:0], g.counts[:0], g.did[:0], g.rowGid[:0]

	if g.index.Len() > 0 {
		g.index.Reset()
	}

	g.arena.reset()
	g.newCharge = 0

	g.gen++
	if g.gen == 0 {
		g.gen = 1
		g.epoch++

		for i := range bc.gOfD {
			bc.gOfD[i].stamp = 0
		}
	}
}

// gidOfValue finds or adds v in G, returning its gid and, when it is in D, its D id (else -1). An
// unstable v is copied before G keeps it.
func (bc *streamBytes) gidOfValue(v []byte, stable bool) (gid, did int32) {
	g := &bc.g

	if x, ok := g.index.Get(v); ok {
		return int32(x), -1
	}

	if bc.d.index.Len() > 0 {
		if x, ok := bc.d.index.Get(v); ok {
			return bc.gidOfD(int32(x)), int32(x)
		}
	}

	if !stable {
		v = g.arena.copy(v)
	}

	gid = g.add(v, -1)
	g.index.Put(v, int(gid))

	_, charge := entryCharge(v)
	g.newCharge += charge

	return gid, -1
}

func (bc *streamBytes) gidOfD(did int32) int32 {
	s := &bc.gOfD[did]
	if s.stamp == bc.g.gen {
		return s.gid
	}

	gid := bc.g.add(bc.d.entries[did], did)
	*s = dictSlot{stamp: bc.g.gen, gid: gid}

	return gid
}

func (bc *streamBytes) release() {
	bc.finished = true

	if bc.g.index != nil {
		bc.g.index.PutBack()
		bc.d.index.PutBack()
		bc.g.index, bc.d.index = nil, nil
	}

	bc.g = dictGranule{}
	bc.d, bc.gOfD, bc.ids = nil, nil, nil
	bc.darena.release()
	bc.rawBlob, bc.rawOffs = nil, nil

	for _, b := range bc.bindings {
		b.entries, b.stamp, b.gid, b.did = nil, nil, nil, nil
	}

	bc.bindings = nil
}

func (bc *streamBytes) residentBytes() int64 {
	if bc == nil {
		return 0
	}

	g := &bc.g

	n := int64(cap(bc.rawBlob)) + int64(cap(bc.rawOffs))*4 + int64(cap(bc.ids))*4
	n += int64(cap(g.vals))*24 + int64(cap(g.counts))*8 + int64(cap(g.did)+cap(g.rowGid))*4 + g.arena.size()
	n += int64(cap(bc.gOfD))*8 + bc.darena.size()

	if g.index != nil {
		n += g.index.SizeBytes() + bc.d.index.SizeBytes()
	}

	if bc.d != nil {
		n += int64(cap(bc.d.entries))*24 + int64(cap(bc.d.counts))*8
	}

	for _, b := range bc.bindings {
		n += int64(cap(b.stamp)+cap(b.gid)+cap(b.did)) * 4
	}

	return n
}

// AppendBytes appends vals to the i-th column, copying each value it keeps.
func (w *StreamWriter) AppendBytes(i int, vals [][]byte) error {
	c, err := w.bytesColumnAt(i)
	if err != nil {
		return err
	}

	for _, v := range vals {
		if err := c.appendByteValue(v, false, w.granuleSize); err != nil {
			return err
		}
	}

	return nil
}

// AppendBytesBlob appends the rows of a blob column — row k is blob[offsets[k]:offsets[k+1]] — to
// the i-th column, as [Column.BytesBlob] describes them.
func (w *StreamWriter) AppendBytesBlob(i int, blob []byte, offsets []int32) error {
	c, err := w.bytesColumnAt(i)
	if err != nil {
		return err
	}

	for k := 1; k < len(offsets); k++ {
		lo, hi := offsets[k-1], offsets[k]
		if lo < 0 || hi < lo || int(hi) > len(blob) {
			return errors.Errorf("block: column %q: offsets [%d,%d) out of blob %d", c.name, lo, hi, len(blob))
		}

		if err := c.appendByteValue(blob[lo:hi], false, w.granuleSize); err != nil {
			return err
		}
	}

	return nil
}

func (w *StreamWriter) bytesColumnAt(i int) (*streamColumn, error) {
	c, err := w.column(i, KindBytes)
	if err != nil {
		return nil, err
	}

	if c.bytes.finished {
		return nil, errWriterFinished
	}

	return c, nil
}

func (c *streamColumn) appendByteValue(v []byte, stable bool, granuleSize int) error {
	bc := c.bytes

	if bc.raw {
		if len(bc.rawBlob) > math.MaxInt32-len(v) {
			return errors.Errorf("block: column %q: granule exceeds %d bytes", c.name, math.MaxInt32)
		}

		c.trackBytesRow(v)
		c.rows++
		c.raw += int64(len(v))

		bc.rawBlob = append(bc.rawBlob, v...)
		bc.rawOffs = append(bc.rawOffs, int32(len(bc.rawBlob)))

		if len(bc.rawOffs)-1 >= granuleSize {
			return c.sealRaw()
		}

		return nil
	}

	gid, _ := bc.gidOfValue(v, stable)

	return c.appendGid(gid, len(v), granuleSize)
}

func (c *streamColumn) appendGid(gid int32, size, granuleSize int) error {
	c.rows++
	c.raw += int64(size)
	c.bytes.g.addRow(gid)

	if len(c.bytes.g.rowGid) >= granuleSize {
		return c.sealDict()
	}

	return nil
}

// trackBytesRow folds one row into the constant tracking, dropping the retained first value as soon
// as a row differs so a large one is not held for the column's life.
func (c *streamColumn) trackBytesRow(v []byte) {
	switch {
	case !c.allSame:
	case !c.haveStats:
		c.first, c.haveStats = append([]byte(nil), v...), true
	case !bytes.Equal(v, c.first):
		c.allSame, c.first = false, nil
	}
}

// trackBytesGranule is [streamColumn.trackBytesRow] over a sealed granule's distinct values.
func (c *streamColumn) trackBytesGranule(vals [][]byte) {
	c.trackBytesRow(vals[0])

	if len(vals) > 1 {
		c.allSame, c.first = false, nil
	}
}

func (c *streamColumn) sealRaw() error {
	bc := c.bytes
	rows := len(bc.rawOffs) - 1

	if err := c.blk.addGranule(func(dst []byte) ([]byte, error) {
		return chunk.EncodeBytesRawBlob(dst, bc.rawBlob, bc.rawOffs), nil
	}); err != nil {
		return err
	}

	c.encoded += rows
	bc.rawBlob, bc.rawOffs = bc.rawBlob[:0], bc.rawOffs[:1]

	return c.maybeAttach()
}

// sealDict decides the staged granule against D exactly as [sharedDictBuilder] does for
// [PartWriter], so the two write the same granule streams.
func (c *streamColumn) sealDict() error {
	bc := c.bytes
	g, d := &bc.g, bc.d
	rows := len(g.rowGid)

	var err error

	if d.joins(len(g.vals), rows, g.newCharge) {
		mode := bc.joinGranule()
		err = c.blk.addGranule(func(dst []byte) ([]byte, error) { return appendSharedIDs(dst, mode, bc.ids), nil })
	} else {
		err = c.blk.addGranule(func(dst []byte) ([]byte, error) {
			return chunk.EncodeBytesDictRange(append(dst, modeSelf), g.vals, g.rowGid, 0, rows), nil
		})

		if err == nil && bc.obs != nil {
			bc.obs.SelfGranule(g.vals, g.counts)
		}
	}

	if err != nil {
		return err
	}

	c.trackBytesGranule(g.vals)
	c.encoded += rows
	bc.resetGranule()

	return c.maybeAttach()
}

// joinGranule inserts G's new values into D, fills ids with each row's D id and returns the mode
// the granule is written at.
func (bc *streamBytes) joinGranule() byte {
	g, d := &bc.g, bc.d

	for gid, did := range g.did {
		if did < 0 {
			v := bc.darena.copy(g.vals[gid])
			did = d.insert(v)
			d.index.Put(v, int(did))
			bc.gOfD = append(bc.gOfD, dictSlot{})
			g.did[gid] = did
		}

		if d.observe {
			d.counts[did] += g.counts[gid]
		}
	}

	bc.ids = bc.ids[:0]
	for _, gid := range g.rowGid {
		bc.ids = append(bc.ids, g.did[gid])
	}

	if d.width() == 2 {
		return modeSharedWide
	}

	return modeSharedNarrow
}

func (c *streamColumn) flushBytes() error {
	switch bc := c.bytes; {
	case bc.raw && len(bc.rawOffs) > 1:
		return c.sealRaw()
	case !bc.raw && len(bc.g.rowGid) > 0:
		return c.sealDict()
	default:
		return nil
	}
}

// finishDict completes a non-constant dictionary column. A buffered column no granule joined is
// rewritten as the single stream [PartWriter] writes for it; a streamed one has already handed its
// frames out, so it keeps the trailer layout with an empty dictionary.
func (c *streamColumn) finishDict(
	ctx context.Context, desc ColumnDesc, granuleSize int, sizing bool,
) (ColumnDesc, []byte, int64, error) {
	bc := c.bytes

	if err := c.blk.seal(); err != nil {
		return ColumnDesc{}, nil, 0, err
	}

	if len(bc.d.entries) == 0 && !c.blk.streams() {
		stream, err := c.blk.unframeSelf(c.rows)
		if err != nil {
			return ColumnDesc{}, nil, 0, err
		}

		c.blk.discard()

		if bc.obs != nil {
			bc.obs.Dictionary(nil, nil)
		}

		bc.release()

		desc, obj := unframedColumn(desc, c.comp, stream, sizing)

		return desc, obj, int64(len(obj)), nil
	}

	if bc.obs != nil {
		bc.obs.Dictionary(bc.d.entries, bc.d.counts)
	}

	region, raw := bc.d.region(c.comp)
	dict := trailerDict{off: int64(c.blk.bytes), length: int64(len(region)), raw: raw, entries: int64(len(bc.d.entries))}

	bc.release()

	obj, n, err := c.blk.finish(ctx, granuleSize, region)
	if err != nil {
		return ColumnDesc{}, nil, 0, err
	}

	dict.apply(&desc)

	if sizing {
		desc.HasSizing, desc.Sizing = true, c.blk.sizing()
	}

	return desc, obj, n, nil
}

// unframeSelf decodes a buffered column of self granules back to its rows and encodes them as one
// chunk bytes stream.
func (a *blockAccum) unframeSelf(rows int) ([]byte, error) {
	vals := make([][]byte, 0, rows)
	g := 0

	for i, f := range a.frames {
		frame, err := a.comp.Decompress(nil, f)
		if err != nil {
			return nil, errors.Wrap(err, "decompress own frame")
		}

		for range a.frameGranules[i] {
			stream := frame[1:a.gLens[g]]
			frame = frame[a.gLens[g]:]
			g++

			var dc chunk.DictColumn
			if _, err := dc.DecodeBytes(stream); err != nil {
				return nil, errors.Wrap(err, "decode own granule")
			}

			for r := range dc.Len() {
				vals = append(vals, dc.At(r))
			}
		}
	}

	return chunk.EncodeBytes(nil, vals), nil
}
