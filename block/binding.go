package block

import (
	"slices"

	"github.com/go-faster/errors"

	"github.com/oteldb/storage/encoding/chunk"
)

// Binding appends a source column's dictionary-encoded rows to one [StreamWriter] bytes column
// without hashing a value per row: it caches, per entry of the table it is bound to, where that
// entry sits in the granule being staged and in the column dictionary.
//
// A binding belongs to its writer and dies with it; it is not safe for concurrent use.
type Binding struct {
	c       *streamColumn
	entries [][]byte
	gen     DictGen
	stable  bool

	// Per bound entry: the granule generation its gid is valid for, that gid, and its column
	// dictionary id (-1 until found there). A dictionary id stays valid for the writer's life.
	stamp []uint32
	gid   []int32
	did   []int32
	epoch uint32
}

// Binding returns a new binding to the i-th column, which must be [KindBytes].
func (w *StreamWriter) Binding(i int) (*Binding, error) {
	c, err := w.bytesColumnAt(i)
	if err != nil {
		return nil, err
	}

	b := &Binding{c: c}
	c.bytes.bindings = append(c.bytes.bindings, b)

	return b, nil
}

// Bind points b at entries, named by gen. The writer copies each value it keeps, so entries need
// stay valid only until the next Bind.
func (b *Binding) Bind(entries [][]byte, gen DictGen) error { return b.bind(entries, gen, false) }

// BindStable is [Binding.Bind] over entries that are never overwritten, which the writer keeps by
// reference instead of copying: a decoder's shared dictionary, valid for the decoder's life, or a
// caller's own table. A self-granule table aliases a reused frame and is refused.
func (b *Binding) BindStable(entries [][]byte, gen DictGen) error { return b.bind(entries, gen, true) }

// AppendDict appends rows [lo,hi) of g, those keep marks when keep is non-nil (keep[j] for row
// lo+j), to the bound column. g's table must be the bound one, and g must still be live: a granule a
// later decode may have overwritten is refused.
func (b *Binding) AppendDict(g DecodedGranule, lo, hi int, keep []bool) error {
	c := b.c
	if c.bytes.finished {
		return errWriterFinished
	}

	if g.table != b.gen {
		return errors.Errorf("block: column %q: dictionary token is not the bound one", c.name)
	}

	if !g.live() {
		return errors.Errorf("block: column %q: granule was overwritten by a later decode", c.name)
	}

	dc := g.dc

	if lo < 0 || hi < lo || hi > dc.Len() {
		return errors.Errorf("block: column %q: rows [%d,%d) out of %d", c.name, lo, hi, dc.Len())
	}

	if keep != nil && len(keep) != hi-lo {
		return errors.Errorf("block: column %q: keep has %d rows, want %d", c.name, len(keep), hi-lo)
	}

	granuleSize := c.granuleSize

	if dc.IDWidth == 0 {
		for r := lo; r < hi; r++ {
			if len(keep) != 0 && !keep[r-lo] {
				continue
			}

			if err := c.appendByteValue(dc.Entries[r], b.stable, granuleSize); err != nil {
				return err
			}
		}

		return nil
	}

	if dc.IDWidth != 1 && dc.IDWidth != 2 {
		return errors.Errorf("block: column %q: id width %d", c.name, dc.IDWidth)
	}

	if len(dc.Entries) != len(b.entries) {
		return errors.Errorf("block: column %q: %d entries against a bound table of %d", c.name, len(dc.Entries), len(b.entries))
	}

	for r := lo; r < hi; r++ {
		if len(keep) != 0 && !keep[r-lo] {
			continue
		}

		e := dictID(dc, r)
		if int(e) >= len(b.entries) {
			return errors.Wrapf(ErrCorrupt, "column %q: id %d at row %d out of %d entries", c.name, e, r, len(b.entries))
		}

		if err := b.append(e, granuleSize); err != nil {
			return err
		}
	}

	return nil
}

func (b *Binding) bind(entries [][]byte, gen DictGen, stable bool) error {
	if b.c.bytes.finished {
		return errWriterFinished
	}

	if !gen.live() {
		return errors.New("block: bind to a zero or retired dictionary token")
	}

	if stable && gen.owner.framed {
		return errors.New("block: a frame-backed table cannot be bound stable")
	}

	b.entries, b.gen, b.stable = entries, gen, stable

	if b.c.bytes.raw {
		return nil
	}

	n := len(entries)
	b.stamp = slices.Grow(b.stamp[:0], n)[:n]
	b.gid = slices.Grow(b.gid[:0], n)[:n]
	b.did = slices.Grow(b.did[:0], n)[:n]

	clear(b.stamp)

	for i := range b.did {
		b.did[i] = -1
	}

	b.epoch = b.c.bytes.g.epoch

	return nil
}

func dictID(dc *chunk.DictColumn, r int) int32 {
	if dc.IDWidth == 1 {
		return int32(dc.IDs[r])
	}

	return int32(dc.IDs[2*r])<<8 | int32(dc.IDs[2*r+1])
}

func (b *Binding) append(e int32, granuleSize int) error {
	c := b.c
	bc := c.bytes
	v := b.entries[e]

	if bc.raw {
		return c.appendByteValue(v, b.stable, granuleSize)
	}

	g := &bc.g
	if b.epoch != g.epoch {
		clear(b.stamp)
		b.epoch = g.epoch
	}

	if b.stamp[e] != g.gen {
		var gid int32

		if did := b.did[e]; did >= 0 {
			gid = bc.gidOfD(did)
		} else {
			gid, b.did[e] = bc.gidOfValue(v, b.stable)
		}

		b.stamp[e], b.gid[e] = g.gen, gid
	}

	return c.appendGid(b.gid[e], len(v), granuleSize)
}
