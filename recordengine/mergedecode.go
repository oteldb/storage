package recordengine

import (
	"context"
	"encoding/binary"
	"slices"
	"unsafe"

	"github.com/oteldb/storage/block"
	"github.com/oteldb/storage/encoding/chunk"
)

// decodedPart is one source part decoded whole for a merge, the fallback for a part a forward cursor
// cannot read ([forwardReadable]): the fixed-width columns as plain slices, and each byte column as
// decoded — dictionary-compressed where the part wrote it so. It stays resident for the whole stream
// sweep, so the writer may keep its values by reference.
type decodedPart struct {
	ts    []int64
	ints  [][]int64
	bytes []arrayBytes
	// tsSorted is whether every stream's rows are ts-ascending, as both part writers leave them. Only then
	// may a merge binary-search a stream's window: on a part that breaks the order, a search would skip
	// in-window rows, and the merge would retire the part that held them.
	tsSorted bool
}

// readForMerge decodes the whole part for a merge. It reads off the engine lock (the part is ref-held
// live by the merge until publish), so a fetch and this decode never race a delete.
func (p *part) readForMerge(ctx context.Context) (*decodedPart, error) {
	d := &decodedPart{
		ints:  make([][]int64, p.schema.numInts()),
		bytes: make([]arrayBytes, p.schema.numBytes()),
	}

	var err error
	if d.ts, err = p.readInt64(ctx, colTs, nil, nil); err != nil {
		return nil, err
	}

	d.tsSorted = streamsTSSorted(d.ts, p.ranges)

	for k := range d.ints {
		if d.ints[k], err = p.readInt64(ctx, p.schema.intColumn(k).Name, nil, nil); err != nil {
			return nil, err
		}
	}

	for k := range d.bytes {
		col, err := p.reader.Column(ctx, p.schema.byteColumn(k).Name)
		if err != nil {
			return nil, err
		}

		dc, err := col.Bytes()
		if err != nil {
			return nil, err
		}

		b := arrayBytes{col: dc, g: block.OwnedGranule(dc, block.NewDictGen()), stable: true}
		if dc.IDWidth != 0 {
			b.entries = dc.Entries
		}

		d.bytes[k] = b
	}

	return d, nil
}

// residentBytes is what the decoded part holds.
func (d *decodedPart) residentBytes() int64 {
	n := int64(cap(d.ts)) * 8

	for k := range d.ints {
		n += int64(cap(d.ints[k])) * 8
	}

	for k := range d.bytes {
		n += dictColumnBytes(d.bytes[k].col)
	}

	return n
}

// viewBytes is a []byte header.
const viewBytes = int64(unsafe.Sizeof([]byte(nil)))

// dictColumnBytes is what a decoded bytes column holds: its values with the length prefix each is
// decoded from, a view per entry, and its ids.
func dictColumnBytes(dc *chunk.DictColumn) int64 {
	n := int64(cap(dc.IDs)) + int64(cap(dc.Entries))*(viewBytes+binary.MaxVarintLen32)

	for _, e := range dc.Entries {
		n += int64(len(e))
	}

	return n
}

func streamsTSSorted(ts []int64, ranges []streamRange) bool {
	for _, r := range ranges {
		if r.end > len(ts) || !slices.IsSorted(ts[r.start:r.end]) {
			return false
		}
	}

	return true
}
