package block

import (
	"encoding/binary"
	"math"
	"math/bits"
	"sort"
	"unsafe"

	"github.com/go-faster/errors"
)

// chunkGeometry owns immutable packed values; decoder clones share it without retaining input bytes.
type chunkGeometry struct {
	rows, minima, spans           []byte
	minBase                       int64
	spanBase                      uint64
	fixedRows                     int64
	count                         int
	rowWidth, minWidth, spanWidth uint8
	timeBounds                    bool
}

func directoryWidth(n uint64) uint8 { return uint8((bits.Len64(n) + 7) / 8) }

// Packed arrays carry seven trailing bytes for a bounded unaligned load at their last value.
func directoryUint(data []byte, at int, width uint8) uint64 {
	if width == 0 {
		return 0
	}
	data = data[at*int(width):]
	return binary.LittleEndian.Uint64(data) & (math.MaxUint64 >> ((8 - width) * 8))
}

// appendDirectoryUint writes increasing indices; its excess bytes land in unwritten values or padding.
func appendDirectoryUint(data []byte, at int, width uint8, value uint64) {
	if width == 0 {
		return
	}
	binary.LittleEndian.PutUint64(data[at*int(width):], value)
}

func (g *chunkGeometry) row(at int) int64 {
	if g.fixedRows != 0 {
		return int64(at) * g.fixedRows
	}
	return int64(directoryUint(g.rows, at, g.rowWidth))
}

func (g *chunkGeometry) blockAt(row int) int {
	if g.fixedRows != 0 {
		return int(int64(row) / g.fixedRows)
	}
	return sort.Search(g.count+1, func(i int) bool { return g.row(i) > int64(row) }) - 1
}

func (g *chunkGeometry) bounds(at int) (int64, int64, bool) {
	if !g.timeBounds || at < 0 || at >= g.count {
		return 0, 0, false
	}
	lo := g.minBase
	if g.minima != nil {
		lo += int64(directoryUint(g.minima, at, g.minWidth))
	}
	span := g.spanBase
	if g.spans != nil {
		span += directoryUint(g.spans, at, g.spanWidth)
	}
	return lo, lo + int64(span), true
}

func (g *chunkGeometry) bytes() int64 {
	return int64(unsafe.Sizeof(*g)) + int64(len(g.rows)+len(g.minima)+len(g.spans))
}

func readChunkGeometry(c *dirCursor, count int, chk dirCheck) (*chunkGeometry, error) {
	rowCeiling := maxPartRows
	if chk.rows > 0 {
		rowCeiling = uint64(chk.rows)
	}
	timeBase, timeWidth := int64(math.MinInt64), uint8(8)
	if chk.timeRange {
		timeBase = chk.minTime
		timeWidth = directoryWidth(uint64(chk.maxTime) - uint64(chk.minTime))
	}
	g := &chunkGeometry{count: count, timeBounds: chk.timeBounds,
		rowWidth: directoryWidth(rowCeiling), minWidth: timeWidth, spanWidth: timeWidth}
	var rows, previous int64
	for i := range count {
		n, err := c.uvarint("chunk rows")
		if err != nil {
			return nil, err
		}
		if n == 0 || n > uint64(chk.maxBlockRows) || uint64(rows)+n > maxPartRows || uint64(rows)+n > rowCeiling {
			return nil, errors.Wrap(ErrCorrupt, "invalid chunk row count")
		}
		rows += int64(n)
		if i == 0 {
			g.fixedRows = int64(n)
		}
		if g.rows == nil && int64(n) != g.fixedRows {
			g.expandRows(i)
		}
		if g.rows != nil {
			appendDirectoryUint(g.rows, i+1, g.rowWidth, uint64(rows))
		}
		if !chk.timeBounds {
			continue
		}
		delta, err := c.varint("chunk minimum")
		if err != nil {
			return nil, err
		}
		span, err := c.varint("chunk maximum")
		if err != nil {
			return nil, err
		}
		lo := previous + delta
		hi := lo + span
		if lo > hi || chk.timeRange && (lo < chk.minTime || hi > chk.maxTime) {
			return nil, errors.Wrap(ErrCorrupt, "invalid chunk time bounds")
		}
		g.putBounds(i, lo, hi, timeBase)
		previous = lo
	}
	if chk.rows > 0 && rows != int64(chk.rows) {
		return nil, errors.Wrap(ErrCorrupt, "chunk rows differ from manifest")
	}
	return g, nil
}

func (g *chunkGeometry) expandRows(i int) {
	g.rows = make([]byte, (g.count+1)*int(g.rowWidth)+7)
	for j := 1; j <= i; j++ {
		appendDirectoryUint(g.rows, j, g.rowWidth, uint64(j)*uint64(g.fixedRows))
	}
	g.fixedRows = 0
}

func (g *chunkGeometry) putBounds(i int, lo, hi, timeBase int64) {
	// Unsigned differences preserve wire deltas spanning the entire int64 range.
	width := uint64(hi) - uint64(lo)
	if i == 0 {
		g.minBase, g.spanBase = lo, width
	}
	if g.minima == nil && lo != g.minBase {
		g.minima = make([]byte, g.count*int(g.minWidth)+7)
		for j := range i {
			appendDirectoryUint(g.minima, j, g.minWidth, uint64(g.minBase)-uint64(timeBase))
		}
		g.minBase = timeBase
	}
	if g.minima != nil {
		appendDirectoryUint(g.minima, i, g.minWidth, uint64(lo)-uint64(g.minBase))
	}
	if g.spans == nil && width != g.spanBase {
		g.spans = make([]byte, g.count*int(g.spanWidth)+7)
		for j := range i {
			appendDirectoryUint(g.spans, j, g.spanWidth, g.spanBase)
		}
		g.spanBase = 0
	}
	if g.spans != nil {
		appendDirectoryUint(g.spans, i, g.spanWidth, width)
	}
}

func chunkGeometryBound(desc ColumnDesc, rows int64) int64 {
	n := int64(unsafe.Sizeof(chunkGeometry{}))
	count := desc.Sizing.NumGranules
	if rows <= 0 {
		rows = int64(maxPartRows)
	}
	if rows != count && rows != satMul(count, int64(desc.MaxBlockRows)) {
		n = satAdd(n, satAdd(7, satMul(count+1, int64(directoryWidth(uint64(rows))))))
	}
	if desc.TimeBounds {
		width := directoryWidth(uint64(desc.MaxInt64) - uint64(desc.MinInt64))
		if width > 0 {
			n = satAdd(n, satAdd(14, satMul(count, int64(width)*2)))
		}
	}
	return n
}
