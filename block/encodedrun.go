package block

import (
	"encoding/binary"
	"math"
	"slices"

	"github.com/go-faster/errors"
)

type encodedRun struct {
	tsCol, valueCol                int
	ts, value                      EncodedGranule
	stats                          GranuleStats
	rows, groups                   int
	first, last, step              int64
	fragments, payload, timestamps []byte
}

// AppendEncodedRun copies aligned timestamp/value granules, coalescing regular cadence up to the row cap.
// EndGroup prevents joining granules from different logical series.
func (w *StreamWriter) AppendEncodedRun(tsCol, valueCol int, ts, value EncodedGranule, stats GranuleStats) error {
	if tsCol == valueCol || ts.Kind != KindInt64 || value.Kind != KindFloat64 || ts.Rows != value.Rows {
		return errors.New("block: encoded run requires aligned timestamp and float granules")
	}
	if err := w.checkEncodedGranule(tsCol, ts); err != nil {
		return err
	}
	if err := w.checkEncodedGranule(valueCol, value); err != nil {
		return err
	}
	if math.IsNaN(stats.MinFloat64) != math.IsNaN(stats.MaxFloat64) || stats.MinFloat64 > stats.MaxFloat64 {
		return errors.New("block: invalid encoded float bounds")
	}
	p := &w.encodedRun
	first, step, regular := regularGranule(ts)
	if !regular {
		if err := w.flushEncodedRun(); err != nil {
			return err
		}
		if err := w.appendEncodedGranule(tsCol, ts, GranuleStats{}, true); err != nil {
			return err
		}
		return w.appendEncodedGranule(valueCol, value, stats, true)
	}
	joinStep, join := p.canJoin(tsCol, valueCol, ts, first, step, w.cols[tsCol].granuleSize)
	if p.rows != 0 && !join {
		if err := w.flushEncodedRun(); err != nil {
			return err
		}
	}
	if p.rows == 0 {
		p.tsCol, p.valueCol = tsCol, valueCol
		p.ts, p.value, p.stats = ts, value, stats
		p.ts.Data, p.value.Data = nil, nil
		p.first, p.last, p.step = first, ts.MaxTime, step
	} else {
		p.step, p.last = joinStep, ts.MaxTime
		p.stats.add(stats.MinFloat64, false)
		p.stats.add(stats.MaxFloat64, false)
	}
	// Framing was checked with the borrow; only the two container counts are skipped here.
	src := value.Data[1:]
	_, n := binary.Uvarint(src)
	groups, m := binary.Uvarint(src[n:])
	p.fragments = append(p.fragments, src[n+m:]...)
	p.groups += int(groups)
	p.rows += ts.Rows
	for _, col := range []int{tsCol, valueCol} {
		w.cols[col].rows += ts.Rows
		w.cols[col].raw += int64(ts.Rows) * 8
	}
	if p.rows == w.cols[tsCol].granuleSize {
		return w.flushEncodedRun()
	}
	return nil
}

func regularGranule(g EncodedGranule) (first, step int64, ok bool) {
	if !g.Ordered || len(g.Data) < 2 || g.Data[0] != 0 {
		return 0, 0, false
	}
	_, n := binary.Uvarint(g.Data[1:])
	first, m := binary.Varint(g.Data[1+n:])
	step, _ = binary.Varint(g.Data[1+n+m:])
	if g.Rows == 1 {
		step = 0
	}
	return first, step, true
}

func (p *encodedRun) canJoin(tsCol, valueCol int, ts EncodedGranule, first, step int64, capRows int) (int64, bool) {
	if p.rows == 0 || p.tsCol != tsCol || p.valueCol != valueCol || ts.Rows > capRows-p.rows {
		return 0, false
	}
	if first <= p.last {
		return 0, false
	}
	joined := p.step
	if p.rows == 1 {
		joined = first - p.first
	}
	// A subtraction across the int64 span can wrap even though both chunks are individually ordered.
	if joined <= 0 || p.last > math.MaxInt64-joined || first != p.last+joined {
		return 0, false
	}
	if ts.Rows > 1 && step != joined {
		return 0, false
	}
	return joined, true
}

func (w *StreamWriter) flushEncodedRun() error {
	p := &w.encodedRun
	if p.rows == 0 {
		return nil
	}
	p.timestamps = append(p.timestamps[:0], 0)
	p.timestamps = binary.AppendUvarint(p.timestamps, uint64(p.rows))
	p.timestamps = binary.AppendVarint(p.timestamps, p.first)
	p.timestamps = binary.AppendVarint(p.timestamps, p.step)
	p.payload = slices.Grow(p.payload[:0], 1+2*binary.MaxVarintLen64+len(p.fragments))
	p.payload = append(p.payload, 1)
	p.payload = binary.AppendUvarint(p.payload, uint64(p.rows))
	p.payload = binary.AppendUvarint(p.payload, uint64(p.groups))
	p.payload = append(p.payload, p.fragments...)
	ts, value := p.ts, p.value
	ts.Data, ts.Rows, ts.MinTime, ts.MaxTime = p.timestamps, p.rows, p.first, p.last
	value.Data, value.Rows = p.payload, p.rows
	value.floatStats, value.haveFloatStats = p.stats, true
	ts.fingerprint, value.fingerprint = ts.checksum(), value.checksum()
	ts.verified, value.verified = ts.shape(), value.shape()
	if err := w.appendEncodedGranule(p.tsCol, ts, GranuleStats{}, false); err != nil {
		return err
	}
	if err := w.appendEncodedGranule(p.valueCol, value, p.stats, false); err != nil {
		return err
	}
	p.rows, p.groups = 0, 0
	p.fragments = p.fragments[:0]
	return nil
}

func (p *encodedRun) residentBytes() int64 {
	return int64(cap(p.fragments) + cap(p.payload) + cap(p.timestamps))
}

func (w *StreamWriter) encodedRunFinishBytes() int64 {
	p := &w.encodedRun
	if p.rows == 0 {
		return 0
	}
	// The owned fragments, assembled packet, accumulator copy and compressed frame coexist on flush.
	raw := 1 + 2*binary.MaxVarintLen64 + len(p.fragments)
	v := w.cols[p.valueCol].blk
	t := w.cols[p.tsCol].blk
	return int64(4*raw+128) + v.comp.CompressTransient(raw, false) + t.comp.CompressTransient(32, false)
}
