package block

import (
	"hash/crc32"
	"math"

	"github.com/go-faster/errors"

	"github.com/oteldb/storage/encoding/chunk"
)

// EncodedGranule borrows a validated numeric stream until its decoder's next operation.
// Data is read-only; metadata must remain unchanged until appended.
type EncodedGranule struct {
	Data             []byte
	Kind             Kind
	Codec            chunk.Codec
	Rows             int
	MinTime, MaxTime int64
	Ordered          bool

	floatStats     GranuleStats
	haveFloatStats bool
	checked        bool
	fingerprint    uint32
	verified       granuleShape
}

// GranuleStats supplies trusted bounds for encoded float values.
type GranuleStats struct {
	MinFloat64, MaxFloat64 float64
}

type granuleShape struct {
	kind             Kind
	codec            chunk.Codec
	rows             int
	minTime, maxTime int64
	ordered          bool
}

// EncodedGranule reads one checked independent granule without materializing its samples.
func (d *Decoder) EncodedGranule(blk int) (EncodedGranule, error) {
	return d.VisitEncodedGranule(blk, nil)
}

// VisitEncodedGranule validates a borrowed stream and visits its float values in row order.
// A callback may observe a prefix before malformed input is rejected.
func (d *Decoder) VisitEncodedGranule(blk int, visit func(float64)) (EncodedGranule, error) {
	if d.streams.dir.src != nil && d.streams.dir.src.closed {
		return EncodedGranule{}, errors.New("block: encoded granule decoder is closed")
	}
	if blk < 0 || blk >= d.NumBlocks() {
		return EncodedGranule{}, errors.New("block: encoded granule out of range")
	}
	if d.streams.dir.rowStarts == nil || d.streams.dir.frameCRC == nil ||
		(d.kind != KindInt64 || d.codec != chunk.CodecAdaptiveTimestamps) &&
			(d.kind != KindFloat64 || d.codec != chunk.CodecFloatGroups) {
		return EncodedGranule{}, errors.New("block: unsupported encoded granule")
	}
	data, err := d.streams.granule(blk)
	if err != nil {
		return EncodedGranule{}, err
	}
	lo, hi := d.BlockSpan(blk)
	g := EncodedGranule{Data: data, Kind: d.kind, Codec: d.codec, Rows: hi - lo}
	if d.kind == KindInt64 {
		g.MinTime, g.MaxTime, g.Ordered, err = chunk.ValidateTimestampsAdaptiveRows(data, g.Rows)

		if err == nil {
			err = d.checkEncodedTimeBounds(blk, g)
		}
	} else {
		if visit == nil {
			err = chunk.ValidateFloatGroupsRows(data, g.Rows)
		} else {
			first := true
			err = chunk.VisitFloatGroupsRows(data, g.Rows, func(v float64) {
				g.floatStats.add(v, first)
				first = false
				visit(v)
			})
			g.haveFloatStats = err == nil
		}
	}
	if err != nil {
		return EncodedGranule{}, errors.Wrap(err, "encoded granule")
	}
	g.checked, g.fingerprint, g.verified = true, g.checksum(), g.shape()
	return g, nil
}

func (d *Decoder) checkEncodedTimeBounds(blk int, g EncodedGranule) error {
	if minT, maxT, ok := d.BlockTimeBounds(blk); ok && (minT != g.MinTime || maxT != g.MaxTime) {
		return errors.Wrap(ErrCorrupt, "encoded granule time bounds")
	}
	return nil
}

// FloatStats returns the validated granule's float bounds, ignoring NaN values.
func (g EncodedGranule) FloatStats() GranuleStats {
	if g.haveFloatStats {
		return g.floatStats
	}
	stats := GranuleStats{MinFloat64: math.NaN(), MaxFloat64: math.NaN()}
	if g.Kind != KindFloat64 || !g.validBorrow() {
		return stats
	}
	first := true
	_ = chunk.VisitFloatGroupsRows(g.Data, g.Rows, func(v float64) {
		stats.add(v, first)
		first = false
	})
	return stats
}

func (g EncodedGranule) shape() granuleShape {
	return granuleShape{g.Kind, g.Codec, g.Rows, g.MinTime, g.MaxTime, g.Ordered}
}

func (g EncodedGranule) checksum() uint32 {
	return crc32.Checksum(g.Data, castagnoli)
}

func (g EncodedGranule) validBorrow() bool {
	return g.checked && g.verified == g.shape() && g.fingerprint == g.checksum()
}

func (s *GranuleStats) add(v float64, first bool) {
	if first {
		s.MinFloat64, s.MaxFloat64 = v, v
		return
	}
	if math.IsNaN(v) {
		return
	}
	if math.IsNaN(s.MinFloat64) || v < s.MinFloat64 {
		s.MinFloat64 = v
	}
	if math.IsNaN(s.MaxFloat64) || v > s.MaxFloat64 {
		s.MaxFloat64 = v
	}
}

// AppendEncodedGranule copies a borrowed granule and recompresses its outer frame.
// End the current group before switching between sample and encoded appends.
func (w *StreamWriter) AppendEncodedGranule(col int, g EncodedGranule, stats GranuleStats) error {
	if err := w.flushEncodedRun(); err != nil {
		return err
	}
	return w.appendEncodedGranule(col, g, stats, true)
}

func (w *StreamWriter) checkEncodedGranule(col int, g EncodedGranule) error {
	if col < 0 || col >= len(w.cols) {
		return errors.New("block: encoded granule column out of range")
	}
	c := w.cols[col]
	if !w.independentGroups || !c.variable || c.kind != g.Kind || c.codec != g.Codec ||
		c.autoCodec || c.alt != nil || g.Rows <= 0 || g.Rows > c.granuleSize ||
		len(c.stageI64) != 0 || len(c.stageF64) != 0 {
		return errors.New("block: incompatible encoded granule")
	}
	if !g.validBorrow() {
		return errors.Wrap(ErrCorrupt, "changed encoded granule")
	}
	return nil
}

func (w *StreamWriter) appendEncodedGranule(col int, g EncodedGranule, stats GranuleStats, accountRows bool) error {
	if err := w.checkEncodedGranule(col, g); err != nil {
		return err
	}
	c := w.cols[col]
	switch c.kind {
	case KindInt64:
		if g.Codec != chunk.CodecAdaptiveTimestamps {
			return errors.New("block: unsupported encoded timestamps")
		}
		if !c.haveStats {
			c.minI64, c.maxI64 = g.MinTime, g.MaxTime
		} else {
			c.minI64, c.maxI64 = min(c.minI64, g.MinTime), max(c.maxI64, g.MaxTime)
		}
		if c.timeBounds {
			c.blk.minKeys = append(c.blk.minKeys, g.MinTime)
			c.blk.maxKeys = append(c.blk.maxKeys, g.MaxTime)
		}
	case KindFloat64:
		if g.Codec != chunk.CodecFloatGroups {
			return errors.New("block: unsupported encoded floats")
		}
		if math.IsNaN(stats.MinFloat64) != math.IsNaN(stats.MaxFloat64) || stats.MinFloat64 > stats.MaxFloat64 {
			return errors.New("block: invalid encoded float bounds")
		}
		c.trackEncodedFloatStats(stats)
	default:
		return errors.New("block: unsupported encoded kind")
	}
	c.haveStats = true
	if accountRows {
		c.rows += g.Rows
		c.raw += int64(g.Rows) * 8
	}
	c.encoded += g.Rows
	if err := c.blk.addGranule(func(dst []byte) ([]byte, error) {
		return append(dst, g.Data...), nil
	}); err != nil {
		return err
	}
	if err := c.sealChunk(g.Rows); err != nil {
		return err
	}
	return c.maybeAttach()
}

func (c *streamColumn) trackEncodedFloatStats(stats GranuleStats) {
	if !c.haveStats {
		c.minF64, c.maxF64 = stats.MinFloat64, stats.MaxFloat64
		c.firstBits = math.Float64bits(stats.MinFloat64)
	} else {
		if !math.IsNaN(stats.MinFloat64) && (math.IsNaN(c.minF64) || stats.MinFloat64 < c.minF64) {
			c.minF64 = stats.MinFloat64
		}
		if !math.IsNaN(stats.MaxFloat64) && (math.IsNaN(c.maxF64) || stats.MaxFloat64 > c.maxF64) {
			c.maxF64 = stats.MaxFloat64
		}
	}
	// Bounds cannot distinguish signed zero or prove identical NaN payloads.
	c.allSame = false
	c.sawNonFinite = c.sawNonFinite || math.IsNaN(stats.MinFloat64) || math.IsNaN(stats.MaxFloat64) ||
		math.IsInf(stats.MinFloat64, 0) || math.IsInf(stats.MaxFloat64, 0)
}
