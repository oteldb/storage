package engine

import (
	"encoding/binary"
	"hash/crc32"
	"math"

	"github.com/go-faster/errors"

	"github.com/oteldb/storage/encoding/chunk"
	"github.com/oteldb/storage/signal"
)

// SeriesAgg is a per-series aggregate over a value window — enough to answer count, sum, min, max,
// and avg (Sum/Count) without the raw samples. It is the unit of the aggregate-pushdown fast path:
// a part precomputes one per series at write time (the stats sidecar) so a query whose range fully
// covers the part folds these instead of decoding its value column.
type SeriesAgg struct {
	Count    int64
	Sum      float64
	Min, Max float64
}

// addSample folds one value into the aggregate.
func (a *SeriesAgg) addSample(v float64) {
	if a.Count == 0 {
		a.Min, a.Max = v, v
	} else {
		if v < a.Min {
			a.Min = v
		}
		if v > a.Max {
			a.Max = v
		}
	}

	a.Sum += v
	a.Count++
}

// merge folds another aggregate in (the two must cover disjoint samples, or counts/sums double).
func (a *SeriesAgg) merge(b SeriesAgg) {
	if b.Count == 0 {
		return
	}

	if a.Count == 0 {
		*a = b

		return
	}

	a.Sum += b.Sum
	a.Count += b.Count

	if b.Min < a.Min {
		a.Min = b.Min
	}

	if b.Max > a.Max {
		a.Max = b.Max
	}
}

// computeSeriesStats folds the (series, ts)-sorted flush columns into one [SeriesAgg] per series, in
// the order series first appear — which, given the sort, is each series' contiguous run. The values
// are the raw (unweighted) column, so the sidecar is written only for an unsampled part (no sf
// column); a sampled part falls back to the weighted decode path.
func computeSeriesStats(cols *flushColumns) ([]chunk.U128, []SeriesAgg) {
	var (
		ids   []chunk.U128
		stats []SeriesAgg
	)

	for i := range cols.series {
		if i == 0 || cols.series[i] != cols.series[i-1] {
			ids = append(ids, cols.series[i])
			stats = append(stats, SeriesAgg{})
		}

		stats[len(stats)-1].addSample(cols.value[i])
	}

	return ids, stats
}

const statsMagic uint32 = 0x4F545341 // "OTSA"

var statsCRC = crc32.MakeTable(crc32.Castagnoli)

// errStatsCorrupt marks an unreadable/absent stats sidecar; the caller falls back to decoding.
var errStatsCorrupt = errors.New("engine: corrupt series-stats sidecar")

// encodeSeriesStats serializes the per-series aggregates: [magic][uvarint n] then per series
// [u128 id][varint count][f64 sum][f64 min][f64 max], with a trailing CRC32C.
func encodeSeriesStats(ids []chunk.U128, stats []SeriesAgg) []byte {
	buf := make([]byte, 0, 8+len(ids)*40)
	buf = binary.BigEndian.AppendUint32(buf, statsMagic)
	buf = binary.AppendUvarint(buf, uint64(len(ids)))

	for i := range ids {
		buf = binary.BigEndian.AppendUint64(buf, ids[i].Hi)
		buf = binary.BigEndian.AppendUint64(buf, ids[i].Lo)
		buf = binary.AppendVarint(buf, stats[i].Count)
		buf = binary.BigEndian.AppendUint64(buf, math.Float64bits(stats[i].Sum))
		buf = binary.BigEndian.AppendUint64(buf, math.Float64bits(stats[i].Min))
		buf = binary.BigEndian.AppendUint64(buf, math.Float64bits(stats[i].Max))
	}

	return binary.BigEndian.AppendUint32(buf, crc32.Checksum(buf, statsCRC))
}

// decodeSeriesStats parses a stats sidecar into a per-series map. It bounds-checks every field and
// never panics, returning [errStatsCorrupt] on any malformed input so a reader can fall back to
// decoding the value column.
func decodeSeriesStats(data []byte) (map[signal.SeriesID]SeriesAgg, error) {
	if len(data) < 8 {
		return nil, errStatsCorrupt
	}

	body := data[:len(data)-4]
	if crc32.Checksum(body, statsCRC) != binary.BigEndian.Uint32(data[len(data)-4:]) {
		return nil, errors.Wrap(errStatsCorrupt, "crc")
	}

	if binary.BigEndian.Uint32(body) != statsMagic {
		return nil, errors.Wrap(errStatsCorrupt, "magic")
	}

	rest := body[4:]
	n, m := binary.Uvarint(rest)
	if m <= 0 {
		return nil, errors.Wrap(errStatsCorrupt, "count")
	}
	rest = rest[m:]

	out := make(map[signal.SeriesID]SeriesAgg, n)
	for range n {
		if len(rest) < 16 {
			return nil, errors.Wrap(errStatsCorrupt, "id")
		}
		id := signal.SeriesID{Hi: binary.BigEndian.Uint64(rest[:8]), Lo: binary.BigEndian.Uint64(rest[8:16])}
		rest = rest[16:]

		count, mm := binary.Varint(rest)
		if mm <= 0 {
			return nil, errors.Wrap(errStatsCorrupt, "series count")
		}
		rest = rest[mm:]

		if len(rest) < 24 {
			return nil, errors.Wrap(errStatsCorrupt, "sum/min/max")
		}
		out[id] = SeriesAgg{
			Count: count,
			Sum:   math.Float64frombits(binary.BigEndian.Uint64(rest[:8])),
			Min:   math.Float64frombits(binary.BigEndian.Uint64(rest[8:16])),
			Max:   math.Float64frombits(binary.BigEndian.Uint64(rest[16:24])),
		}
		rest = rest[24:]
	}

	return out, nil
}
