package engine

import (
	"context"
	"sort"

	"github.com/oteldb/storage/backend"
	"github.com/oteldb/storage/index/symbols"
)

// PartStat is one flushed part's in-memory shape (no backend I/O, no decode): identity, time
// bounds, and the series/row counts derivable from the part's in-memory row-range index.
type PartStat struct {
	ID      string // the part's backend key prefix
	MinTime int64  // inclusive unix-ns bounds of the part's samples
	MaxTime int64
	Series  int   // distinct series in the part (len of its row-range index)
	Rows    int64 // total samples (sum of the per-series row spans)
}

// PartDetailStat augments [PartStat] with fields that need a backend read: the on-backend byte size
// (summed over the part's objects) and the column/codec layout and chunk count from the manifest
// (the manifest is already cached on the open part, so only Bytes incurs additional I/O).
type PartDetailStat struct {
	PartStat

	Bytes   int64        // sum of the part's backend object sizes
	Chunks  int          // sparse-index granules: ceil(RowCount / GranuleSize)
	Columns []ColumnStat // per-column physical layout
}

// ColumnStat is one part column's physical description (from the manifest).
type ColumnStat struct {
	Name     string
	Kind     string // physical type: int64 / float64 / bytes / int128
	Codec    string // value codec
	Compress string // block-compression algorithm
}

// CardinalityStat summarizes the engine's label cardinality (the head's index spans head ∪ flushed
// series). TotalSeries and SymbolCount are exact; Top is the highest-cardinality label names.
type CardinalityStat struct {
	TotalSeries        int64
	DistinctLabelNames int
	SymbolCount        int
	Top                []LabelCard // sorted by Series desc, then Name; truncated to the requested top-N
}

// LabelCard is one label name's cardinality: how many series carry it and how many distinct values
// it takes across them.
type LabelCard struct {
	Name           string
	Series         int64
	DistinctValues int
}

// Parts returns an in-memory snapshot of the engine's flushed parts under a read lock — no backend
// I/O and no decode, safe to poll. For byte sizes, codecs, and chunk counts, use [Engine.PartsDetailed].
func (e *Engine) Parts() []PartStat {
	e.mu.RLock()
	defer e.mu.RUnlock()

	out := make([]PartStat, 0, len(e.parts))
	for _, p := range e.parts {
		out = append(out, PartStat{
			ID: p.prefix, MinTime: p.minTime, MaxTime: p.maxTime,
			Series: len(p.ranges), Rows: int64(p.rows()),
		})
	}

	return out
}

// PartsDetailed augments [Engine.Parts] with each part's on-backend byte size, column/codec layout,
// and chunk (granule) count. It reads from the backend (object sizes), so unlike Parts it is not
// hot-path-free — call it for a drill-down view, not a high-frequency poll. Each part is ref-held
// for the duration so a concurrent merge cannot reclaim its objects mid-read.
func (e *Engine) PartsDetailed(ctx context.Context) ([]PartDetailStat, error) {
	e.mu.RLock()
	parts := make([]*part, len(e.parts))
	copy(parts, e.parts)

	for _, p := range parts {
		p.acquire()
	}

	e.mu.RUnlock()

	defer func() {
		for _, p := range parts {
			p.release()
		}
	}()

	out := make([]PartDetailStat, 0, len(parts))
	for _, p := range parts {
		man := p.reader.Manifest()

		cols := make([]ColumnStat, 0, len(man.Columns))
		for _, c := range man.Columns {
			cols = append(cols, ColumnStat{
				Name: c.Name, Kind: c.Kind.String(), Codec: c.Codec.String(), Compress: c.Compress.String(),
			})
		}

		bytes, err := partBytes(ctx, p.be, p.prefix)
		if err != nil {
			return nil, err
		}

		out = append(out, PartDetailStat{
			PartStat: PartStat{
				ID: p.prefix, MinTime: p.minTime, MaxTime: p.maxTime,
				Series: len(p.ranges), Rows: int64(p.rows()),
			},
			Bytes:   bytes,
			Chunks:  granuleCount(man.RowCount, man.GranuleSize),
			Columns: cols,
		})
	}

	return out, nil
}

// partBytes sums the backend object sizes of the part at prefix (manifest, marks, and column
// objects), using the [backend.Sizer] fast path when available.
func partBytes(ctx context.Context, b backend.Backend, prefix string) (int64, error) {
	if b == nil {
		return 0, nil
	}

	keys, err := b.List(ctx, prefix)
	if err != nil {
		return 0, err
	}

	var total int64
	for _, k := range keys {
		n, err := backend.SizeOf(ctx, b, k)
		if err != nil {
			return 0, err
		}

		total += n
	}

	return total, nil
}

// granuleCount returns the number of sparse-index granules for a part of rowCount rows at
// granuleSize rows each (ceiling division), guarding a zero/absent granule size.
func granuleCount(rowCount, granuleSize int) int {
	if rowCount <= 0 {
		return 0
	}

	if granuleSize <= 0 {
		return 1
	}

	return (rowCount + granuleSize - 1) / granuleSize
}

// MergeRunning reports whether a merge/compaction is currently executing on this engine (an
// in-memory liveness flag for introspection).
func (e *Engine) MergeRunning() bool { return e.mergeRunning.Load() }

// MergeBacklog returns the number of flushed parts — the compaction-backlog proxy (many small parts
// means merge is behind). It takes a brief read lock.
func (e *Engine) MergeBacklog() int {
	e.mu.RLock()
	defer e.mu.RUnlock()

	return len(e.parts)
}

// WALState returns the current WAL segment count, the open segment's byte size, and the active flush
// epoch. ok is false when the engine has no WAL (the ephemeral in-memory engine). It takes a read
// lock, excluding concurrent appends (which hold the write lock).
func (e *Engine) WALState() (segments int, bytes int64, epoch uint64, ok bool) {
	e.mu.RLock()
	defer e.mu.RUnlock()

	if e.cfg.WAL == nil {
		return 0, 0, 0, false
	}

	return e.cfg.WAL.Seq(), int64(e.cfg.WAL.Size()), e.cfg.WAL.Epoch(), true
}

// Cardinality summarizes the engine's label cardinality from the head's inverted index (which spans
// every series ever seen, flushed or not). topN bounds the returned Top slice (≤0 returns all label
// names). It takes a read lock and does no backend I/O.
func (e *Engine) Cardinality(topN int) CardinalityStat {
	e.mu.RLock()
	defer e.mu.RUnlock()

	cs := CardinalityStat{
		TotalSeries: int64(e.head.series.Len()),
		SymbolCount: e.head.sym.Len(),
	}

	e.head.post.ForEachName(func(nameID uint32, distinctValues, totalSeries int) {
		name := ""
		if b, ok := e.head.sym.Get(symbols.ID(nameID)); ok {
			name = string(b)
		}

		cs.Top = append(cs.Top, LabelCard{Name: name, Series: int64(totalSeries), DistinctValues: distinctValues})
	})

	cs.DistinctLabelNames = len(cs.Top)

	sort.Slice(cs.Top, func(i, j int) bool {
		if cs.Top[i].Series != cs.Top[j].Series {
			return cs.Top[i].Series > cs.Top[j].Series
		}

		return cs.Top[i].Name < cs.Top[j].Name
	})

	if topN > 0 && len(cs.Top) > topN {
		cs.Top = cs.Top[:topN]
	}

	return cs
}
