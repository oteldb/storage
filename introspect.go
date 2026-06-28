package storage

import (
	"context"

	"github.com/go-faster/errors"

	"github.com/oteldb/storage/engine"
	"github.com/oteldb/storage/recordengine"
	"github.com/oteldb/storage/signal"
)

// PartInfo is one flushed part's in-memory shape for a drill-down dashboard: identity, time bounds,
// and the series/row counts. It does no backend I/O — see [Storage.Parts]. For byte sizes, codecs,
// and chunk counts use [Storage.PartsDetailed].
type PartInfo struct {
	ID      string // the part's backend key prefix
	MinTime int64  // inclusive unix-ns bounds of the part's data
	MaxTime int64
	Series  int   // distinct series/streams in the part
	Rows    int64 // total samples/records in the part
}

// PartDetail augments [PartInfo] with fields that require a backend read: the part's on-backend byte
// size and its column/codec layout and chunk (granule) count.
type PartDetail struct {
	PartInfo

	Bytes   int64        // sum of the part's backend object sizes
	Chunks  int          // sparse-index granules
	Columns []ColumnInfo // per-column physical layout
}

// ColumnInfo is one part column's physical description.
type ColumnInfo struct {
	Name     string
	Kind     string // physical type: int64 / float64 / bytes / int128
	Codec    string // value codec
	Compress string // block-compression algorithm
}

// CardinalityStats summarizes a (tenant, signal) engine's label cardinality — the operator's first
// stop for a cardinality-explosion incident. It is computed from the in-memory inverted index (which
// spans head ∪ flushed series), so it does no backend I/O and is safe to poll. See [Storage.Cardinality].
type CardinalityStats struct {
	// TotalSeries is the distinct series/streams the engine indexes (head ∪ flushed).
	TotalSeries int64
	// DistinctLabelNames is the number of distinct indexed label names.
	DistinctLabelNames int
	// SymbolCount is the size of the engine's interned-symbol table (names + values).
	SymbolCount int
	// TopLabelNames is the highest-cardinality label names, sorted by series count descending then
	// name; bounded by the topN argument.
	TopLabelNames []LabelCardinality
}

// LabelCardinality is one label name's cardinality: how many series carry it and how many distinct
// values it takes across them.
type LabelCardinality struct {
	Name           string
	Series         int64
	DistinctValues int
}

// Parts returns an in-memory snapshot of a (tenant, signal) engine's flushed parts. It does no
// backend I/O and decodes nothing — safe to poll at dashboard cadence — and returns nil when the
// tenant has no engine for the signal. For byte sizes, codecs, and chunk counts, use
// [Storage.PartsDetailed].
func (s *Storage) Parts(tenant signal.TenantID, sig signal.Signal) []PartInfo {
	key := s.normalizeTenant(tenant)

	if sig == signal.Metric {
		eng, ok := s.lookupEngine(key)
		if !ok {
			return nil
		}

		return metricPartInfos(eng.Parts())
	}

	eng, ok := s.lookupRecordEngine(sig, key)
	if !ok {
		return nil
	}

	return recordPartInfos(eng.Parts())
}

// PartsDetailed augments [Storage.Parts] with each part's on-backend byte size, column/codec layout,
// and chunk count. It reads object sizes from the backend, so unlike Parts it is not hot-path-free —
// call it for a drill-down view, not a high-frequency poll. It returns nil (no error) when the
// tenant has no engine for the signal.
func (s *Storage) PartsDetailed(ctx context.Context, tenant signal.TenantID, sig signal.Signal) ([]PartDetail, error) {
	if s.closed.Load() {
		return nil, errors.Wrap(ErrClosed, "parts detailed")
	}

	key := s.normalizeTenant(tenant)

	if sig == signal.Metric {
		eng, ok := s.lookupEngine(key)
		if !ok {
			return nil, nil
		}

		ds, err := eng.PartsDetailed(ctx)
		if err != nil {
			return nil, errors.Wrap(err, "metric parts detailed")
		}

		return metricPartDetails(ds), nil
	}

	eng, ok := s.lookupRecordEngine(sig, key)
	if !ok {
		return nil, nil
	}

	ds, err := eng.PartsDetailed(ctx)
	if err != nil {
		return nil, errors.Wrapf(err, "%s parts detailed", sig)
	}

	return recordPartDetails(ds), nil
}

// Cardinality summarizes a (tenant, signal) engine's label cardinality. topN bounds the returned
// TopLabelNames slice (≤0 returns every label name). It does no backend I/O and returns a zero value
// when the tenant has no engine for the signal.
func (s *Storage) Cardinality(tenant signal.TenantID, sig signal.Signal, topN int) CardinalityStats {
	key := s.normalizeTenant(tenant)

	if sig == signal.Metric {
		eng, ok := s.lookupEngine(key)
		if !ok {
			return CardinalityStats{}
		}

		return metricCardinality(eng.Cardinality(topN))
	}

	eng, ok := s.lookupRecordEngine(sig, key)
	if !ok {
		return CardinalityStats{}
	}

	return recordCardinality(eng.Cardinality(topN))
}

// metricPartInfos / recordPartInfos map the per-engine part shapes to the public [PartInfo]. The two
// engine packages carry structurally identical but distinct types, so each signal family maps once.
func metricPartInfos(ps []engine.PartStat) []PartInfo {
	out := make([]PartInfo, len(ps))
	for i, p := range ps {
		out[i] = PartInfo{ID: p.ID, MinTime: p.MinTime, MaxTime: p.MaxTime, Series: p.Series, Rows: p.Rows}
	}

	return out
}

func recordPartInfos(ps []recordengine.PartStat) []PartInfo {
	out := make([]PartInfo, len(ps))
	for i, p := range ps {
		out[i] = PartInfo{ID: p.ID, MinTime: p.MinTime, MaxTime: p.MaxTime, Series: p.Series, Rows: p.Rows}
	}

	return out
}

func metricPartDetails(ds []engine.PartDetailStat) []PartDetail {
	out := make([]PartDetail, len(ds))
	for i, d := range ds {
		out[i] = PartDetail{
			PartInfo: PartInfo{ID: d.ID, MinTime: d.MinTime, MaxTime: d.MaxTime, Series: d.Series, Rows: d.Rows},
			Bytes:    d.Bytes, Chunks: d.Chunks, Columns: metricColumns(d.Columns),
		}
	}

	return out
}

func recordPartDetails(ds []recordengine.PartDetailStat) []PartDetail {
	out := make([]PartDetail, len(ds))
	for i, d := range ds {
		out[i] = PartDetail{
			PartInfo: PartInfo{ID: d.ID, MinTime: d.MinTime, MaxTime: d.MaxTime, Series: d.Series, Rows: d.Rows},
			Bytes:    d.Bytes, Chunks: d.Chunks, Columns: recordColumns(d.Columns),
		}
	}

	return out
}

func metricColumns(cs []engine.ColumnStat) []ColumnInfo {
	out := make([]ColumnInfo, len(cs))
	for i, c := range cs {
		out[i] = ColumnInfo{Name: c.Name, Kind: c.Kind, Codec: c.Codec, Compress: c.Compress}
	}

	return out
}

func recordColumns(cs []recordengine.ColumnStat) []ColumnInfo {
	out := make([]ColumnInfo, len(cs))
	for i, c := range cs {
		out[i] = ColumnInfo{Name: c.Name, Kind: c.Kind, Codec: c.Codec, Compress: c.Compress}
	}

	return out
}

func metricCardinality(c engine.CardinalityStat) CardinalityStats {
	out := CardinalityStats{
		TotalSeries: c.TotalSeries, DistinctLabelNames: c.DistinctLabelNames, SymbolCount: c.SymbolCount,
		TopLabelNames: make([]LabelCardinality, len(c.Top)),
	}
	for i, l := range c.Top {
		out.TopLabelNames[i] = LabelCardinality{Name: l.Name, Series: l.Series, DistinctValues: l.DistinctValues}
	}

	return out
}

func recordCardinality(c recordengine.CardinalityStat) CardinalityStats {
	out := CardinalityStats{
		TotalSeries: c.TotalSeries, DistinctLabelNames: c.DistinctLabelNames, SymbolCount: c.SymbolCount,
		TopLabelNames: make([]LabelCardinality, len(c.Top)),
	}
	for i, l := range c.Top {
		out.TopLabelNames[i] = LabelCardinality{Name: l.Name, Series: l.Series, DistinctValues: l.DistinctValues}
	}

	return out
}
