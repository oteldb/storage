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

	Bytes int64 // sum of the part's backend object sizes
	// LogicalBytes is the part's uncompressed size, so that Bytes / LogicalBytes gives its
	// compression ratio without a second call. It is measured per signal family exactly as
	// [SignalEfficiency.LogicalBytes] describes: Rows × [engine.SampleBytes] for metrics, the
	// manifest's decoded column footprint (or the per-row estimate, for a legacy part) for records.
	LogicalBytes int64
	Chunks       int          // sparse-index granules
	Columns      []ColumnInfo // per-column physical layout
}

// ColumnInfo is one part column's physical description.
type ColumnInfo struct {
	Name     string
	Kind     string // physical type: int64 / float64 / bytes / int128
	Codec    string // value codec
	Compress string // block-compression algorithm
	// Level is the block-compression level the column was written at (0 ⇒ the algorithm default, or
	// uncompressed). Merged metric parts climb a size-graduated ladder, so it varies across parts.
	Level int
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

// StreamCostOptions selects what [Storage.StreamCosts] attributes and how much of it to estimate.
type StreamCostOptions struct {
	// GroupBy is the stream label (a resource or scope attribute name, e.g. "service.name") whose
	// value keys the report. Empty groups by raw stream id.
	GroupBy string
	// Columns restricts the byte columns that are decoded and attributed (nil ⇒ every byte column).
	Columns []string
	// TopN keeps only the N costliest groups by RawBytes (≤0 ⇒ every group).
	TopN int
	// MaxSketchGroups bounds how many groups carry distinct estimates (≤0 ⇒ the engine default of
	// 4096). The budget goes to the groups with the most rows.
	MaxSketchGroups int
}

// StreamCost is one group's (one label value's, or one stream's) share of a record signal's storage:
// the per-service cost attribution that [Storage.Cardinality] (label cardinality) and
// [Storage.PartsDetailed] (per-part layout) cannot give. See [Storage.StreamCosts].
type StreamCost struct {
	Key     string // the GroupBy label's value, or the stream id; empty ⇒ the label is absent
	Streams int    // distinct streams folded into this group
	Rows    int64
	// RawBytes is the decoded footprint of the group's rows over the accounted columns.
	RawBytes int64
	// DiskBytes is APPROXIMATE: compression is per column per frame and a frame spans whatever
	// streams its rows fall in, so each frame's compressed size is apportioned across the groups
	// holding its rows by their raw-byte share.
	DiskBytes int64
	// DistinctEstimated reports whether the per-column distinct counts were computed for this group
	// (see [StreamCostOptions.MaxSketchGroups]). False ⇒ they are 0 because they were not measured.
	DistinctEstimated bool
	Columns           []ColumnCost
}

// ColumnCost is one column's share of a [StreamCost].
type ColumnCost struct {
	Name      string
	RawBytes  int64
	DiskBytes int64 // approximate, as [StreamCost.DiskBytes]
	// Distinct estimates the distinct values the group's rows hold in this column (HyperLogLog,
	// ~1.6% standard error). 0 for an int column and for a group outside the sketch budget.
	Distinct int64
	// DistinctNormalized is Distinct over values with every run of ASCII digits collapsed to '#'.
	// A group whose Distinct is large and whose DistinctNormalized is tiny is mis-parsed at the
	// source — one templated line with an embedded timestamp or id, never turned into fields — which
	// is a fixable diagnosis rather than a number to stare at.
	DistinctNormalized int64
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

// StreamCosts attributes a record signal's flushed parts to streams — or, with
// [StreamCostOptions.GroupBy], to a label's values: rows, decoded bytes, an approximate compressed
// share, and per-column distinct estimates. It is the "which service is costing me, and why"
// drill-down.
//
// It is the heaviest introspection call in the library: every accounted byte column of every live
// part is read and decoded once. Run it on operator demand, not on a schedule, and narrow it with
// [StreamCostOptions.Columns] when only one column is in question. It returns nil (no error) when
// the tenant has no engine for the signal, and an error for [signal.Metric], whose samples carry no
// per-record columns to attribute (use [Storage.Cardinality] there).
func (s *Storage) StreamCosts(
	ctx context.Context, tenant signal.TenantID, sig signal.Signal, opts StreamCostOptions,
) ([]StreamCost, error) {
	if s.closed.Load() {
		return nil, errors.Wrap(ErrClosed, "stream costs")
	}

	if sig == signal.Metric {
		return nil, errors.New("stream costs: metrics have no per-record columns to attribute")
	}

	eng, ok := s.lookupRecordEngine(sig, s.normalizeTenant(tenant))
	if !ok {
		return nil, nil
	}

	cs, err := eng.StreamCost(ctx, recordengine.StreamCostOptions{
		GroupBy: opts.GroupBy, Columns: opts.Columns, TopN: opts.TopN, MaxSketchGroups: opts.MaxSketchGroups,
	})
	if err != nil {
		return nil, errors.Wrapf(err, "%s stream costs", sig)
	}

	return recordStreamCosts(cs), nil
}

func recordStreamCosts(cs []recordengine.StreamCostStat) []StreamCost {
	out := make([]StreamCost, len(cs))

	for i, c := range cs {
		cols := make([]ColumnCost, len(c.Columns))
		for j, cc := range c.Columns {
			cols[j] = ColumnCost{
				Name: cc.Name, RawBytes: cc.RawBytes, DiskBytes: cc.DiskBytes,
				Distinct: cc.Distinct, DistinctNormalized: cc.DistinctNormalized,
			}
		}

		out[i] = StreamCost{
			Key: c.Key, Streams: c.Streams, Rows: c.Rows, RawBytes: c.RawBytes, DiskBytes: c.DiskBytes,
			DistinctEstimated: c.DistinctEstimated, Columns: cols,
		}
	}

	return out
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
			Bytes:    d.Bytes, LogicalBytes: d.Rows * engine.SampleBytes,
			Chunks: d.Chunks, Columns: metricColumns(d.Columns),
		}
	}

	return out
}

func recordPartDetails(ds []recordengine.PartDetailStat) []PartDetail {
	out := make([]PartDetail, len(ds))
	for i, d := range ds {
		out[i] = PartDetail{
			PartInfo: PartInfo{ID: d.ID, MinTime: d.MinTime, MaxTime: d.MaxTime, Series: d.Series, Rows: d.Rows},
			Bytes:    d.Bytes, LogicalBytes: d.LogicalBytes(),
			Chunks: d.Chunks, Columns: recordColumns(d.Columns),
		}
	}

	return out
}

func metricColumns(cs []engine.ColumnStat) []ColumnInfo {
	out := make([]ColumnInfo, len(cs))
	for i, c := range cs {
		out[i] = ColumnInfo{Name: c.Name, Kind: c.Kind, Codec: c.Codec, Compress: c.Compress, Level: c.Level}
	}

	return out
}

func recordColumns(cs []recordengine.ColumnStat) []ColumnInfo {
	out := make([]ColumnInfo, len(cs))
	for i, c := range cs {
		out[i] = ColumnInfo{Name: c.Name, Kind: c.Kind, Codec: c.Codec, Compress: c.Compress, Level: c.Level}
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
