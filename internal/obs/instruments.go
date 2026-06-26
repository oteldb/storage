package obs

import (
	"context"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
)

// imb is a small instrument builder that defers the first error, so a group of related instruments
// is declared without error-checking each line. obs.New surfaces the error (instrument creation only
// fails on an invalid name, which the library controls; the noop meter never fails).
type imb struct {
	m   metric.Meter
	err error
}

func (b *imb) counter(name, desc, unit string) metric.Int64Counter {
	if b.err != nil {
		return nil
	}

	c, err := b.m.Int64Counter(name, metric.WithDescription(desc), metric.WithUnit(unit))
	b.err = err

	return c
}

func (b *imb) i64hist(name, desc, unit string) metric.Int64Histogram {
	if b.err != nil {
		return nil
	}

	h, err := b.m.Int64Histogram(name, metric.WithDescription(desc), metric.WithUnit(unit))
	b.err = err

	return h
}

func (b *imb) f64hist(name, desc, unit string) metric.Float64Histogram {
	if b.err != nil {
		return nil
	}

	h, err := b.m.Float64Histogram(name, metric.WithDescription(desc), metric.WithUnit(unit))
	b.err = err

	return h
}

func sigAttr(sig string) metric.MeasurementOption {
	return metric.WithAttributes(attribute.String("signal", sig))
}

// Flush instruments a head→part flush.
type Flush struct {
	total    metric.Int64Counter
	duration metric.Float64Histogram
	rows     metric.Int64Histogram
}

// Record accounts one flush: the row count written and the wall time dur, for the given signal.
func (f *Flush) Record(ctx context.Context, sig string, dur time.Duration, rows int64) {
	a := sigAttr(sig)
	f.total.Add(ctx, 1, a)
	f.duration.Record(ctx, dur.Seconds(), a)

	if rows > 0 {
		f.rows.Record(ctx, rows, a)
	}
}

// Merge instruments a background merge (compaction/retention/downsample/recompress).
type Merge struct {
	total    metric.Int64Counter
	duration metric.Float64Histogram
	parts    metric.Int64Histogram
}

// Record accounts one merge that compacted partsIn source parts in dur, for the given signal.
func (m *Merge) Record(ctx context.Context, sig string, dur time.Duration, partsIn int64) {
	a := sigAttr(sig)
	m.total.Add(ctx, 1, a)
	m.duration.Record(ctx, dur.Seconds(), a)

	if partsIn > 0 {
		m.parts.Record(ctx, partsIn, a)
	}
}

// Fetch instruments a fetch over the head ∪ parts.
type Fetch struct {
	total        metric.Int64Counter
	duration     metric.Float64Histogram
	series       metric.Int64Histogram
	rows         metric.Int64Histogram
	partsScanned metric.Int64Counter
}

// Record accounts one fetch (matched `series` series, scanned `partsScanned` parts, returned
// `rows` rows, took dur) for the given signal.
func (f *Fetch) Record(ctx context.Context, sig string, dur time.Duration, series, partsScanned, rows int64) {
	a := sigAttr(sig)
	f.total.Add(ctx, 1, a)
	f.duration.Record(ctx, dur.Seconds(), a)
	f.series.Record(ctx, series, a)
	f.rows.Record(ctx, rows, a)

	if partsScanned > 0 {
		f.partsScanned.Add(ctx, partsScanned, a)
	}
}

func newEngineInstruments(m metric.Meter) (*Flush, *Merge, *Fetch, error) {
	b := &imb{m: m}

	flush := &Flush{
		total:    b.counter("storage.flush.total", "head flushes to a part", "{flush}"),
		duration: b.f64hist("storage.flush.duration", "flush wall time", "s"),
		rows:     b.i64hist("storage.flush.rows", "rows written per flush", "{row}"),
	}
	merge := &Merge{
		total:    b.counter("storage.merge.total", "background merges", "{merge}"),
		duration: b.f64hist("storage.merge.duration", "merge wall time", "s"),
		parts:    b.i64hist("storage.merge.parts_in", "source parts compacted per merge", "{part}"),
	}
	fetch := &Fetch{
		total:        b.counter("storage.fetch.total", "fetch requests served", "{fetch}"),
		duration:     b.f64hist("storage.fetch.duration", "fetch wall time", "s"),
		series:       b.i64hist("storage.fetch.series_matched", "series matched per fetch", "{series}"),
		rows:         b.i64hist("storage.fetch.rows_returned", "rows returned per fetch", "{row}"),
		partsScanned: b.counter("storage.fetch.parts_scanned", "parts scanned across fetches", "{part}"),
	}

	return flush, merge, fetch, b.err
}
