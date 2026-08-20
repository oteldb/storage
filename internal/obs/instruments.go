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

// f64hist builds a seconds-valued duration histogram (every duration instrument is in seconds).
func (b *imb) f64hist(name, desc string) metric.Float64Histogram {
	if b.err != nil {
		return nil
	}

	h, err := b.m.Float64Histogram(name, metric.WithDescription(desc), metric.WithUnit("s"))
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

// Parts reports the merge selector's view of a signal's flushed parts as gauges: how many parts
// there are, how many are sealed (no merge will reconsider them), how many a merge may still take,
// how many the next merge would select, and the seal threshold in effect. Gauges rather than a log
// line because the question they answer — is compaction ever going to reduce this part count? — is
// asked of a dashboard over time, not of one cycle.
type Parts struct {
	total      metric.Int64Gauge
	sealed     metric.Int64Gauge
	backlog    metric.Int64Gauge
	candidates metric.Int64Gauge
	capBytes   metric.Int64Gauge
}

// Record publishes one signal's part shape, summed over the tenants this node holds. The values are
// tagged by signal only: tenant ids are unbounded, and [storage.Storage.Inspect] is the per-tenant
// surface.
func (p *Parts) Record(ctx context.Context, sig string, total, sealed, backlog, candidates, capBytes int64) {
	a := sigAttr(sig)
	p.total.Record(ctx, total, a)
	p.sealed.Record(ctx, sealed, a)
	p.backlog.Record(ctx, backlog, a)
	p.candidates.Record(ctx, candidates, a)
	p.capBytes.Record(ctx, capBytes, a)
}

// Fetch instruments a fetch over the head ∪ parts.
type Fetch struct {
	total        metric.Int64Counter
	duration     metric.Float64Histogram
	series       metric.Int64Histogram
	rows         metric.Int64Histogram
	partsScanned metric.Int64Counter
	budgetForced metric.Int64Counter
}

// ForcedAdmission accounts one query admitted over the decode-memory ceiling because its wait made
// no progress. It is a liveness escape, so a non-zero rate means the budget is oversubscribed (or a
// caller holds several unscoped reads open) and the ceiling is not holding.
func (f *Fetch) ForcedAdmission(ctx context.Context, sig string) {
	f.budgetForced.Add(ctx, 1, sigAttr(sig))
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

// Backend instruments the L1 object-store operations.
type Backend struct {
	ops     metric.Int64Counter
	bytes   metric.Int64Counter
	latency metric.Float64Histogram
}

// Record accounts one backend op (read/write/list/delete/cas) with its result (ok/not_found/error),
// wall time dur, and the bytes it moved (0 for none).
func (b *Backend) Record(ctx context.Context, op, result string, dur time.Duration, bytes int64) {
	opAttr := metric.WithAttributes(attribute.String("op", op))
	b.ops.Add(ctx, 1, metric.WithAttributes(attribute.String("op", op), attribute.String("result", result)))
	b.latency.Record(ctx, dur.Seconds(), opAttr)

	if bytes > 0 {
		b.bytes.Add(ctx, bytes, opAttr)
	}
}

// RPC instruments the node-to-node cluster transport's reliability behavior: how often calls are
// attempted, retried, or hedged (an opportunistic concurrent attempt fired because the in-flight one
// was slow). Counts are tagged by op ("read"/"write"/"series"/"side"), so a rising retry/hedge rate
// localizes a degrading link or peer.
type RPC struct {
	attempts   metric.Int64Counter
	retries    metric.Int64Counter
	hedges     metric.Int64Counter
	absent     metric.Int64Counter
	incomplete metric.Int64Counter
}

func opAttr(op string) metric.MeasurementOption {
	return metric.WithAttributes(attribute.String("op", op))
}

// Attempt accounts one transport attempt (including the first) for op.
func (r *RPC) Attempt(ctx context.Context, op string) { r.attempts.Add(ctx, 1, opAttr(op)) }

// Retry accounts one sequential retry for op.
func (r *RPC) Retry(ctx context.Context, op string) { r.retries.Add(ctx, 1, opAttr(op)) }

// ShardAbsent accounts one read of a shard this node is a ring owner of but holds no data for, so
// the read failed over to another owner instead of answering empty. A sustained rate means the ring
// and the data disagree — a rebalance whose backfill has not caught up, or a lagging membership view.
func (r *RPC) ShardAbsent(ctx context.Context, op string) { r.absent.Add(ctx, 1, opAttr(op)) }

// ShardIncomplete accounts one read of a shard this node holds but cannot fully answer for: it came
// back from a restart (or a rebalance) without the head the shard held unflushed, so the query
// window overlaps data only another owner has, and the read failed over. It clears once the shard's
// parts cover that window; a sustained rate means no owner is flushing.
func (r *RPC) ShardIncomplete(ctx context.Context, op string) { r.incomplete.Add(ctx, 1, opAttr(op)) }

// Hedge accounts one hedged (opportunistic concurrent) attempt for op.
func (r *RPC) Hedge(ctx context.Context, op string) { r.hedges.Add(ctx, 1, opAttr(op)) }

func newRPC(m metric.Meter) (*RPC, error) {
	b := &imb{m: m}
	r := &RPC{
		attempts: b.counter("storage.rpc.attempts", "cluster RPC attempts (incl. first)", "{attempt}"),
		retries:  b.counter("storage.rpc.retries", "cluster RPC sequential retries", "{retry}"),
		hedges:   b.counter("storage.rpc.hedges", "cluster RPC hedged (concurrent) attempts", "{hedge}"),
		absent:   b.counter("storage.rpc.shard_absent", "shard reads failed over because the owner holds no data", "{read}"),
		incomplete: b.counter("storage.rpc.shard_incomplete",
			"shard reads failed over because the owner's copy is missing its unflushed head", "{read}"),
	}

	return r, b.err
}

func newBackend(m metric.Meter) (*Backend, error) {
	b := &imb{m: m}
	bk := &Backend{
		ops:     b.counter("storage.backend.ops", "backend object-store operations", "{op}"),
		bytes:   b.counter("storage.backend.bytes", "bytes read from / written to the backend", "By"),
		latency: b.f64hist("storage.backend.latency", "backend operation wall time"),
	}

	return bk, b.err
}

// WAL instruments the write-ahead log. The recording methods use a background context (the WAL
// writer carries none) and are called at append/fsync/rotation granularity, never per sample.
type WAL struct {
	appends   metric.Int64Counter
	fsyncs    metric.Int64Counter
	rotations metric.Int64Counter
}

// Append accounts one WAL record-batch append.
func (w *WAL) Append() { w.appends.Add(context.Background(), 1) }

// Fsync accounts one WAL fsync.
func (w *WAL) Fsync() { w.fsyncs.Add(context.Background(), 1) }

// Rotate accounts one WAL segment rotation.
func (w *WAL) Rotate() { w.rotations.Add(context.Background(), 1) }

func newWAL(m metric.Meter) (*WAL, error) {
	b := &imb{m: m}
	w := &WAL{
		appends:   b.counter("storage.wal.appends", "WAL record-batch appends", "{append}"),
		fsyncs:    b.counter("storage.wal.fsyncs", "WAL fsyncs", "{fsync}"),
		rotations: b.counter("storage.wal.rotations", "WAL segment rotations", "{rotation}"),
	}

	return w, b.err
}

// Cluster instruments this node's standing in the cluster member set — the state that decides
// whether any peer routes anything here at all. self_absent is the alertable form of a node
// that is up and serving but missing from etcd: the failure this cannot be inferred from any
// other signal, because the node itself looks perfectly healthy while it is happening.
type Cluster struct {
	selfAbsent metric.Int64Gauge
	rejoins    metric.Int64Counter
}

// Record publishes this node's membership standing: absent is whether it is currently missing
// from the member set, rejoins the re-registrations since the previous call.
func (c *Cluster) Record(ctx context.Context, absent bool, rejoins int64) {
	var v int64
	if absent {
		v = 1
	}

	c.selfAbsent.Record(ctx, v)

	if rejoins > 0 {
		c.rejoins.Add(ctx, rejoins)
	}
}

func newCluster(m metric.Meter) (*Cluster, error) {
	b := &imb{m: m}
	c := &Cluster{
		selfAbsent: b.gauge("storage.cluster.self_absent",
			"1 while this node is running but missing from the cluster member set", "{node}"),
		rejoins: b.counter("storage.cluster.rejoins",
			"times this node re-registered after losing its membership lease", "{rejoin}"),
	}

	return c, b.err
}

func (b *imb) gauge(name, desc, unit string) metric.Int64Gauge {
	if b.err != nil {
		return nil
	}

	g, err := b.m.Int64Gauge(name, metric.WithDescription(desc), metric.WithUnit(unit))
	b.err = err

	return g
}

func newParts(m metric.Meter) (*Parts, error) {
	b := &imb{m: m}
	p := &Parts{
		total:      b.gauge("storage.parts.total", "flushed immutable parts", "{part}"),
		sealed:     b.gauge("storage.parts.sealed", "parts at the merge cap, which no merge reconsiders", "{part}"),
		backlog:    b.gauge("storage.parts.merge_backlog", "unsealed parts a merge may still take", "{part}"),
		candidates: b.gauge("storage.parts.merge_candidates", "parts the next merge would select", "{part}"),
		capBytes:   b.gauge("storage.merge.cap_bytes", "seal threshold in effect for a merged part", "By"),
	}

	return p, b.err
}

func newEngineInstruments(m metric.Meter) (*Flush, *Merge, *Fetch, error) {
	b := &imb{m: m}

	flush := &Flush{
		total:    b.counter("storage.flush.total", "head flushes to a part", "{flush}"),
		duration: b.f64hist("storage.flush.duration", "flush wall time"),
		rows:     b.i64hist("storage.flush.rows", "rows written per flush", "{row}"),
	}
	merge := &Merge{
		total:    b.counter("storage.merge.total", "background merges", "{merge}"),
		duration: b.f64hist("storage.merge.duration", "merge wall time"),
		parts:    b.i64hist("storage.merge.parts_in", "source parts compacted per merge", "{part}"),
	}
	fetch := &Fetch{
		total:        b.counter("storage.fetch.total", "fetch requests served", "{fetch}"),
		duration:     b.f64hist("storage.fetch.duration", "fetch wall time"),
		series:       b.i64hist("storage.fetch.series_matched", "series matched per fetch", "{series}"),
		rows:         b.i64hist("storage.fetch.rows_returned", "rows returned per fetch", "{row}"),
		partsScanned: b.counter("storage.fetch.parts_scanned", "parts scanned across fetches", "{part}"),
		budgetForced: b.counter("storage.fetch.decode_budget_forced_admissions",
			"queries admitted over the decode-memory ceiling after a stalled wait", "{admission}"),
	}

	return flush, merge, fetch, b.err
}

// Disk instruments the medium's ability to take what the node accepts. A full disk or an exhausted
// inode table is not a transient backend fault: the node keeps acking writes it cannot store, so
// the two states need separate signals — a gauge that stands while the engine refuses writes, and
// a counter of the writes it refused.
type Disk struct {
	outOfSpace metric.Int64Gauge
	rejected   metric.Int64Counter
}

// Record publishes a signal's disk-pressure state: whether its engine is currently refusing writes
// for want of room, and how many writes it refused since the previous call. Both are published
// together, from the flush that decides the state — the ingest path that counts a rejection carries
// no context of its own, and a counter is as useful at flush cadence as per write.
func (d *Disk) Record(ctx context.Context, sig string, outOfSpace bool, rejected int64) {
	a := sigAttr(sig)

	var v int64
	if outOfSpace {
		v = 1
	}

	d.outOfSpace.Record(ctx, v, a)

	if rejected > 0 {
		d.rejected.Add(ctx, rejected, a)
	}
}

func newDisk(m metric.Meter) (*Disk, error) {
	b := &imb{m: m}
	d := &Disk{
		outOfSpace: b.gauge("storage.disk.out_of_space",
			"1 while an engine refuses writes because the medium is out of bytes or inodes", "{engine}"),
		rejected: b.counter("storage.disk.rejected_writes",
			"writes refused because the medium cannot store them", "{write}"),
	}

	return d, b.err
}
