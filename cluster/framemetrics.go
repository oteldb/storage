package cluster

import (
	"bytes"

	"github.com/oteldb/storage/signal"
	"github.com/oteldb/storage/signal/metric"
	"github.com/oteldb/storage/wal"
)

// DefaultTenant is the tenant id a record routes to when no routing callback is configured, or
// when one returns the empty id.
const DefaultTenant signal.TenantID = "default"

// TenantFunc derives a batch's tenant from its resource and scope, so one OTLP batch may fan out
// to many tenants. A nil func, or one returning "", routes everything to [DefaultTenant].
type TenantFunc func(signal.Resource, signal.Scope) signal.TenantID

// AdmitFunc is the origin-side ingest valve: it reports whether a projected batch is admitted, and
// a false sheds the whole batch before it is framed. Nil admits everything.
//
// It is called once per projected batch in projection order, so an implementation may cache the
// state it resolves per tenant across a tenant-contiguous run.
type AdmitFunc func(tenant signal.TenantID, b *metric.Batch) bool

// MetricFrames is what [FrameMetrics] produced: one WAL-encoded payload per shard key, ready to be
// routed to that shard's primary, plus what projection saw.
type MetricFrames struct {
	// Shards maps a shard key to its WAL-encoded run of records.
	Shards map[signal.TenantID][]byte
	// Emitted is how many points projection produced, admitted or not.
	Emitted int
	// Shed is how many points the admit valve dropped before framing.
	Shed int
}

// FrameMetrics projects md and groups every point by the shard key it routes to, framing each
// shard's records as a WAL payload.
//
// Grouping by shard rather than by tenant is what spreads one large tenant's ingest across the
// ring: each shard routes to its own primary independently. With a single shard the key is the
// tenant, identical to the unsharded path.
//
// Both the node and a standalone ingester frame writes through here, so the two cannot disagree
// about which shard a series belongs to — a divergence that would otherwise be silent, writing to
// a shard nobody reads.
func FrameMetrics(md metric.Metrics, shards int, tenantOf TenantFunc, admit AdmitFunc) MetricFrames {
	type shardWAL struct {
		buf  bytes.Buffer
		w    *wal.Writer
		seen map[signal.SeriesID]struct{}
	}

	n := ShardCount(shards)
	byShard := make(map[signal.TenantID]*shardWAL)

	var shed int

	emitted := metric.Project(md, func(b *metric.Batch) {
		tid := DefaultTenant
		if tenantOf != nil {
			if t := tenantOf(b.Resource(), b.Scope()); t != "" {
				tid = t
			}
		}

		if admit != nil && !admit(tid, b) {
			shed += b.Len()

			return
		}

		for i := range b.Len() {
			id := b.IDs[i]
			sk := ShardKeyOf(tid, ShardOf(id, n), n)

			sw := byShard[sk]
			if sw == nil {
				sw = &shardWAL{seen: make(map[signal.SeriesID]struct{})}
				sw.w = wal.NewWriter(&sw.buf)
				byShard[sk] = sw
			}

			if _, ok := sw.seen[id]; !ok { // register each series once per shard
				sw.seen[id] = struct{}{}
				_ = sw.w.WriteSeries(id, b.Series(i))
			}

			_ = sw.w.WriteSamples(id, b.Ts[i:i+1], b.Values[i:i+1])
		}
	})

	frames := make(map[signal.TenantID][]byte, len(byShard))
	for sk, sw := range byShard {
		frames[sk] = sw.buf.Bytes()
	}

	return MetricFrames{Shards: frames, Emitted: emitted, Shed: shed}
}
