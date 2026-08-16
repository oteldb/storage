package cluster

import (
	"github.com/oteldb/storage/recordengine"
	"github.com/oteldb/storage/signal"
)

// RecordProjector projects a record signal's ingest batch, calling emit once per stream and
// returning the total record count. It is what log.Project, trace.Project and profile.Project
// present, so the three signals share one framing path.
type RecordProjector func(emit func(*recordengine.Batch)) int

// RecordAdmitFunc is the origin-side ingest valve for records: false sheds the whole stream batch
// before it is framed. Nil admits everything. Batches arrive in projection order, so an
// implementation may cache per-tenant state across a contiguous run.
type RecordAdmitFunc func(tenant signal.TenantID, b *recordengine.Batch) bool

// RecordFrames is what [FrameRecords] produced: one WAL-encoded payload per shard key.
type RecordFrames struct {
	// Shards maps a shard key to its WAL-encoded run of records.
	Shards map[signal.TenantID][]byte
	// Emitted is how many records projection produced, admitted or not.
	Emitted int
	// Shed is how many records the admit valve dropped before framing.
	Shed int
}

// FrameRecords groups a record signal's streams by shard key and frames each shard's records as a
// WAL payload — the record-signal twin of [FrameMetrics].
//
// A stream is the unit here, not a record: a stream's identity is registered once and its records
// follow, so the whole stream routes to one shard. That is what keeps a log stream's records
// together on one primary rather than scattered across the ring.
func FrameRecords(project RecordProjector, shards int, tenantOf TenantFunc, admit RecordAdmitFunc) RecordFrames {
	n := ShardCount(shards)
	byShard := make(map[signal.TenantID][]byte)

	var shed int

	emitted := project(func(b *recordengine.Batch) {
		id := b.Identity()

		tid := DefaultTenant
		if tenantOf != nil {
			if t := tenantOf(id.Resource, id.Scope); t != "" {
				tid = t
			}
		}

		if admit != nil && !admit(tid, b) {
			shed += b.Len()

			return
		}

		sk := ShardKeyOf(tid, ShardOf(b.Stream, n), n)
		byShard[sk] = append(byShard[sk], recordengine.EncodeWAL(b)...)
	})

	return RecordFrames{Shards: byShard, Emitted: emitted, Shed: shed}
}
