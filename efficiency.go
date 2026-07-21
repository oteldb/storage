package storage

import (
	"context"
	"sort"

	"github.com/oteldb/storage/engine"
	"github.com/oteldb/storage/recordengine"
	"github.com/oteldb/storage/signal"
)

// SignalEfficiency is one (tenant, signal)'s flushed-data shape for capacity/efficiency
// dashboards: how many points it stores, in how many bytes, and how well they compress.
type SignalEfficiency struct {
	Signal signal.Signal
	// Series is the distinct series/streams (index span, head ∪ flushed).
	Series int64
	// Parts is the flushed part count; Points the total samples (metrics) or records
	// (logs/traces/profiles) across them.
	Parts  int
	Points int64
	// StoredBytes is the sum of the parts' backend object sizes on THIS node — under erasure
	// coding with slot filtering that is the node's local footprint (its shard + sidecars), not
	// the cluster-wide logical total.
	StoredBytes int64
	// BytesPerPoint is StoredBytes / Points (0 when empty) — the per-sample storage cost, the
	// single most useful capacity-planning number.
	BytesPerPoint float64
	// LogicalBytes is the uncompressed logical size of the stored points. Exact for metrics
	// (Points × 16: an int64 timestamp + a float64 value); 0 for the record signals, whose
	// per-record logical size varies and is not recorded per part.
	LogicalBytes int64
	// CompressionRatio is LogicalBytes / StoredBytes (e.g. 8.0 = stored at 1/8th of logical);
	// 0 when LogicalBytes is unknown (record signals) or nothing is stored.
	CompressionRatio float64
}

// TenantEfficiency is one tenant's per-signal efficiency breakdown.
type TenantEfficiency struct {
	Tenant  signal.TenantID
	Signals []SignalEfficiency
}

// EfficiencyStats returns the flushed-data shape of every tenant — signal counts, stored bytes,
// bytes per point, compression ratios — for an admin/capacity dashboard. Unlike [Storage.Inspect]
// it **does backend I/O** (per-part object sizes, like [engine.Engine.PartsDetailed]): poll it at
// dashboard cadence, not per request. Results are sorted by tenant, then signal.
func (s *Storage) EfficiencyStats(ctx context.Context) ([]TenantEfficiency, error) {
	byTenant := make(map[signal.TenantID]*TenantEfficiency)

	tenantOf := func(tid signal.TenantID) *TenantEfficiency {
		te, ok := byTenant[tid]
		if !ok {
			te = &TenantEfficiency{Tenant: tid}
			byTenant[tid] = te
		}

		return te
	}

	for tid, eng := range s.engineSnapshotByTenant() {
		parts, err := eng.PartsDetailed(ctx)
		if err != nil {
			return nil, err
		}

		se := SignalEfficiency{Signal: signal.Metric, Series: eng.Stats().Series, Parts: len(parts)}
		for _, p := range parts {
			se.Points += p.Rows
			se.StoredBytes += p.Bytes
		}

		se.LogicalBytes = se.Points * engine.SampleBytes
		finishEfficiency(&se)
		tenantOf(tid).Signals = append(tenantOf(tid).Signals, se)
	}

	addRecord := func(sig signal.Signal, engines map[signal.TenantID]*recordengine.Engine) error {
		for tid, eng := range engines {
			parts, err := eng.PartsDetailed(ctx)
			if err != nil {
				return err
			}

			se := SignalEfficiency{Signal: sig, Series: eng.Stats().Streams, Parts: len(parts)}
			for _, p := range parts {
				se.Points += p.Rows
				se.StoredBytes += p.Bytes
			}

			finishEfficiency(&se) // LogicalBytes stays 0: record logical size is not recorded
			tenantOf(tid).Signals = append(tenantOf(tid).Signals, se)
		}

		return nil
	}

	if err := addRecord(signal.Log, s.logEngineSnapshotByTenant()); err != nil {
		return nil, err
	}

	if err := addRecord(signal.Trace, s.traceEngineSnapshotByTenant()); err != nil {
		return nil, err
	}

	if err := addRecord(signal.Profile, s.profileEngineSnapshotByTenant()); err != nil {
		return nil, err
	}

	out := make([]TenantEfficiency, 0, len(byTenant))
	for _, te := range byTenant {
		sort.Slice(te.Signals, func(i, j int) bool { return te.Signals[i].Signal < te.Signals[j].Signal })
		out = append(out, *te)
	}

	sort.Slice(out, func(i, j int) bool { return out[i].Tenant < out[j].Tenant })

	return out, nil
}

// finishEfficiency computes the derived ratios once the sums are in.
func finishEfficiency(se *SignalEfficiency) {
	if se.Points > 0 {
		se.BytesPerPoint = float64(se.StoredBytes) / float64(se.Points)
	}

	if se.LogicalBytes > 0 && se.StoredBytes > 0 {
		se.CompressionRatio = float64(se.LogicalBytes) / float64(se.StoredBytes)
	}
}
