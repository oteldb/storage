package storage

import (
	"context"

	"github.com/go-faster/errors"

	"github.com/oteldb/storage/cluster"
	"github.com/oteldb/storage/recordengine"
	"github.com/oteldb/storage/signal"
)

// ValuesRequest selects a distinct-value enumeration over a record signal (logs, traces, profiles).
// Exactly one of Column (a byte column of the signal's schema, e.g. a span name) and AttrKey (one
// key inside the per-record attributes) must be set. Start/End bound the window, a zero start AND
// end disabling the time filter; a Limit ≤ 0 is unlimited.
type ValuesRequest struct {
	Signal     signal.Signal
	Column     string
	AttrKey    []byte
	Start, End int64
	Limit      int
}

// ColumnValues enumerates the distinct values a record column — or one per-record attribute key —
// takes for a tenant within the window. It is the enumeration primitive behind tag/label value
// autocomplete: a flushed part answers from its column dictionary, so the cost is O(distinct values)
// rather than O(records), and no attributes, events or links are materialized.
//
// The result is sorted, deduplicated, and a **superset** for the window — a part whose bounds
// overlap the window contributes its whole dictionary, so a value occurring only outside the window
// may be returned (consistent with the fetch contract, and harmless for autocomplete). Empty values
// are omitted; Limit truncates the sorted union to its lexicographically smallest values without
// signalling that it did. Numeric columns are not enumerable — a signal's numeric enums (span kind,
// status code) are a static set the caller already knows.
//
// In cluster mode it serves each shard locally when this node owns it, else from an owner (hedged),
// and unions the shards.
func (s *Storage) ColumnValues(ctx context.Context, tenant signal.TenantID, req ValuesRequest) ([][]byte, error) {
	if s.closed.Load() {
		return nil, errors.Wrap(ErrClosed, "column values")
	}

	if !isRecordSignal(req.Signal) {
		return nil, errors.Errorf("column values: %s is not a record signal", req.Signal)
	}

	tid := s.normalizeTenant(tenant)

	if s.cluster != nil {
		return s.clusterValues(ctx, cluster.ValuesRequest{
			Signal:  req.Signal,
			Tenant:  string(tid),
			Column:  req.Column,
			AttrKey: req.AttrKey,
			Start:   req.Start,
			End:     req.End,
			Limit:   req.Limit,
		})
	}

	eng, ok := s.lookupRecordEngine(req.Signal, tid)
	if !ok {
		return nil, nil
	}

	return eng.ColumnValues(ctx, recordengine.ValuesRequest{
		Column:  req.Column,
		AttrKey: req.AttrKey,
		Start:   req.Start,
		End:     req.End,
		Limit:   req.Limit,
	})
}

// isRecordSignal reports whether the signal is served by the record engine (metrics are not: they
// have no per-record columns to enumerate).
func isRecordSignal(sig signal.Signal) bool {
	switch sig {
	case signal.Log, signal.Trace, signal.Profile:
		return true
	default:
		return false
	}
}
