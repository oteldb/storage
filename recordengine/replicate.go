package recordengine

import (
	"bytes"

	"github.com/oteldb/storage/signal"
	"github.com/oteldb/storage/wal"
)

// ApplyPrimary applies a write as the stream's **primary**: it runs each record through the
// admission-checked append path (the single OOO decision for the shard, plus the cardinality and
// in-flight-memory valves from limits) and re-frames the *accepted* records into a WAL payload to
// replicate to the secondary owners. It returns that accepted payload and an [AppendResult]
// breaking the disposition down by reason, so the clustered ingest path attributes OTLP
// partial-success exactly like the single-node path. Every replica converges on the same data.
// Safe for concurrent use.
func (e *Engine) ApplyPrimary(data []byte, limits AppendLimits) (accepted []byte, res AppendResult, err error) {
	e.mu.Lock()
	defer e.mu.Unlock()

	var (
		buf      bytes.Buffer
		w        = wal.NewWriter(&buf)
		byID     = make(map[signal.SeriesID]signal.Series)
		written  = make(map[signal.SeriesID]struct{})
		overCard = make(map[signal.SeriesID]struct{}) // streams shed by the cardinality cap
	)

	err = wal.Replay(data, wal.Handlers{
		OnSeries: func(id signal.SeriesID, s signal.Series) error {
			byID[id] = s
			// The primary is the shard's single authority, so it makes the cardinality decision
			// here: a new stream that would exceed MaxSeries is shed (its records are counted as
			// cardinality rejections in OnRecords below).
			if _, ok := e.head.ensureStream(id, func() signal.Series { return s }, limits.MaxSeries); !ok {
				overCard[id] = struct{}{}
			}

			return nil
		},
		OnRecords: func(id signal.SeriesID, blob []byte) error {
			recs, derr := decodeRecs(blob, e.cfg.Schema.numInts(), e.cfg.Schema.numBytes())
			if derr != nil {
				return derr
			}

			if _, shed := overCard[id]; shed {
				res.RejectedCardinality += len(recs)

				return nil
			}

			acc := recs[:0]
			for i := range recs {
				// OOO + in-flight memory are the remaining primary-applied valves; secondaries apply
				// the accepted set verbatim via ApplyReplicated.
				switch e.head.appendRecord(id, recs[i], e.cfg.OOOWindow, limits.MaxInFlightBytes) {
				case admitted:
					acc = append(acc, recs[i])
					res.Accepted++
				case rejectOOO:
					res.RejectedOOO++
				case rejectBytes:
					res.RejectedBytes++
				}
			}

			if len(acc) == 0 {
				return nil
			}

			if _, ok := written[id]; !ok {
				written[id] = struct{}{}
				if werr := w.WriteSeries(id, byID[id]); werr != nil {
					return werr
				}
			}

			return w.WriteRecords(id, encodeRecs(acc))
		},
		OnSide: func(payload []byte) error {
			// Absorb the symbol delta locally and forward it to the secondaries (content-addressed,
			// so re-absorbing on every replica is an idempotent dedup).
			if e.cfg.SideStore != nil {
				if aerr := e.cfg.SideStore.Absorb(payload); aerr != nil {
					return aerr
				}
			}

			return w.WriteSide(payload)
		},
	})

	return buf.Bytes(), res, err
}

// ApplyReplicated applies a replicated write from the primary verbatim (no OOO re-check — the
// primary already decided the accepted set), so all replicas hold identical data. Safe for
// concurrent use.
func (e *Engine) ApplyReplicated(data []byte) error {
	e.mu.Lock()
	defer e.mu.Unlock()

	return wal.Replay(data, e.replayHandlers())
}
