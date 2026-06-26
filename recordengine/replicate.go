package recordengine

import (
	"bytes"

	"github.com/oteldb/storage/signal"
	"github.com/oteldb/storage/wal"
)

// ApplyPrimary applies a write as the stream's **primary**: it appends each record through the
// out-of-order check (the single OOO decision for the shard) and re-frames the *accepted* records
// into a WAL payload to replicate to the secondary owners. It returns that accepted payload and the
// number of records rejected as out-of-order. Every replica converges on the same data. Safe for
// concurrent use.
func (e *Engine) ApplyPrimary(data []byte) (accepted []byte, rejected int, err error) {
	e.mu.Lock()
	defer e.mu.Unlock()

	var (
		buf     bytes.Buffer
		w       = wal.NewWriter(&buf)
		byID    = make(map[signal.SeriesID]signal.Series)
		written = make(map[signal.SeriesID]struct{})
	)

	err = wal.Replay(data, wal.Handlers{
		OnSeries: func(id signal.SeriesID, s signal.Series) error {
			byID[id] = s
			e.head.registerStream(s)

			return nil
		},
		OnRecords: func(id signal.SeriesID, blob []byte) error {
			recs, derr := decodeRecs(blob, e.cfg.Schema.numInts(), e.cfg.Schema.numBytes())
			if derr != nil {
				return derr
			}

			acc := recs[:0]
			for i := range recs {
				if e.head.appendRecord(id, recs[i], e.cfg.OOOWindow) {
					acc = append(acc, recs[i])
				} else {
					rejected++
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

	return buf.Bytes(), rejected, err
}

// ApplyReplicated applies a replicated write from the primary verbatim (no OOO re-check — the
// primary already decided the accepted set), so all replicas hold identical data. Safe for
// concurrent use.
func (e *Engine) ApplyReplicated(data []byte) error {
	e.mu.Lock()
	defer e.mu.Unlock()

	return wal.Replay(data, e.replayHandlers())
}
