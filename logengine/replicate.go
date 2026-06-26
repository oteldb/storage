package logengine

import (
	"bytes"

	"github.com/oteldb/storage/signal"
	"github.com/oteldb/storage/wal"
)

// ApplyPrimary applies a write as the stream's **primary**: it appends each record through the
// out-of-order check (the single OOO decision for the shard) and re-frames the *accepted* records
// into a WAL payload to replicate to the secondary owners. It returns that accepted payload and
// the number of records rejected as out-of-order. Because only the primary OOO-checks and dictates
// the accepted set, every replica converges on the same data. Safe for concurrent use. It mirrors
// engine.Engine.ApplyPrimary for the logs vertical.
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
		OnLogRecords: func(id signal.SeriesID, recs []wal.LogRecord) error {
			s := byID[id] // the stream record precedes its log records in the frame

			var acc []wal.LogRecord

			for i := range recs {
				if e.head.appendRecord(id, fromWALRecord(recs[i]), e.cfg.OOOWindow) {
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
				if err := w.WriteSeries(id, s); err != nil {
					return err
				}
			}

			return w.WriteLogRecords(id, acc)
		},
	})

	return buf.Bytes(), rejected, err
}

// ApplyReplicated applies a replicated write from the stream's primary to this secondary verbatim
// (no OOO re-check — the primary already decided the accepted set), the way WAL replay trusts the
// log, so all replicas hold identical data. Safe for concurrent use.
func (e *Engine) ApplyReplicated(data []byte) error {
	e.mu.Lock()
	defer e.mu.Unlock()

	return wal.Replay(data, wal.Handlers{
		OnSeries: func(_ signal.SeriesID, s signal.Series) error {
			e.head.registerStream(s)

			return nil
		},
		OnLogRecords: func(id signal.SeriesID, recs []wal.LogRecord) error {
			e.head.replayRecords(id, toRecs(recs))

			return nil
		},
	})
}
