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
//
// Like [Engine.AppendBatch] it decides the accepted set, logs it, and only then applies it, so an
// error leaves no row in the head that the log does not hold. Side deltas lead the accepted payload
// for the same reason: they are idempotent to re-absorb, records are not, so a log write that stops
// part-way must not have committed records ahead of their delta.
func (e *Engine) ApplyPrimary(data []byte, limits AppendLimits) (accepted []byte, res AppendResult, err error) {
	if err := e.refuseWrite(); err != nil {
		return nil, AppendResult{}, err
	}

	e.mu.Lock()
	defer e.mu.Unlock()

	a := &primaryApply{
		e:         e,
		limits:    limits,
		byID:      make(map[signal.SeriesID]signal.Series),
		written:   make(map[signal.SeriesID]struct{}),
		overCard:  make(map[signal.SeriesID]struct{}),
		appenders: make(map[signal.SeriesID]*streamAppender),
	}
	a.sides, a.w = wal.NewWriter(&a.sideBuf), wal.NewWriter(&a.recBuf)

	if err := wal.Replay(data, wal.Handlers{OnSeries: a.series, OnRecords: a.records, OnSide: a.side}); err != nil {
		return nil, AppendResult{}, err
	}

	accepted = a.recBuf.Bytes()
	if a.sideBuf.Len() > 0 {
		accepted = append(a.sideBuf.Bytes(), accepted...)
	}

	// The accepted frames are the primary's durable copy of the shard's unflushed head, the one the
	// quorum ack counts on: a restart replays them, instead of serving a hole for everything written
	// since the last flush. They are already framed for replication, so the log takes them verbatim.
	if e.cfg.WAL != nil {
		if err := e.cfg.WAL.WriteFrames(accepted); err != nil {
			return nil, AppendResult{}, err
		}
	}

	for _, r := range a.runs {
		for i := range r.recs {
			r.app.apply(r.recs[i])
		}
	}

	for _, app := range a.appenders {
		app.commit()
	}

	return accepted, a.res, nil
}

// primaryApply is one [Engine.ApplyPrimary] call's decision state: the accepted set is decided while
// the incoming payload replays, and applied only after it is logged.
type primaryApply struct {
	e      *Engine
	limits AppendLimits

	sideBuf, recBuf bytes.Buffer
	sides, w        *wal.Writer

	byID     map[signal.SeriesID]signal.Series
	written  map[signal.SeriesID]struct{}
	overCard map[signal.SeriesID]struct{} // streams shed by the cardinality cap

	// appenders holds one appender per stream for the whole write: a second run of the same stream
	// must see the first run's admitted records in its out-of-order watermark.
	appenders map[signal.SeriesID]*streamAppender
	runs      []primaryRun
	pending   int64
	res       AppendResult
}

type primaryRun struct {
	app  *streamAppender
	recs []rec
}

func (a *primaryApply) series(id signal.SeriesID, s signal.Series) error {
	a.byID[id] = s
	// The primary is the shard's single authority, so it makes the cardinality decision here: a new
	// stream that would exceed MaxSeries is shed (its records are counted as cardinality rejections in
	// records below).
	if ok := a.e.head.ensureStream(id, func() signal.Series { return s }, a.limits.MaxSeries); !ok {
		a.overCard[id] = struct{}{}
	}

	return nil
}

func (a *primaryApply) records(id signal.SeriesID, blob []byte) error {
	e := a.e

	recs, err := decodeRecs(blob, e.cfg.Schema.numInts(), e.cfg.Schema.numBytes())
	if err != nil {
		return err
	}

	if _, shed := a.overCard[id]; shed {
		a.res.RejectedCardinality += len(recs)

		return nil
	}

	app, ok := a.appenders[id]
	if !ok {
		fresh := e.head.appenderFor(id)
		app = &fresh
		a.appenders[id] = app
	}

	acc := recs[:0]

	for i := range recs {
		// OOO + in-flight memory are the remaining primary-applied valves; secondaries apply the
		// accepted set verbatim via ApplyReplicated.
		out := app.admit(recs[i], e.cfg.OOOWindow, a.limits.MaxInFlightBytes, &a.pending)
		a.res = a.res.with(out)

		if out == admitted {
			acc = append(acc, recs[i])
		}
	}

	if len(acc) == 0 {
		return nil
	}

	a.runs = append(a.runs, primaryRun{app: app, recs: acc})

	if _, ok := a.written[id]; !ok {
		a.written[id] = struct{}{}
		if err := a.w.WriteSeries(id, a.byID[id]); err != nil {
			return err
		}
	}

	return a.w.WriteRecords(id, encodeRecs(acc))
}

func (a *primaryApply) side(payload []byte) error {
	// Absorb the symbol delta locally and forward it to the secondaries (content-addressed, so
	// re-absorbing on every replica is an idempotent dedup).
	if a.e.cfg.SideStore != nil {
		if err := a.e.cfg.SideStore.Absorb(payload); err != nil {
			return err
		}
	}

	return a.sides.WriteSide(payload)
}

// ApplyReplicated applies a replicated write from the primary verbatim (no OOO re-check — the
// primary already decided the accepted set), so all replicas hold identical data. Safe for
// concurrent use.
func (e *Engine) ApplyReplicated(data []byte) error {
	e.mu.Lock()
	defer e.mu.Unlock()

	return wal.Replay(data, e.replayHandlers())
}
