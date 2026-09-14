package recordengine

import (
	"github.com/go-faster/errors"
)

// batchRec loads record i of b into scratch. The byte cells alias b.
func batchRec(scratch *rec, b *Batch, i int) {
	scratch.ts = b.Ts[i]
	for k := range b.Ints {
		scratch.ints[k] = b.Ints[k][i]
	}

	for k := range b.Bytes {
		scratch.bytes[k] = b.Bytes[k][i]
	}
}

func newBatchScratch(b *Batch) rec {
	return rec{ints: make([]int64, len(b.Ints)), bytes: make([][]byte, len(b.Bytes))}
}

// appendUnlogged is [Engine.AppendBatch] for an engine with no WAL: nothing can fail between admitting
// a record and applying it, so it does both in one pass. Caller holds e.mu.
func (e *Engine) appendUnlogged(b *Batch, limits AppendLimits) (AppendResult, error) {
	scratch := newBatchScratch(b)

	var res AppendResult

	app := e.head.appenderFor(b.Stream)

	for i := range b.Ts {
		batchRec(&scratch, b, i)
		res = res.with(app.append(scratch, e.cfg.OOOWindow, limits.MaxInFlightBytes))
	}

	app.commit()

	if e.cfg.SideStore != nil && res.Accepted > 0 && len(b.Side) > 0 {
		if err := e.cfg.SideStore.Absorb(b.Side); err != nil {
			return res, errors.Wrap(err, "absorb side delta")
		}
	}

	return res, nil
}

// appendLogged is [Engine.AppendBatch] for an engine with a WAL. It decides the accepted set, logs it,
// and only then applies it, so a write that fails leaves the head exactly as it was: no row that the
// log does not hold, and no out-of-order watermark raised by one. Were rows applied first, a failed
// log write would leave them in the head un-logged while the caller, seeing the error, retried — and
// records are append-only, so the retry would store them twice.
//
// The side delta is absorbed and logged before the records for the same reason. It is content-addressed
// and absorbing it is an idempotent dedup, so a delta that reached the log (or the side store) without
// its records costs nothing on retry, whereas records that reached the log without their delta would be
// replayed and then written again. Caller holds e.mu.
func (e *Engine) appendLogged(b *Batch, limits AppendLimits) (AppendResult, error) {
	scratch := newBatchScratch(b)

	var (
		res     AppendResult
		pending int64
	)

	app := e.head.appenderFor(b.Stream)
	rows := e.admitRows[:0]
	payload := openRecs(e.walBuf)

	for i := range b.Ts {
		batchRec(&scratch, b, i)

		out := app.admit(scratch, e.cfg.OOOWindow, limits.MaxInFlightBytes, &pending)
		res = res.with(out)

		if out == admitted {
			rows = append(rows, i)
			payload = appendBatchRec(payload, b, i)
		}
	}

	e.admitRows, e.walBuf = rows, payload
	if cap(payload) > walBufKeepBytes {
		e.admitRows, e.walBuf = nil, nil
	}

	if res.Accepted == 0 {
		return res, nil
	}

	if e.cfg.SideStore != nil && len(b.Side) > 0 {
		if err := e.cfg.SideStore.Absorb(b.Side); err != nil {
			return AppendResult{}, errors.Wrap(err, "absorb side delta")
		}
	}

	if err := e.logWAL(b, sealRecs(payload, res.Accepted)); err != nil {
		return AppendResult{}, err
	}

	for _, i := range rows {
		batchRec(&scratch, b, i)
		app.apply(scratch)
	}

	app.commit()

	return res, nil
}

// walBufKeepBytes bounds the WAL payload buffer an engine keeps between writes.
const walBufKeepBytes = 4 << 20

// with returns r with one more record counted under out.
func (r AppendResult) with(out admitOutcome) AppendResult {
	switch out {
	case admitted:
		r.Accepted++
	case rejectOOO:
		r.RejectedOOO++
	case rejectBytes:
		r.RejectedBytes++
	}

	return r
}
