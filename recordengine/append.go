package recordengine

import (
	"github.com/go-faster/errors"
)

func batchRowBytes(b *Batch, i int) int64 {
	n := int64(8 + 8*len(b.Ints))
	for _, col := range b.Bytes {
		n += int64(len(col[i]))
	}
	return n
}

// applyBatch appends admitted rows column by column, copying byte cells into head-owned blobs.
func (a *streamAppender) applyBatch(b *Batch, rows []int, size int64) {
	if len(rows) == 0 {
		return
	}
	for len(rows) > 0 {
		end := 1
		for end < len(rows) && rows[end] == rows[end-1]+1 {
			end++
		}
		a.applyBatchRange(b, rows[0], rows[end-1]+1)
		rows = rows[end:]
	}
	a.h.grow(size)
}

func (a *streamAppender) applyBatchRange(b *Batch, lo, hi int) {
	if lo == hi {
		return
	}
	if a.buf == nil || (len(a.buf.ts) == 0 && cap(a.buf.ts) == 0 && a.h.reusable != nil) {
		a.buf = a.h.takeRecordCols()
		a.h.records[a.id] = a.buf
	}
	a.buf.ts = append(a.buf.ts, b.Ts[lo:hi]...)
	for _, ts := range b.Ts[lo:hi] {
		a.buf.noteTS(ts)
		a.h.newest = max(a.h.newest, ts)
	}
	for k, col := range b.Ints {
		a.buf.ints[k] = append(a.buf.ints[k], col[lo:hi]...)
	}
	for k, col := range b.Bytes {
		for _, cell := range col[lo:hi] {
			a.buf.bytes[k].appendCell(cell)
		}
	}
}

// appendUnlogged admits rows in arrival order and applies contiguous accepted runs without staging
// a WAL payload. Caller holds e.mu.
func (e *Engine) appendUnlogged(b *Batch, limits AppendLimits) (AppendResult, error) {
	var (
		res     AppendResult
		pending int64
		start   int
	)
	app := e.head.appenderFor(b.Stream)
	for i, ts := range b.Ts {
		out := rejectRetention
		if !limits.EnforceMinTimestamp || ts >= limits.MinTimestamp {
			out = app.admitSized(ts, batchRowBytes(b, i), e.cfg.OOOWindow, limits.MaxInFlightBytes, &pending)
		}
		res = res.with(out)
		if out != admitted {
			app.applyBatchRange(b, start, i)
			if pending > 0 {
				e.head.grow(pending)
				pending = 0
			}
			start = i + 1
		}
	}
	app.applyBatchRange(b, start, len(b.Ts))
	if pending > 0 {
		e.head.grow(pending)
	}
	app.commit()

	if e.cfg.SideStore != nil && res.Accepted > 0 && len(b.Side) > 0 {
		e.sideStray = e.sideStray || res.Rejected() > 0
		if err := e.absorbSideLocked(b.Side); err != nil {
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
	var (
		res     AppendResult
		pending int64
	)

	app := e.head.appenderFor(b.Stream)
	rows := e.admitRows[:0]
	payload := openRecs(e.walBuf)

	for i := range b.Ts {
		if limits.EnforceMinTimestamp && b.Ts[i] < limits.MinTimestamp {
			res.RejectedRetention++
			continue
		}
		out := app.admitSized(b.Ts[i], batchRowBytes(b, i), e.cfg.OOOWindow, limits.MaxInFlightBytes, &pending)
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
		e.sideStray = e.sideStray || res.Rejected() > 0
		if err := e.absorbSideLocked(b.Side); err != nil {
			return AppendResult{}, errors.Wrap(err, "absorb side delta")
		}
	}

	if err := e.mu.WithReaders(func() error { return e.logWAL(b, sealRecs(payload, res.Accepted)) }); err != nil {
		e.sideStray = e.sideStray || len(b.Side) > 0

		return AppendResult{}, err
	}

	app.applyBatch(b, rows, pending)

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
	case rejectRetention:
		r.RejectedRetention++
	case rejectOOO:
		r.RejectedOOO++
	case rejectBytes:
		r.RejectedBytes++
	}

	return r
}
