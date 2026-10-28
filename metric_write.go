package storage

import (
	"github.com/oteldb/storage/engine"
	"github.com/oteldb/storage/signal"
	"github.com/oteldb/storage/signal/metric"
)

type metricWriter struct {
	store          *Storage
	buffer         *metricAppendBuffer
	run            metricRun
	rejected       rejectTally
	sampledDropped int64
	overflowed     int64
	err            error
}

func (w *metricWriter) flush() error {
	if len(w.buffer.ids) == 0 {
		return nil
	}
	res, err := w.buffer.flush(w.run.eng)
	if err != nil {
		return err
	}
	w.rejected.ooo += int64(res.RejectedOOO)
	w.run.admit.record(int64(res.Accepted), int64(res.RejectedOOO), 0, 0)
	w.store.pokeFlush(w.run.eng)
	return nil
}

func (w *metricWriter) write(b *metric.Batch) {
	if w.err != nil {
		return
	}
	tid := w.store.tenantFor(b.Resource(), b.Scope())
	if w.run.eng != nil && w.run.tenant != tid {
		if w.err = w.flush(); w.err != nil {
			return
		}
	}
	if tenantReserved(tid) {
		w.rejected.reserved += int64(b.Len())
		return
	}
	w.run, w.err = w.store.metricRunFor(w.run, tid)
	if w.err != nil {
		return
	}
	if w.run.canBatch() && b.Len() <= metricAppendPoints {
		weights, dropped := w.run.admit.sampleBatch(0, w.store.now(), b.IDs, b.Ts)
		if weights == nil {
			w.appendBuffered(b)
		} else if w.err = w.flush(); w.err == nil {
			w.appendSampled(b, weights, dropped)
		}
		return
	}
	if w.err = w.flush(); w.err != nil {
		return
	}
	w.appendDirect(b)
}

func (w *metricWriter) appendBuffered(b *metric.Batch) {
	if len(w.buffer.ids)+b.Len() > metricAppendPoints {
		if w.err = w.flush(); w.err != nil {
			return
		}
	}
	w.buffer.append(b)
	if len(w.buffer.ids) == metricAppendPoints {
		w.err = w.flush()
	}
}

func (w *metricWriter) appendDirect(b *metric.Batch) {
	run := w.run
	// Admission runs between tenant resolution and the engine: the ingest-rate
	// valve sheds a whole over-budget batch up front; cardinality and in-flight-memory limits
	// are enforced per sample inside the engine (race-free under its lock).
	if !run.admit.allowRate(run.limits, int64(b.Len())*engine.SampleBytes, w.store.now()) {
		w.rejected.rate += int64(b.Len())
		run.admit.addRate(int64(b.Len()))

		return
	}

	weights, dropped := run.admit.sampleBatch(run.sampling, w.store.now(), b.IDs, b.Ts)
	w.appendSampled(b, weights, dropped)
}

func (w *metricWriter) appendSampled(b *metric.Batch, weights []float64, dropped int64) {
	run := w.run
	// Budgeted (lossy) sampling: keep a representative subset and weight each kept sample with a
	// scale factor so the embedder's count/sum/rate stays unbiased. Under budget this window the
	// sampler returns no weights and the batch passes through unchanged (no scale-factor column).
	ids, tss, vals, sf := b.IDs, b.Ts, b.Values, []float64(nil)
	mat := b.Series

	if weights != nil {
		fids := make([]signal.SeriesID, 0, len(weights))
		fts := make([]int64, 0, len(weights))
		fvals := make([]float64, 0, len(weights))
		fsf := make([]float64, 0, len(weights))
		kept := make([]int, 0, len(weights))

		for i, w := range weights {
			if w == 0 {
				continue
			}

			fids = append(fids, b.IDs[i])
			fts = append(fts, b.Ts[i])
			fvals = append(fvals, b.Values[i])
			fsf = append(fsf, w)
			kept = append(kept, i)
		}

		ids, tss, vals, sf = fids, fts, fvals, fsf
		mat = func(j int) signal.Series { return b.Series(kept[j]) }
		w.sampledDropped += dropped
	}

	res, err := run.eng.AppendBatch(ids, tss, vals, sf, mat, engine.AppendLimits{
		MaxSeries:        run.limits.MaxSeries,
		MaxSeriesSoft:    run.limits.MaxSeriesSoft,
		Overflow:         metricOverflow,
		MaxInFlightBytes: run.limits.MaxInFlightBytes,
	})
	if err != nil {
		w.err = err

		return
	}

	w.rejected.ooo += int64(res.RejectedOOO)
	w.rejected.cardinality += int64(res.RejectedCardinality)
	w.rejected.inflight += int64(res.RejectedBytes)
	run.admit.record(int64(res.Accepted), int64(res.RejectedOOO), int64(res.RejectedCardinality), int64(res.RejectedBytes))

	if res.Overflowed > 0 {
		w.overflowed += int64(res.Overflowed)
		run.admit.recordOverflowed(int64(res.Overflowed))
	}

	w.store.pokeFlush(run.eng)
}
