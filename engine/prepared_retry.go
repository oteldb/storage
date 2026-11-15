package engine

import "github.com/oteldb/storage/signal"

//nolint:contextcheck,nolintlint // Append has no caller context; pressure waits use engine lifetime.
func (e *Engine) retryPrepared(
	ids []signal.SeriesID, ts []int64, values, sf []float64, materialize func(int) signal.Series,
	limits AppendLimits, p *preparedBatch,
) (AppendResult, preparedOutcome, string, error) {
	retried := ""
	for attempt := 0; ; attempt++ {
		res, outcome, err := e.appendKnownPrepared(ids, ts, values, sf, materialize, limits, p)
		e.recordPreparedAttempt(outcome, err)
		if outcome == preparedDone {
			reason := "first"
			if retried != "" {
				reason = "retry_" + retried
			}
			if err != nil {
				reason = "error"
			}
			return res, outcome, reason, err
		}
		reason := outcome.reason()
		if outcome == preparedWorkLimit {
			return AppendResult{}, outcome, reason, nil
		}
		if attempt == 1 {
			return AppendResult{}, outcome, "exhausted_" + reason, nil
		}
		retried = reason
		// Transient misses precede materialization and WAL submission.
		p.resetPrepared(ids)
		if outcome == preparedPressure {
			if err := e.flushResidentHead(); err != nil {
				return AppendResult{}, preparedDone, "wait_error", err
			}
		}
	}
}
