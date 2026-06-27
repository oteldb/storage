package storage

import (
	"context"
	"sync"

	"github.com/go-faster/sdk/zctx"
	"go.opentelemetry.io/otel/attribute"
	"go.uber.org/zap"

	"github.com/oteldb/storage/signal"
	"github.com/oteldb/storage/tenant"
)

// AdmissionStats are the per-tenant admission counters (the overload-control meta-metrics): how
// many samples were accepted and how many were shed, by reason. They make it observable which
// valve tripped under load. Counts are cumulative since the store opened.
type AdmissionStats struct {
	Accepted            int64
	RejectedOOO         int64 // out of the out-of-order window
	RejectedRate        int64 // over IngestBytesPerSecond
	RejectedCardinality int64 // over MaxSeries
	RejectedInFlight    int64 // over MaxInFlightBytes
	// SampledDropped counts points dropped by budgeted (lossy) sampling. Unlike the Rejected
	// counters these are NOT failures — the kept samples carry a scale factor that represents
	// them — so they count toward Accepted, not Rejected.
	SampledDropped int64
}

// Rejected returns the total shed across all reasons.
func (s AdmissionStats) Rejected() int64 {
	return s.RejectedOOO + s.RejectedRate + s.RejectedCardinality + s.RejectedInFlight
}

// tokenBucket is a byte-rate limiter: tokens (bytes) accrue at ratePerSec up to burst, and a
// request of n bytes is admitted only if n tokens are available. It is the per-tenant ingest-rate
// valve. Safe for concurrent use. The clock is injected (unix nanoseconds) so the facade can pass
// a test clock; production passes time.Now().UnixNano().
type tokenBucket struct {
	mu        sync.Mutex
	ratePerNs float64
	burst     float64
	tokens    float64
	last      int64 // unix-nano of the last refill
}

// reconfigure updates the rate/burst from the current policy (hot-reload) without discarding the
// accrued tokens, and lazily seeds the clock on first use.
func (b *tokenBucket) reconfigure(ratePerSec, burst float64, nowNs int64) {
	if b.last == 0 {
		b.last = nowNs
		b.tokens = burst
	}

	b.ratePerNs = ratePerSec / 1e9
	b.burst = burst
}

// allow reports whether n bytes fit the budget at nowNs, consuming them when they do. A
// non-positive rate means "unlimited" and always admits.
func (b *tokenBucket) allow(n float64, nowNs int64, unlimited bool) bool {
	b.mu.Lock()
	defer b.mu.Unlock()

	if unlimited {
		return true
	}

	if d := nowNs - b.last; d > 0 {
		b.tokens = min(b.burst, b.tokens+float64(d)*b.ratePerNs)
		b.last = nowNs
	}

	if b.tokens >= n {
		b.tokens -= n

		return true
	}

	return false
}

// tenantAdmission is the per-tenant admission state: the ingest-rate bucket plus the cumulative
// counters. The engine enforces cardinality/in-flight limits itself (race-free under its lock);
// this struct owns the rate valve and aggregates every reason for AdmissionStats.
type tenantAdmission struct {
	bucket tokenBucket

	mu                  sync.Mutex
	accepted            int64
	rejectedOOO         int64
	rejectedRate        int64
	rejectedCardinality int64
	rejectedInFlight    int64
	sampledDropped      int64

	// Budgeted-sampling window state (guarded by smu): the sampler adapts the scale factor each
	// 1-second window from the prior window's observed rate, so the kept set tracks the budget.
	smu      sync.Mutex
	winStart int64 // unix-nano of the current window's start (0 ⇒ uninitialized)
	winCount int64 // rows observed in the current window
	winSF    int64 // scale factor applied this window (≥1), derived from the prior window
}

// sample decides a batch's budgeted sampling. When sampling is active it returns a per-point
// weight slice — a kept point gets its scale factor (≥1), a dropped point gets 0 — and active=true.
// When the tenant is under budget this window it returns (nil, false): every point is kept at
// weight 1, so the caller takes the allocation-free pass-through. Sampling is deterministic by
// (series, ts) — the same point is consistently kept or dropped — and the scale factor adapts each
// 1-second window from the prior window's observed rate, so the kept rate tracks the budget while
// the weights keep counts/sums unbiased.
func (a *tenantAdmission) sample(budgetPerSec, nowNs int64, ids []signal.SeriesID, ts []int64) (weights []float64, active bool) {
	a.smu.Lock()
	defer a.smu.Unlock()

	if a.winStart == 0 || nowNs-a.winStart >= 1e9 {
		a.winSF = 1
		if budgetPerSec > 0 && a.winCount > budgetPerSec {
			a.winSF = (a.winCount + budgetPerSec - 1) / budgetPerSec // scale factor = ceil of observed over budget
		}

		a.winStart, a.winCount = nowNs, 0
	}

	a.winCount += int64(len(ids)) // every observed row feeds the next window's rate estimate

	sf := a.winSF
	if sf <= 1 {
		return nil, false // under budget this window: keep everything, no scale factors
	}

	weights = make([]float64, len(ids))
	for i := range ids {
		if sampleHash(ids[i], ts[i])%uint64(sf) == 0 {
			weights[i] = float64(sf) // kept; its weight represents the sf-1 dropped peers
		}
	}

	return weights, true
}

// sampleHash is a deterministic mix of a series id and timestamp, so a given (series, ts) is
// consistently kept or dropped across batches and nodes.
func sampleHash(id signal.SeriesID, ts int64) uint64 {
	const k = 0x9E3779B97F4A7C15

	h := id.Hi * k
	h = (h ^ (id.Lo + k + (h << 6) + (h >> 2)))
	h = (h ^ (uint64(ts) + k + (h << 6) + (h >> 2)))

	return h
}

func (a *tenantAdmission) addRate(n int64) { a.add(&a.rejectedRate, n) }

// recordSampledDropped accounts the sampled-out points: they count toward Accepted (the producer
// must not retry — a kept peer's scale factor represents them) and are also tracked as the sampled
// subset for observability. The kept points are accounted by the normal record path via the engine
// result.
func (a *tenantAdmission) recordSampledDropped(n int64) {
	a.mu.Lock()
	defer a.mu.Unlock()

	a.accepted += n
	a.sampledDropped += n
}

func (a *tenantAdmission) record(accepted, ooo, cardinality, inflight int64) {
	a.mu.Lock()
	defer a.mu.Unlock()

	a.accepted += accepted
	a.rejectedOOO += ooo
	a.rejectedCardinality += cardinality
	a.rejectedInFlight += inflight
}

func (a *tenantAdmission) add(p *int64, n int64) {
	a.mu.Lock()
	defer a.mu.Unlock()

	*p += n
}

func (a *tenantAdmission) stats() AdmissionStats {
	a.mu.Lock()
	defer a.mu.Unlock()

	return AdmissionStats{
		Accepted:            a.accepted,
		RejectedOOO:         a.rejectedOOO,
		RejectedRate:        a.rejectedRate,
		RejectedCardinality: a.rejectedCardinality,
		RejectedInFlight:    a.rejectedInFlight,
		SampledDropped:      a.sampledDropped,
	}
}

// admissionFor returns the tenant's admission state (keyed by the normalized tenant id, like the
// engine maps), creating it on first use.
func (s *Storage) admissionFor(tid signal.TenantID) *tenantAdmission {
	key := s.normalizeTenant(tid)

	s.admitMu.Lock()
	defer s.admitMu.Unlock()

	a := s.admit[key]
	if a == nil {
		a = &tenantAdmission{}
		s.admit[key] = a
	}

	return a
}

// allowRate consumes the rate budget for a batch of nBytes against the tenant's policy, returning
// whether the batch is admitted. An unset (zero) IngestBytesPerSecond is unlimited.
func (a *tenantAdmission) allowRate(limits tenant.Limits, nBytes, nowNs int64) bool {
	unlimited := limits.IngestBytesPerSecond <= 0
	if !unlimited {
		// Burst is one second of budget, so a steady producer at the limit is never shed and a
		// spike up to one second's worth is absorbed.
		a.bucket.reconfigure(float64(limits.IngestBytesPerSecond), float64(limits.IngestBytesPerSecond), nowNs)
	}

	return a.bucket.allow(float64(nBytes), nowNs, unlimited)
}

// AdmissionStats returns the cumulative admission counters for a tenant (the overload-control
// meta-metrics). It is safe to call concurrently; an unknown tenant returns the zero value.
func (s *Storage) AdmissionStats(tid signal.TenantID) AdmissionStats {
	s.admitMu.Lock()
	a := s.admit[s.normalizeTenant(tid)]
	s.admitMu.Unlock()

	if a == nil {
		return AdmissionStats{}
	}

	return a.stats()
}

// writeSpan starts a span for a public Write* call and returns the child ctx and a finish closure
// that stamps the result (accepted/rejected counts, any error) on the span and ends it. With the
// no-op tracer it is free. Use: ctx, finish := s.writeSpan(ctx, "…"); defer finish(&acc, &err).
func (s *Storage) writeSpan(ctx context.Context, name string) (context.Context, func(*Accepted, *error)) {
	ctx = s.obs.Base(ctx)
	ctx, span := s.obs.Tracer.Start(ctx, name) //nolint:spancheck // the returned finish() closure ends the span

	log := zctx.From(ctx)
	log.Debug("write start", zap.String("op", name))

	return ctx, func(acc *Accepted, err *error) { //nolint:spancheck // span.End is called here, in the returned closure
		span.SetAttributes(
			attribute.Int64("storage.accepted", acc.Accepted),
			attribute.Int64("storage.rejected", acc.Rejected),
		)

		if *err != nil {
			span.RecordError(*err)
			log.Error("write failed", zap.String("op", name), zap.Error(*err))
		} else {
			log.Debug("write done",
				zap.String("op", name),
				zap.Int64("accepted", acc.Accepted), zap.Int64("rejected", acc.Rejected))
		}

		span.End()
	}
}

// emitAdmission records a write's admission outcome to the OTel meta-metrics (DESIGN §8a/§16): the
// accepted count, the per-reason rejections, and the sampled-dropped count, all tagged with the
// signal. It is called once per write (bulk), so it never touches the per-point hot path; with the
// no-op meter it is a no-op.
func (s *Storage) emitAdmission(ctx context.Context, sig signal.Signal, accepted int64, rej rejectTally, sampled int64) {
	name := sig.String()
	a := s.obs.Admission
	a.Accepted(ctx, accepted, name)
	a.Rejected(ctx, rej.ooo, name, reasonOutOfOrder)
	a.Rejected(ctx, rej.rate, name, reasonRateLimit)
	a.Rejected(ctx, rej.cardinality, name, reasonMaxSeries)
	a.Rejected(ctx, rej.inflight, name, reasonMaxInFlightBytes)
	a.SampledDropped(ctx, sampled, name)

	// Shedding is the overload/backpressure event — log it (Warn) so operators see it without
	// scraping metrics. Only fires when something was actually rejected, so it stays coarse.
	if total := rej.ooo + rej.rate + rej.cardinality + rej.inflight; total > 0 {
		zctx.From(ctx).Warn("admission shed writes",
			zap.String("signal", name), zap.Int64("rejected", total),
			zap.Int64(reasonOutOfOrder, rej.ooo), zap.Int64(reasonRateLimit, rej.rate),
			zap.Int64(reasonMaxSeries, rej.cardinality), zap.Int64(reasonMaxInFlightBytes, rej.inflight))
	}
}
