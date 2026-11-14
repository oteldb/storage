package obs

import (
	"context"
	"runtime/pprof"
	runtimetrace "runtime/trace"
	"sync"
	"time"

	"github.com/go-faster/sdk/zctx"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/trace"
	"go.uber.org/zap"

	"github.com/oteldb/storage/internal/memlimit"
)

const memoryWaitWarningAfter = time.Second

// Memory instruments contended build, merge and decode admission.
type Memory struct {
	BuildFlush, BuildMerge, Merge, Decode memlimit.WaitObserver

	waiters, waitingBytes, slowWaiters metric.Int64UpDownCounter
	slowWaits                          metric.Int64Counter
	duration                           metric.Float64Histogram
}

func newMemory(meter metric.Meter, logger *zap.Logger, tracer trace.Tracer) (*Memory, error) {
	b := imb{m: meter}
	m := &Memory{
		slowWaits: b.counter("storage.memory.admission.slow_waits", "admissions still waiting after one second", "{request}"),
		duration:  b.f64hist("storage.memory.admission.wait.duration", "contended memory admission duration"),
	}
	if b.err != nil {
		return nil, b.err
	}
	m.waiters, b.err = meter.Int64UpDownCounter("storage.memory.admission.waiters",
		metric.WithDescription("contended admissions still waiting"))
	if b.err != nil {
		return nil, b.err
	}
	m.waitingBytes, b.err = meter.Int64UpDownCounter("storage.memory.admission.waiting_bytes", metric.WithUnit("By"),
		metric.WithDescription("effective reservation bytes waiting for admission"))
	if b.err != nil {
		return nil, b.err
	}
	m.slowWaiters, b.err = meter.Int64UpDownCounter("storage.memory.admission.slow_waiters",
		metric.WithDescription("admissions still waiting after one second"))
	if b.err != nil {
		return nil, b.err
	}
	m.BuildFlush = m.observer("build", "flush", logger, tracer)
	m.BuildMerge = m.observer("build", "merge", logger, tracer)
	m.Merge = m.observer("merge", "merge", logger, tracer)
	m.Decode = m.observer("decode", "query", logger, tracer)
	return m, b.err
}

type memoryWaitLogs struct {
	mu                sync.Mutex
	waiting, finished time.Time
}

type memoryWaitLog struct {
	logger  *zap.Logger
	request memlimit.Wait
	started time.Time
	rate    *memoryWaitLogs
}

func (w memoryWaitLog) warn(ctx context.Context, message, outcome string, finished bool) {
	if !w.logger.Core().Enabled(zap.WarnLevel) || !w.rate.allow(finished) {
		return
	}
	state := w.request.State()
	fields := []zap.Field{
		zap.Int64("requested_bytes", w.request.RequestedBytes), zap.Int64("reserved_bytes", w.request.ReservedBytes),
		zap.Int64("limit_bytes", state.LimitBytes), zap.Int64("used_bytes", state.UsedBytes),
		zap.Int64("free_bytes", max(state.LimitBytes-state.UsedBytes, 0)), zap.Int("waiters", state.Waiters),
		zap.Int64("head_waiter_bytes", state.HeadBytes),
		zap.Duration("elapsed", time.Since(w.started)),
	}
	if outcome != "" {
		fields = append(fields, zap.String("outcome", outcome))
	}
	if deadline, ok := ctx.Deadline(); ok {
		fields = append(fields, zap.Time("deadline", deadline), zap.Duration("remaining", time.Until(deadline)))
	}
	if err := ctx.Err(); err != nil {
		fields = append(fields, zap.NamedError("context_error", err))
	}
	zctx.From(zctx.Base(ctx, w.logger)).Warn(message, fields...)
}

func (l *memoryWaitLogs) allow(finished bool) bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	last := &l.waiting
	if finished {
		last = &l.finished
	}
	now := time.Now()
	if !last.IsZero() && now.Sub(*last) < 30*time.Second {
		return false
	}
	*last = now
	return true
}

func (m *Memory) observer(pool, operation string, logger *zap.Logger, tracer trace.Tracer) memlimit.WaitObserver {
	attrs := []attribute.KeyValue{attribute.String("pool", pool), attribute.String("operation", operation)}
	labels := metric.WithAttributes(attrs...)
	profileLabels := pprof.Labels("storage.memory.pool", pool, "storage.memory.operation", operation)
	logger = logger.With(zap.String("pool", pool), zap.String("operation", operation))
	var logs memoryWaitLogs
	return func(ctx context.Context, request memlimit.Wait, wait func() (bool, error)) (forced bool, err error) {
		started := time.Now()
		ctx, span := tracer.Start(ctx, "storage.memory.wait", trace.WithAttributes(append(attrs,
			attribute.Int64("storage.memory.requested_bytes", request.RequestedBytes),
			attribute.Int64("storage.memory.reserved_bytes", request.ReservedBytes))...))
		defer span.End()
		if span.IsRecording() {
			state := request.State()
			span.SetAttributes(attribute.Int64("storage.memory.limit_bytes", state.LimitBytes),
				attribute.Int64("storage.memory.used_bytes", state.UsedBytes), attribute.Int("storage.memory.waiters", state.Waiters),
				attribute.Int64("storage.memory.head_waiter_bytes", state.HeadBytes))
		}
		region := runtimetrace.StartRegion(ctx, "storage.memory.wait")
		defer region.End()
		if runtimetrace.IsEnabled() {
			runtimetrace.Log(ctx, "storage.memory.wait", pool+"/"+operation)
		}
		m.waiters.Add(ctx, 1, labels)
		m.waitingBytes.Add(ctx, request.ReservedBytes, labels)
		slow := false
		completed := false
		log := memoryWaitLog{logger: logger, request: request, started: started, rate: &logs}
		warned := make(chan struct{})
		timer := time.AfterFunc(memoryWaitWarningAfter, func() {
			defer close(warned)
			slow = true
			m.slowWaits.Add(ctx, 1, labels)
			m.slowWaiters.Add(ctx, 1, labels)
			span.AddEvent("storage.memory.wait.slow")
			span.SetAttributes(attribute.Bool("storage.memory.slow", true))
			log.warn(ctx, "memory admission wait is slow", "", false)
		})
		defer func() {
			if !timer.Stop() {
				<-warned
			}
			outcome := "admitted"
			switch {
			case !completed:
				outcome = "failed"
			case err != nil:
				outcome = "failed"
				if ctx.Err() != nil {
					outcome = "canceled"
				}
			case forced:
				outcome = "forced"
			}
			m.waiters.Add(ctx, -1, labels)
			m.waitingBytes.Add(ctx, -request.ReservedBytes, labels)
			m.duration.Record(ctx, time.Since(started).Seconds(), labels, metric.WithAttributes(attribute.String("outcome", outcome)))
			span.SetAttributes(attribute.String("storage.memory.outcome", outcome))
			if err != nil {
				span.RecordError(err)
				span.SetStatus(codes.Error, err.Error())
			}
			if slow {
				m.slowWaiters.Add(ctx, -1, labels)
				log.warn(ctx, "memory admission wait finished", outcome, true)
			}
		}()
		pprof.Do(ctx, profileLabels, func(context.Context) { forced, err = wait() })
		completed = true
		return forced, err
	}
}
