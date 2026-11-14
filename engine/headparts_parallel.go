package engine

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	"github.com/go-faster/errors"

	"github.com/oteldb/storage/index/series"
	"github.com/oteldb/storage/internal/buildbudget"
	"github.com/oteldb/storage/internal/memsize"
	"github.com/oteldb/storage/internal/parallel"
	"github.com/oteldb/storage/signal"
)

const maxHeadPartJobs = 64

var (
	errHeadPlanUnavailable = errors.New("engine: bounded parallel head plan unavailable")
	errHeadJobBudget       = errors.New("engine: parallel head job exceeded its reservation")
)

type headPartJob struct {
	entries                                             []headRunEntry
	encoded, materialized, sorting, rows, identityBytes int64
}

type headPartResult struct {
	parts []*part
	rows  int
}

type headPartPlan struct {
	entries []headRunEntry
	jobs    []headPartJob
	shared  int64
	parts   int
}

// reservePlan retains every grant while the planner still owns its buffers.
func (b *headPartBuilder) reservePlan(bytes int64, phase string) error {
	if bytes > b.target {
		return errHeadPlanUnavailable
	}
	if bytes <= b.reserved {
		return nil
	}
	start := time.Now()
	var release func()
	if b.release == nil {
		var err error
		release, err = b.e.cfg.FlushAdmission(b.ctx, bytes)
		if err != nil {
			return err
		}
	} else {
		bytes = min(b.target, max(bytes, b.reserved+max(b.reserved/2, 1<<20)))
		var ok bool
		release, ok = b.e.cfg.TryFlushAdmission(bytes - b.reserved)
		if !ok {
			return errHeadPlanUnavailable
		}
	}
	prior := b.release
	b.release = func() {
		release()
		if prior != nil {
			prior()
		}
	}
	b.reserved = bytes
	b.e.cfg.Obs.Flush.Reservation(b.ctx, phase, bytes, b.target, time.Since(start))
	return nil
}

func (p *headPartPlan) record(b *headPartBuilder) error {
	if len(p.jobs) == maxHeadPartJobs {
		return errHeadPlanUnavailable
	}
	parts := int64(1)
	if limit := b.partRows(); limit > 0 {
		parts = 1 + (b.runRows-1)/int64(limit)
	}
	if parts > int64(maxHeadPartJobs-p.parts) {
		return errHeadPlanUnavailable
	}
	p.parts += int(parts)
	start := len(p.entries)
	p.entries = append(p.entries, b.entries...)
	p.jobs = append(p.jobs, headPartJob{
		entries: p.entries[start:len(p.entries):len(p.entries)],
		encoded: b.encoded, materialized: b.materialized, sorting: b.sorting, rows: b.runRows,
		identityBytes: b.identities.Bytes(),
	})
	b.writer.abort()
	b.writer = nil
	b.closedBloomBytes = buildbudget.Add(b.closedBloomBytes, buildbudget.Mul(parts,
		buildbudget.Add(partIDBloomBytes(len(b.entries)), 2*memsize.Of[*part]())))
	clear(b.entries)
	b.entries = b.entries[:0]
	b.identities.Reset()
	b.encoded, b.materialized, b.sorting, b.runRows = 0, 0, 0, 0
	return b.reservePhase(max(b.reservationFloor, buildbudget.Add(b.baseBytes(), b.identities.Bytes())), "shape")
}

func (e *Engine) streamHeadParallel(ctx context.Context, samples map[signal.SeriesID]*sampleBuf) ([]*part, int, error) {
	plan := headPartPlan{}
	plan.shared = buildbudget.Add(buildbudget.Mul(int64(len(samples)), memsize.Of[headRunEntry]()),
		buildbudget.Mul(maxHeadPartJobs, memsize.Of[headPartJob]()+memsize.Of[headPartResult]()))
	b := headPartBuilder{ctx: ctx, e: e, target: e.flushMemoryBytes(), plan: &plan, orderWorkspace: plan.shared}
	capacity := min(len(samples), max(1, int(b.target/1024)))
	buffers := buildbudget.Mul(int64(capacity), memsize.Of[headRunEntry]()+memsize.Of[uint32]())
	b.reservationFloor = min(b.target, buildbudget.Add(b.baseBytes(), buffers))
	if err := b.reservePhase(buildbudget.Add(b.baseBytes(), buffers), "initial"); err != nil {
		return nil, 0, err
	}
	defer func() {
		if b.writer != nil {
			b.writer.abort()
		}
		b.entries, b.partRefs = nil, nil
		b.identities = series.EncodingBudget{}
		plan.entries, plan.jobs = nil, nil
		b.release()
	}()
	b.entries, b.partRefs = make([]headRunEntry, 0, capacity), make([]uint32, 0, capacity)
	plan.entries = make([]headRunEntry, 0, len(samples))
	plan.jobs = make([]headPartJob, 0, maxHeadPartJobs)
	order, err := e.prepareDetachedOrder(ctx, samples, func(bytes int64) error {
		return b.reservePhase(max(b.reservationFloor, buildbudget.Add(b.baseBytes(), bytes)), "order")
	})
	if err != nil {
		return nil, 0, err
	}
	b.view = order.view
	plan.shared = buildbudget.Add(plan.shared, order.workspace)
	b.orderWorkspace = plan.shared
	if err := b.reserveHeadShape(ctx, samples, order); err != nil {
		return nil, 0, err
	}
	if err := walkIdentityOrder(ctx, order.view, order.runs, func(id signal.SeriesID, ref uint32) error {
		if buf := samples[id]; buf != nil && len(buf.ts) > 0 {
			return b.add(id, ref, buf)
		}
		return nil
	}); err != nil {
		return nil, 0, err
	}
	if err := b.flushRun(); err != nil {
		return nil, 0, err
	}
	// Only the immutable plan survives into encoding; its shared storage is charged once.
	b.entries, b.partRefs = nil, nil
	b.identities = series.EncodingBudget{}
	parts, rows, err := plan.build(ctx, &b)
	if err != nil {
		return nil, 0, err
	}
	expected := 0
	for _, buf := range samples {
		expected += len(buf.ts)
	}
	if rows != expected {
		closePartReaders(parts)
		return nil, 0, errors.Errorf("head has %d samples without identities", expected-rows)
	}
	return parts, rows, nil
}

func (p *headPartPlan) build(ctx context.Context, planner *headPartBuilder) ([]*part, int, error) {
	count := 1
	if len(p.jobs) > 1 {
		if release, ok := planner.e.cfg.TryFlushAdmission(planner.reserved); ok {
			defer release()
			count = 2
		}
	}
	planner.e.cfg.Obs.Flush.BuildPlan(ctx, count, len(p.jobs))
	results := make([]headPartResult, len(p.jobs))
	workers := make([]headPartBuilder, count)
	completed := false
	defer func() {
		for i := range workers {
			if workers[i].writer != nil {
				workers[i].writer.abort()
			}
			if !completed {
				closePartReaders(workers[i].parts)
			}
			workers[i] = headPartBuilder{}
		}
	}()
	workCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	var next atomic.Uint32
	for i := range workers {
		b := &workers[i]
		*b = headPartBuilder{ctx: workCtx, e: planner.e, view: planner.view, target: planner.target,
			reserved: planner.reserved, reservationFloor: planner.reserved, fixedLimit: planner.reserved}
		if i == 0 {
			b.orderWorkspace = p.shared
		}
	}
	var failed sync.Once
	var buildErr error
	parallel.ForEach(count, count, func(i int) {
		if err := p.runJobs(workCtx, &workers[i], &next, results); err != nil {
			failed.Do(func() {
				buildErr = err
				cancel()
			})
		}
	})
	if buildErr != nil {
		return nil, 0, buildErr
	}
	var parts []*part
	var rows int
	for _, result := range results {
		parts = append(parts, result.parts...)
		rows += result.rows
	}
	completed = true
	return parts, rows, nil
}

func (p *headPartPlan) runJobs(ctx context.Context, b *headPartBuilder, next *atomic.Uint32, results []headPartResult) error {
	for {
		j := int(next.Add(1) - 1)
		if j >= len(p.jobs) {
			return nil
		}
		if err := ctx.Err(); err != nil {
			return err
		}
		job := p.jobs[j]
		start, rows := len(b.parts), b.rows
		b.entries = job.entries
		b.encoded, b.materialized, b.sorting, b.runRows = job.encoded, job.materialized, job.sorting, job.rows
		b.fixedIdentityBytes = job.identityBytes
		if err := b.prepareWriter(); err != nil {
			return err
		}
		if err := b.flushRun(); err != nil {
			return err
		}
		results[j] = headPartResult{parts: b.parts[start:len(b.parts):len(b.parts)], rows: b.rows - rows}
	}
}
