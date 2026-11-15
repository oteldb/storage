package engine

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	"github.com/go-faster/errors"

	"github.com/oteldb/storage/block"
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
	privateBytes, retainedBytes                         int64
}

type headPartResult struct {
	parts []*part
	rows  int
}

type headPartPlan struct {
	entries        []headRunEntry
	jobs           []headPartJob
	shared         int64
	parts          int
	retained       int64
	workerRetained int64
	balanced       bool
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
	private := b.estimate(len(b.entries), 0, 0, 0, 0)
	private -= b.orderWorkspace + b.closedBloomBytes + memsize.Slice(b.entries) + memsize.Slice(b.partRefs)
	private -= 2 * memsize.Slice(b.parts)
	private = buildbudget.Add(private, p.workerRetained)
	retained := buildbudget.Mul(parts, buildbudget.Add(partIDBloomBytes(len(b.entries)), 64<<10))
	p.retained = buildbudget.Add(p.retained, retained)
	start := len(p.entries)
	p.entries = append(p.entries, b.entries...)
	p.jobs = append(p.jobs, headPartJob{
		entries: p.entries[start:len(p.entries):len(p.entries)],
		encoded: b.encoded, materialized: b.materialized, sorting: b.sorting, rows: b.runRows,
		identityBytes: b.identities.Bytes(), privateBytes: private, retainedBytes: retained,
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
	plan.workerRetained = buildbudget.Add(block.MaxStreamBuildScratchBytes, buildbudget.Mul(int64(capacity), 8))
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
	if err := plan.balance(&b); err != nil {
		return nil, 0, err
	}
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

type headPartAdmission struct {
	largest, workers         int
	primary, secondary, held int64
	release                  func()
}

func (p *headPartPlan) admit(planner *headPartBuilder) (headPartAdmission, error) {
	largest, second := p.largestJobs()
	a := headPartAdmission{largest: largest, workers: 1, held: planner.reserved}
	shared := buildbudget.Add(p.shared, p.retained)
	if largest >= 0 {
		a.primary = p.jobs[largest].privateBytes
	}
	if second >= 0 {
		a.secondary = p.jobs[second].privateBytes
		need := buildbudget.Add(shared, buildbudget.Add(a.primary, a.secondary))
		if need <= a.held {
			a.workers = 2
		} else if release, ok := planner.e.cfg.TryFlushAdmission(need - a.held); ok {
			a.release, a.held, a.workers = release, need, 2
		}
	}
	if a.workers == 1 {
		need := buildbudget.Add(shared, a.primary)
		if need > a.held {
			var ok bool
			a.release, ok = planner.e.cfg.TryFlushAdmission(need - a.held)
			if !ok {
				return headPartAdmission{}, errHeadPlanUnavailable
			}
			a.held = need
		}
	}
	return a, nil
}

func (p *headPartPlan) build(ctx context.Context, planner *headPartBuilder) ([]*part, int, error) {
	admission, err := p.admit(planner)
	if err != nil {
		return nil, 0, err
	}
	if admission.release != nil {
		defer admission.release()
	}
	planner.e.cfg.Obs.Flush.BuildMemory(ctx, "shared", p.shared)
	planner.e.cfg.Obs.Flush.BuildMemory(ctx, "retained", p.retained)
	planner.e.cfg.Obs.Flush.BuildMemory(ctx, "held", admission.held)
	planner.e.cfg.Obs.Flush.BuildMemory(ctx, "additional", admission.held-planner.reserved)
	for _, job := range p.jobs {
		planner.e.cfg.Obs.Flush.BuildMemory(ctx, "job", job.privateBytes)
	}
	planner.e.cfg.Obs.Flush.BuildPlan(ctx, admission.workers, len(p.jobs))
	results := make([]headPartResult, len(p.jobs))
	workers := make([]headPartBuilder, admission.workers)
	completed := false
	defer func() {
		for i := range workers {
			if workers[i].writer != nil {
				workers[i].writer.abort()
			}
			if !completed {
				closePartReaders(workers[i].parts)
			}
			planner.e.cfg.Obs.Flush.BuildMemory(ctx, "checked_private", workers[i].checkedPrivate)
			planner.e.cfg.Obs.Flush.BuildMemory(ctx, "tracked_private", workers[i].trackedPrivate)
			workers[i] = headPartBuilder{}
		}
	}()
	workCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	var next atomic.Uint32
	for i := range workers {
		b := &workers[i]
		*b = headPartBuilder{ctx: workCtx, e: planner.e, view: planner.view, target: planner.target,
			reserved: planner.reserved, reservationFloor: planner.reserved, fixedLimit: admission.primary}
		if i == 1 {
			b.fixedLimit = admission.secondary
		}
	}
	var failed sync.Once
	var buildErr error
	parallel.ForEach(admission.workers, admission.workers, func(i int) {
		err := error(nil)
		if i == 0 && admission.largest >= 0 {
			err = p.runJob(&workers[i], admission.largest, results)
		}
		if err == nil {
			err = p.runJobs(workCtx, &workers[i], &next, results, admission.largest)
		}
		if err != nil {
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
	if p.balanced && len(parts) > p.parts {
		return nil, 0, errHeadJobBudget
	}
	completed = true
	return parts, rows, nil
}

func (p *headPartPlan) runJobs(ctx context.Context, b *headPartBuilder, next *atomic.Uint32, results []headPartResult, skip int) error {
	for {
		j := int(next.Add(1) - 1)
		if j >= len(p.jobs) {
			return nil
		}
		if j == skip {
			continue
		}
		if err := ctx.Err(); err != nil {
			return err
		}
		if err := p.runJob(b, j, results); err != nil {
			return err
		}
	}
}

func (p *headPartPlan) runJob(b *headPartBuilder, j int, results []headPartResult) error {
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
	retained := int64(0)
	for _, out := range b.parts[start:] {
		n := out.reader.ResidentBytes() + out.idBloom.Load().sizeBytes() + memsize.Of[part]() + 2*memsize.Of[*part]()
		n += memsize.Slice(out.index.ids) + memsize.Slice(out.index.starts)
		retained = buildbudget.Add(retained, n)
	}
	if retained > job.retainedBytes {
		return errHeadJobBudget
	}
	results[j] = headPartResult{parts: b.parts[start:len(b.parts):len(b.parts)], rows: b.rows - rows}
	return nil
}

func (p *headPartPlan) largestJobs() (first, second int) {
	first, second = -1, -1
	for i := range p.jobs {
		if first < 0 || p.jobs[i].privateBytes > p.jobs[first].privateBytes {
			second, first = first, i
		} else if second < 0 || p.jobs[i].privateBytes > p.jobs[second].privateBytes {
			second = i
		}
	}
	return first, second
}
