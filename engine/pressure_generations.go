package engine

import (
	"context"
	"time"

	"github.com/go-faster/errors"

	"github.com/oteldb/storage/index/series"
	"github.com/oteldb/storage/internal/memsize"
	"github.com/oteldb/storage/internal/partid"
	"github.com/oteldb/storage/signal"
)

type headBuildInput struct {
	names  *partid.Sequence
	index  *series.Index
	proofs descriptorProofs
	retain func([]*part)
}

type pressureGeneration struct {
	mergeNames      *partid.Sequence
	names           *partid.Sequence
	prefixFloor     string
	started         time.Time
	head            *head
	epoch           uint64
	sequence        int
	term            uint64
	parts           []*part
	rows            int
	building, ready bool
}

type generationResult struct {
	generation *pressureGeneration
	parts      []*part
	rows       int
	err        error
}

func (e *Engine) twoPressureGenerations() bool {
	return e.cfg.PressureGenerations == 2 && e.pressurePipelineEnabled()
}

func (e *Engine) pressureRotationBytes() int64 {
	limit := e.cfg.ResidentHeadBytes / 2
	if e.fixedHeadRotation() {
		return min(limit, e.cfg.HeadRotationBytes)
	}
	return limit
}

// headPressureLocked caps both aggregate ownership and the active generation.
func (e *Engine) headPressureLocked(used int64) bool {
	if used >= e.cfg.ResidentHeadBytes {
		return true
	}
	return e.fixedHeadRotation() && e.head.ownedIdentityBytes()+e.head.bytes >= e.pressureRotationBytes()
}

func (e *Engine) fixedHeadRotation() bool {
	return e.cfg.HeadRotationBytes > 0 && e.commitOwnedRotation()
}

// freezePressureGenerationLocked seals before transferring ownership, so seal failure leaves active state intact.
func (e *Engine) freezePressureGenerationLocked(ctx context.Context) error {
	if err := e.establishTenureLocked(ctx); err != nil {
		return err
	}
	if err := e.checkDiskAdmissionLocked(); err != nil {
		return err
	}
	if err := e.fenceLocked(); err != nil {
		return err
	}
	term := e.term()
	if err := e.tenureLocked(term); err != nil {
		return err
	}
	epoch := e.flushedEpoch + 1
	sequence := -1
	if e.cfg.WAL != nil {
		epoch = max(epoch, e.cfg.WAL.Epoch())
		if err := e.mu.WithReaders(func() (err error) {
			sequence, err = e.cfg.WAL.Seal(epoch + 1)
			return err
		}); err != nil {
			return errors.Wrap(err, "seal wal")
		}
	} else if n := len(e.pressureGenerations); n > 0 {
		// With no segment to advance the epoch, account for a frozen generation still building.
		epoch = max(epoch, e.pressureGenerations[n-1].epoch+1)
	}
	h := e.head
	frozen := newHeadWithContexts(e.cfg.SeriesContexts, e.cfg.SeriesTemplates)
	h.swapGenerationIdentity(frozen)
	frozen.samples, frozen.memory = h.samples, h.memory
	frozen.bytes, frozen.since = h.bytes, h.since
	frozen.bufferedMin, frozen.bufferedKnown, frozen.newest = h.bufferedMin, h.bufferedKnown, h.newest
	frozen.frozen, h.frozen = h.frozen, frozen
	h.samples, h.memory, h.buffers = make(map[signal.SeriesID]*sampleBuf), sampleArena{}, headBuffers{}
	h.detachedBytes += h.bytes
	h.bytes, h.since, h.bufferedKnown = 0, time.Time{}, false
	h.detachedKnown = false
	mergeNames := partid.NewSequence()
	g := &pressureGeneration{
		mergeNames: mergeNames, names: partid.NewSequence(), started: time.Now(),
		head: frozen, epoch: epoch, sequence: sequence, term: term,
	}
	floor, _ := g.names.Next()
	g.prefixFloor = e.cfg.Prefix + "/" + floor.String()
	e.pressureGenerations = append(e.pressureGenerations, g)
	e.updateGenerationBuffersLocked()
	e.notifyPressureRotation()
	return nil
}

func (e *Engine) updateGenerationBuffersLocked() {
	e.flushing, e.flushingNext = nil, nil
	e.head.detachedMin, e.head.detachedKnown = maxInt64, len(e.pressureGenerations) > 0
	for _, g := range e.pressureGenerations {
		e.head.detachedKnown = e.head.detachedKnown && g.head.bufferedKnown
		e.head.detachedMin = min(e.head.detachedMin, g.head.bufferedMin)
	}
	if len(e.pressureGenerations) > 0 {
		e.flushing = e.pressureGenerations[0].head.samples
	}
	if len(e.pressureGenerations) > 1 {
		e.flushingNext = e.pressureGenerations[1].head.samples
	}
}

// runPressureGenerations owns flushRun and the lifecycle read lease throughout every worker's lifetime.
func (e *Engine) runPressureGenerations(ctx context.Context, force bool) (rows int, written int64, err error) {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	pressureMode := !force
	wake := e.generationWake()
	results := make(chan generationResult, 2)
	running := 0
	var warming <-chan struct{}
	defer func() {
		cancel()
		e.joinPressureWorkers(results, running, warming)
	}()
	for {
		if err == nil {
			started, froze, startErr := e.startGenerationBuilds(ctx, force, pressureMode, results)
			running += started
			if froze {
				force = false
			}
			err = startErr
		}
		if err == nil && warming == nil {
			var n int
			var bytes int64
			n, bytes, warming, err = e.publishReadyGenerations(ctx)
			rows += n
			written += bytes
		}
		if running == 0 && warming == nil {
			if stopErr := errors.Join(err, ctx.Err()); stopErr != nil {
				return rows, written, stopErr
			}
			e.mu.RLock()
			more := e.generationRotationNeededLocked(force, pressureMode)
			e.mu.RUnlock()
			if !more {
				e.reclaimRetired(ctx)
				return rows, written, nil
			}
			continue
		}
		if err != nil {
			cancel()
		}
		select {
		case r := <-results:
			running--
			e.finishGenerationBuild(r)
			if err == nil {
				err = r.err
			}
		case <-wake:
		case <-warming:
			warming = nil
		case <-ctx.Done():
			if err == nil {
				err = ctx.Err()
			}
			return rows, written, err
		}
	}
}

func (e *Engine) joinPressureWorkers(results <-chan generationResult, running int, warming <-chan struct{}) {
	for range running {
		e.finishGenerationBuild(<-results)
	}
	if warming != nil {
		<-warming
	}
}

func (e *Engine) startGenerationBuilds(
	ctx context.Context, force, pressureMode bool, results chan<- generationResult,
) (started int, froze bool, err error) {
	if err := ctx.Err(); err != nil {
		return 0, false, err
	}
	if e.commitOwnedRotation() {
		return e.startCommitOwnedGenerationBuilds(ctx, force, pressureMode, results)
	}
	e.mu.RLock()
	needed := len(e.pressureGenerations) < 2 && e.generationRotationNeededLocked(force, pressureMode)
	for _, g := range e.pressureGenerations {
		needed = needed || !g.building && !g.ready
	}
	e.mu.RUnlock()
	if !needed {
		return 0, false, nil
	}

	e.mu.Lock()
	defer e.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return 0, false, err
	}
	if len(e.pressureGenerations) < 2 && e.generationRotationNeededLocked(force, pressureMode) {
		if err := e.freezePressureGenerationLocked(ctx); err != nil {
			return 0, false, err
		}
		froze = true
	}
	if err == nil {
		started = e.launchGenerationBuildsLocked(ctx, results)
	}

	return started, froze, err
}

// commitOwnedRotation is scoped to the standalone two-generation pipeline.
func (e *Engine) commitOwnedRotation() bool {
	return e.twoPressureGenerations() && e.cfg.Term == nil && e.cfg.WriterID == ""
}

func (e *Engine) startCommitOwnedGenerationBuilds(ctx context.Context, force, pressureMode bool,
	results chan<- generationResult,
) (int, bool, error) {
	e.mu.RLock()
	rotate := len(e.pressureGenerations) < 2 && e.generationRotationNeededLocked(force, pressureMode)
	needsBuilder := false
	for _, g := range e.pressureGenerations {
		needsBuilder = needsBuilder || !g.building && !g.ready
	}
	e.mu.RUnlock()
	if !rotate && !needsBuilder {
		return 0, false, nil
	}
	var froze bool
	if rotate {
		var err error
		froze, err = e.rotateWithCommitOwner(ctx, force, pressureMode)
		if err != nil {
			return 0, false, err
		}
	}
	e.mu.LockState()
	defer e.mu.UnlockState()
	return e.launchGenerationBuildsLocked(ctx, results), froze, ctx.Err()
}

func (e *Engine) rotateWithCommitOwner(ctx context.Context, force, pressureMode bool) (bool, error) {
	// Lifetime stays pinned, while the narrower generation gate prevents new
	// preparations and drains the old ones through publication before WAL seal.
	e.mu.RLockWriter()
	defer e.mu.RUnlockWriter()
	ph := e.parallel
	ph.generation.Lock()
	defer ph.generation.Unlock()
	var froze bool
	err := ph.commits.control(func() (err error) {
		if err := ctx.Err(); err != nil {
			return err
		}
		e.mu.Publish(func() {
			if len(e.pressureGenerations) < 2 && e.generationRotationNeededLocked(force, pressureMode) {
				err = e.freezePressureGenerationLocked(ctx)
				froze = err == nil
			}
		})
		return err
	})
	return froze, err
}

func (e *Engine) launchGenerationBuildsLocked(ctx context.Context, results chan<- generationResult) (started int) {
	if ctx.Err() != nil {
		return 0
	}
	for _, g := range e.pressureGenerations {
		if g.building || g.ready {
			continue
		}
		g.building = true
		started++
		go func() {
			if e.pressure.beforeBuild != nil {
				e.pressure.beforeBuild(g.epoch)
			}
			results <- e.buildFrozenGeneration(ctx, g)
		}()
	}
	return started
}

func (e *Engine) publishReadyGenerations(
	ctx context.Context,
) (rows int, written int64, warming <-chan struct{}, err error) {
	for len(e.pressureGenerations) > 0 && e.pressureGenerations[0].ready {
		g := e.pressureGenerations[0]
		n, bytes := g.rows, partsBytes(g.parts)
		warming, err = e.publishPressureGeneration(ctx, g)
		if len(e.pressureGenerations) == 0 || e.pressureGenerations[0] != g {
			rows += n
			written += bytes
			if err == nil {
				e.cfg.Obs.Flush.Record(ctx, metricSignal, time.Since(g.started), int64(n), bytes)
			}
		}
		if err != nil || warming != nil {
			return rows, written, warming, err
		}
	}
	return rows, written, nil, nil
}

func generationPartBytes(parts []*part) int64 {
	var n int64
	for _, p := range parts {
		n += p.reader.ResidentBytes() + p.idBloom.Load().sizeBytes() + memsize.Of[part]() + 2*memsize.Of[*part]()
		n += memsize.Slice(p.index.ids) + memsize.Slice(p.index.starts)
	}
	return n
}

func (e *Engine) finishGenerationBuild(r generationResult) {
	e.mu.LockState()
	defer e.mu.UnlockState()
	g := r.generation
	g.building = false
	if r.err != nil {
		closePartReaders(r.parts)
		g.head.buildRetained = 0
		return
	}
	g.parts, g.rows, g.ready = r.parts, r.rows, true
}

func (e *Engine) publishPressureGeneration(ctx context.Context, g *pressureGeneration) (<-chan struct{}, error) {
	rememberPublished := e.cfg.SeparateSeries && e.fixedHeadRotation()
	if !rememberPublished {
		if err := e.rememberGenerationWatermarks(ctx, g.head); err != nil {
			return nil, err
		}
	}
	var released sampleArena
	retired := false
	var catalogAt time.Time
	var refresh *retiredWatermarks
	e.mu.Lock()
	defer func() {
		e.mu.Unlock()
		released.release()
		if retired {
			e.notifyPressureProgress()
			if !catalogAt.IsZero() {
				e.cfg.Obs.Flush.HeadPhase(ctx, "catalog_to_retired", time.Since(catalogAt))
			}
		}
		if refresh != nil {
			go e.refreshRetiredWatermarks(context.WithoutCancel(ctx), refresh)
		}
	}()
	if err := e.fenceLocked(); err != nil {
		return nil, err
	}
	if err := e.tenureLocked(g.term); err != nil {
		return nil, err
	}
	next := e.parts
	for _, p := range g.parts {
		next = appendPart(next, p)
	}
	e.sortPressureParts(next)
	err := e.commitIndexLocked(ctx, next, max(g.epoch, e.flushedEpoch), g.term, nil)
	if err == nil {
		catalogAt = time.Now()
		e.cfg.Obs.Flush.HeadPhase(ctx, "frozen_to_published", catalogAt.Sub(g.started))
	}
	if rememberPublished {
		if err == nil && e.compactMetricMetadata {
			refresh = newRetiredWatermarks(e.head.disk.cache, g.head, e.seriesTable.membershipLineage())
			e.head.retiring = refresh
		} else {
			// Preserve the frozen watermark on an ambiguous catalog commit.
			_ = e.mu.WithWriters(func() error {
				return e.rememberGenerationWatermarks(context.WithoutCancel(ctx), g.head)
			})
		}
	}
	// Keep ambiguous candidates queryable and retain WAL exactly as the single-generation path does.
	e.parts, e.flushedEpoch = next, max(g.epoch, e.flushedEpoch)
	if e.recentEnabled() {
		e.populateRecent(g.head.samples)
	}
	e.head.detachedBytes -= g.head.bytes
	for owner := e.head; owner.frozen != nil; owner = owner.frozen {
		if owner.frozen == g.head {
			owner.frozen = g.head.frozen
			break
		}
	}
	e.pressureGenerations[0] = nil
	e.pressureGenerations = e.pressureGenerations[1:]
	e.updateGenerationBuffersLocked()
	released, g.head.memory = g.head.memory, sampleArena{}
	g.head.buildRetained = 0
	g.head.samples, g.head.frozen = nil, nil
	g.head, g.parts = nil, nil
	retired = true
	if err == nil {
		err = e.checkpointLocked(g.sequence)
	}
	if refresh != nil {
		return refresh.done, err
	}
	return nil, err
}

func (e *Engine) releasePressureGenerationsLocked() {
	for _, g := range e.pressureGenerations {
		closePartReaders(g.parts)
		g.head.memory.release()
		g.head.samples, g.head.frozen = nil, nil
		g.head, g.parts = nil, nil
	}
	clear(e.pressureGenerations)
	e.pressureGenerations = nil
	e.flushingNext = nil
}

func (e *Engine) generationRotationNeededLocked(force, pressureMode bool) bool {
	if e.head.bytes == 0 {
		return false
	}
	if force {
		return true
	}
	if !pressureMode {
		return false
	}
	active := e.head.ownedIdentityBytes() + e.head.bytes
	if active >= e.pressureRotationBytes() {
		return true
	}
	if e.fixedHeadRotation() || len(e.pressureGenerations) != 1 || e.pressureGenerations[0].ready {
		return false
	}
	used := e.head.residentIdentityBytes() + e.head.inFlightBytes() + e.parallel.pending.Load()
	return secondPressureRotationNeeded(active, used, e.cfg.ResidentHeadBytes)
}

func secondPressureRotationNeeded(active, used, limit int64) bool {
	return active >= max(limit/4, 1) && used >= limit-limit/8
}

func (e *Engine) buildFrozenGeneration(ctx context.Context, g *pressureGeneration) generationResult {
	active := e.pressure.generationBuilds.Add(1)
	defer e.pressure.generationBuilds.Add(-1)
	e.cfg.Obs.Flush.GenerationStarted(ctx, metricSignal, active > 1)
	input := &headBuildInput{names: g.names, index: g.head.series, proofs: g.head.descriptorProofs, retain: func(parts []*part) {
		e.mu.LockState()
		g.head.buildRetained = generationPartBytes(parts)
		e.mu.UnlockState()
	}}
	parts, rows, err := e.streamHead(ctx, g.head.samples, input)
	return generationResult{generation: g, parts: parts, rows: rows, err: err}
}

func (e *Engine) generationWake() <-chan struct{} {
	e.pressure.mu.Lock()
	defer e.pressure.mu.Unlock()
	if e.pressure.wake == nil {
		e.pressure.wake = make(chan struct{}, 1)
	}
	return e.pressure.wake
}
