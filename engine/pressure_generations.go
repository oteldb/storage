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
	retain func([]*part)
}

type pressureGeneration struct {
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
	return e.cfg.PressureGenerations == 2 && e.pressurePipelineEnabled() && !e.cfg.SeparateSeries
}

func (e *Engine) pressureRotationBytes() int64 { return e.cfg.ResidentHeadBytes / 2 }

// freezePressureGenerationLocked seals before transferring ownership, so seal failure leaves active state intact.
func (e *Engine) freezePressureGenerationLocked(ctx context.Context) (*pressureGeneration, error) {
	if err := e.establishTenureLocked(ctx); err != nil {
		return nil, err
	}
	if err := e.checkDiskAdmissionLocked(); err != nil {
		return nil, err
	}
	if err := e.fenceLocked(); err != nil {
		return nil, err
	}
	term := e.term()
	if err := e.tenureLocked(term); err != nil {
		return nil, err
	}
	epoch := max(e.flushedEpoch+1, e.cfg.WAL.Epoch())
	sequence := -1
	if err := e.mu.WithReaders(func() (err error) {
		sequence, err = e.cfg.WAL.Seal(epoch + 1)
		return err
	}); err != nil {
		return nil, errors.Wrap(err, "seal wal")
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
	g := &pressureGeneration{started: time.Now(), names: partid.NewSequence(), head: frozen, epoch: epoch, sequence: sequence, term: term}
	floor, _ := g.names.Next()
	g.prefixFloor = e.cfg.Prefix + "/" + floor.String()
	e.pressureGenerations = append(e.pressureGenerations, g)
	e.updateGenerationBuffersLocked()
	return g, nil
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
	defer func() {
		cancel()
		for running > 0 {
			r := <-results
			running--
			e.finishGenerationBuild(r)
		}
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
		if err == nil {
			n, bytes, publishErr := e.publishReadyGenerations(ctx)
			rows += n
			written += bytes
			err = publishErr
		}
		if running == 0 {
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
		case <-ctx.Done():
			if err == nil {
				err = ctx.Err()
			}
			return rows, written, err
		}
	}
}

func (e *Engine) startGenerationBuilds(
	ctx context.Context, force, pressureMode bool, results chan<- generationResult,
) (started int, froze bool, err error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return 0, false, err
	}
	if len(e.pressureGenerations) < 2 && e.generationRotationNeededLocked(force, pressureMode) {
		if _, err := e.freezePressureGenerationLocked(ctx); err != nil {
			return 0, false, err
		}
		froze = true
	}
	if err == nil && ctx.Err() == nil {
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
				input := &headBuildInput{names: g.names, index: g.head.series, retain: func(parts []*part) {
					e.mu.LockState()
					g.head.buildRetained = generationPartBytes(parts)
					e.mu.UnlockState()
				}}
				parts, n, buildErr := e.streamHead(ctx, g.head.samples, input)
				results <- generationResult{generation: g, parts: parts, rows: n, err: buildErr}
			}()
		}
	}

	return started, froze, err
}

func (e *Engine) publishReadyGenerations(ctx context.Context) (rows int, written int64, err error) {
	for len(e.pressureGenerations) > 0 && e.pressureGenerations[0].ready {
		g := e.pressureGenerations[0]
		n, bytes := g.rows, partsBytes(g.parts)
		err = e.publishPressureGeneration(ctx, g)
		if len(e.pressureGenerations) == 0 || e.pressureGenerations[0] != g {
			rows += n
			written += bytes
			if err == nil {
				e.cfg.Obs.Flush.Record(ctx, metricSignal, time.Since(g.started), int64(n), bytes)
			}
		}
		if err != nil {
			return rows, written, err
		}
	}
	return rows, written, nil
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

func (e *Engine) publishPressureGeneration(ctx context.Context, g *pressureGeneration) error {
	if err := e.rememberGenerationWatermarks(ctx, g.head); err != nil {
		return err
	}
	var released sampleArena
	retired := false
	e.mu.Lock()
	defer func() {
		e.mu.Unlock()
		released.release()
		if retired {
			e.notifyPressureProgress()
		}
	}()
	if err := e.fenceLocked(); err != nil {
		return err
	}
	if err := e.tenureLocked(g.term); err != nil {
		return err
	}
	next := e.parts
	for _, p := range g.parts {
		next = appendPart(next, p)
	}
	e.sortPressureParts(next)
	err := e.commitIndexLocked(ctx, next, max(g.epoch, e.flushedEpoch), g.term, nil)
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
	return err
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
	return e.head.bytes > 0 && (force || pressureMode && e.head.ownedIdentityBytes()+e.head.bytes >= e.pressureRotationBytes())
}

func (e *Engine) generationWake() <-chan struct{} {
	e.pressure.mu.Lock()
	defer e.pressure.mu.Unlock()
	if e.pressure.wake == nil {
		e.pressure.wake = make(chan struct{}, 1)
	}
	return e.pressure.wake
}
