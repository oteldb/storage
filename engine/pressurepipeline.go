package engine

import (
	"context"
	"os"
	"sync"
	"sync/atomic"
)

type pressureJob struct {
	done   chan struct{}
	cancel context.CancelFunc
	err    error
}

type pressurePipeline struct {
	generationBuilds atomic.Int32
	beforeBuild      func(uint64)
	mu               sync.Mutex
	job              *pressureJob
	wake             chan struct{}
	progress         chan struct{}
	rotation         chan struct{}
	closed           atomic.Bool
}

func (e *Engine) pressurePipelineEnabled() bool {
	return e.cfg.AsyncPressureFlush && e.cfg.DiskSeries && e.parallel != nil
}

//nolint:contextcheck,nolintlint // Append has no caller context; the engine owns this bounded worker.
func (e *Engine) pipelineResidentPressure() error {
	for {
		if e.pressure.closed.Load() {
			return os.ErrClosed
		}
		e.mu.RLock()
		used := e.head.residentIdentityBytes() + e.head.inFlightBytes() + e.parallel.pending.Load()
		active := e.head.bytes > 0 || len(e.pressureGenerations) > 0
		firstRotation := e.twoPressureGenerations() && len(e.pressureGenerations) == 0 &&
			e.head.ownedIdentityBytes()+e.head.bytes >= e.pressureRotationBytes()
		e.mu.RUnlock()
		if !active || used < e.pressureRotationBytes() {
			return nil
		}
		job := e.startPressureJob()
		if job == nil {
			return os.ErrClosed
		}
		if firstRotation {
			if err := e.waitPressureRotation(job); err != nil {
				return err
			}
			continue
		}
		if used < e.cfg.ResidentHeadBytes {
			return nil
		}
		changed := e.pressureProgress()
		if e.pressure.closed.Load() {
			return os.ErrClosed
		}
		e.mu.RLock()
		used = e.head.residentIdentityBytes() + e.head.inFlightBytes() + e.parallel.pending.Load()
		active = e.head.bytes > 0 || len(e.pressureGenerations) > 0
		e.mu.RUnlock()
		if !active || used < e.cfg.ResidentHeadBytes {
			continue
		}
		select {
		case <-changed:
		case <-job.done:
			if job.err != nil {
				return job.err
			}
		}
	}
}

func (e *Engine) pressureProgress() <-chan struct{} {
	e.pressure.mu.Lock()
	defer e.pressure.mu.Unlock()
	if e.pressure.progress == nil {
		e.pressure.progress = make(chan struct{})
	}
	return e.pressure.progress
}

func (e *Engine) notifyPressureProgress() {
	e.pressure.mu.Lock()
	if e.pressure.progress != nil {
		close(e.pressure.progress)
		e.pressure.progress = nil
	}
	e.pressure.mu.Unlock()
}

func (e *Engine) schedulePressureFlush() {
	if !e.pressurePipelineEnabled() {
		return
	}
	e.mu.RLock()
	needed := (e.head.bytes > 0 || len(e.pressureGenerations) > 0) &&
		e.head.residentIdentityBytes()+e.head.inFlightBytes()+e.parallel.pending.Load() >= e.pressureRotationBytes()
	e.mu.RUnlock()
	if needed {
		e.startPressureJob()
	}
}

//nolint:contextcheck,nolintlint // Append has no caller context; Close owns cancellation and drain.
func (e *Engine) startPressureJob() *pressureJob {
	e.pressure.mu.Lock()
	defer e.pressure.mu.Unlock()
	if e.pressure.closed.Load() {
		return nil
	}
	if job := e.pressure.job; job != nil {
		select {
		case e.pressure.wake <- struct{}{}:
		default:
		}
		return job
	}
	ctx, cancel := context.WithCancel(context.Background())
	job := &pressureJob{done: make(chan struct{}), cancel: cancel}
	e.pressure.job = job
	if e.pressure.wake == nil {
		e.pressure.wake = make(chan struct{}, 1)
	}
	go func() {
		err := e.observeFlush(ctx, e.buildPressureGeneration)
		cancel()
		e.pressure.mu.Lock()
		job.err = err
		e.pressure.job = nil
		close(job.done)
		e.pressure.mu.Unlock()
	}()
	return job
}

func (e *Engine) buildPressureGeneration(ctx context.Context) (int, int64, error) {
	unlock, err := e.admitFlushWork(ctx)
	if unlock == nil {
		return 0, 0, err
	}
	defer unlock()
	// Another flush may have won the slot before this job began.
	e.mu.RLock()
	needed := (e.head.bytes > 0 || len(e.pressureGenerations) > 0) &&
		e.head.residentIdentityBytes()+e.head.inFlightBytes() >= e.pressureRotationBytes()
	e.mu.RUnlock()
	if !needed && !e.twoPressureGenerations() {
		return 0, 0, nil
	}
	if e.twoPressureGenerations() {
		return e.runPressureGenerations(ctx, false)
	}
	return e.flushAdmitted(ctx)
}

func (e *Engine) stopPressurePipeline(ctx context.Context, abort bool) error {
	if !e.pressurePipelineEnabled() {
		return nil
	}
	e.pressure.mu.Lock()
	e.pressure.closed.Store(true)
	job := e.pressure.job
	e.pressure.mu.Unlock()
	e.notifyPressureProgress()
	e.notifyPressureRotation()
	if job == nil {
		return nil
	}
	if abort {
		job.cancel()
	}
	select {
	case <-job.done:
		return nil
	case <-ctx.Done():
		job.cancel()
		return ctx.Err()
	}
}

func (e *Engine) waitPressureRotation(job *pressureJob) error {
	e.pressure.mu.Lock()
	if e.pressure.rotation == nil {
		e.pressure.rotation = make(chan struct{})
	}
	changed := e.pressure.rotation
	e.pressure.mu.Unlock()
	if e.pressure.closed.Load() {
		return os.ErrClosed
	}
	e.mu.RLock()
	needed := len(e.pressureGenerations) == 0 && e.head.bytes > 0 &&
		e.head.ownedIdentityBytes()+e.head.bytes >= e.pressureRotationBytes()
	e.mu.RUnlock()
	if !needed {
		return nil
	}
	select {
	case <-changed:
		return nil
	case <-job.done:
		return job.err
	}
}

func (e *Engine) notifyPressureRotation() {
	e.pressure.mu.Lock()
	if e.pressure.rotation != nil {
		close(e.pressure.rotation)
		e.pressure.rotation = nil
	}
	e.pressure.mu.Unlock()
}
