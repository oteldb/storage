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
	mu     sync.Mutex
	job    *pressureJob
	closed atomic.Bool
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
		active := e.head.bytes > 0
		e.mu.RUnlock()
		if !active || used < e.cfg.ResidentHeadBytes/2 {
			return nil
		}
		e.pressure.mu.Lock()
		if e.pressure.closed.Load() {
			e.pressure.mu.Unlock()
			return os.ErrClosed
		}
		job := e.pressure.job
		if job == nil {
			ctx, cancel := context.WithCancel(context.Background())
			job = &pressureJob{done: make(chan struct{}), cancel: cancel}
			e.pressure.job = job
			go func() {
				err := e.observeFlush(ctx, e.buildPressureGeneration)
				cancel()
				e.pressure.mu.Lock()
				job.err = err
				e.pressure.job = nil
				close(job.done)
				e.pressure.mu.Unlock()
			}()
		}
		e.pressure.mu.Unlock()
		if used < e.cfg.ResidentHeadBytes {
			return nil
		}
		<-job.done
		if job.err != nil {
			return job.err
		}
	}
}

func (e *Engine) buildPressureGeneration(ctx context.Context) (int, int64, error) {
	unlock, err := e.admitFlushWork(ctx)
	if unlock == nil {
		return 0, 0, err
	}
	defer unlock()
	// Another flush may have won the slot before this job began.
	e.mu.RLock()
	needed := e.head.bytes > 0 && e.head.residentIdentityBytes()+e.head.inFlightBytes() >= e.cfg.ResidentHeadBytes/2
	e.mu.RUnlock()
	if !needed {
		return 0, 0, nil
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
