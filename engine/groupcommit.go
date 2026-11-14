package engine

import "sync"

type preparedCommit struct {
	done, owner bool
	err         error
}

type commitQueue struct {
	mu      sync.Mutex
	changed *sync.Cond
	pending [maxPreparedBatches]*preparedBatch
	n       int
	active  bool
}

var commitBuffers = make(chan []byte, maxPreparedBatches)

func (e *Engine) commitPrepared(p *preparedBatch) error {
	if !e.cfg.WAL.SyncOnWrite() {
		e.parallel.walMu.Lock()
		defer e.parallel.walMu.Unlock()
		return e.writePreparedGroup([]*preparedBatch{p}, p.batch.frames.Len())
	}
	q := &e.parallel.commits
	q.mu.Lock()
	if q.changed == nil {
		q.changed = sync.NewCond(&q.mu)
	}
	for q.n == len(q.pending) {
		q.changed.Wait()
	}
	p.commit.done, p.commit.owner, p.commit.err = false, !q.active, nil
	q.active = true
	q.pending[q.n] = p
	q.n++
	if h := e.parallel.hooks; h != nil && h.queued != nil {
		h.queued()
	}
	for !p.commit.done && !p.commit.owner {
		q.changed.Wait()
	}
	if p.commit.done {
		err := p.commit.err
		q.mu.Unlock()
		return err
	}

	// Ready callers share one WAL write without delaying the first caller to form a group.
	var group [maxPreparedBatches]*preparedBatch
	n, size := 0, 0
	for n < q.n {
		frames := q.pending[n].batch.frames.Len()
		if n > 0 && size+frames > maxPreparedFrameBytes {
			break
		}
		group[n] = q.pending[n]
		n++
		size += frames
	}
	copy(q.pending[:], q.pending[n:q.n])
	clear(q.pending[q.n-n : q.n])
	q.n -= n
	q.changed.Broadcast()
	q.mu.Unlock()

	err := e.writePreparedGroup(group[:n], size)

	q.mu.Lock()
	for _, member := range group[:n] {
		member.commit.err, member.commit.done, member.commit.owner = err, true, false
	}
	if q.n == 0 {
		q.active = false
	} else {
		q.pending[0].commit.owner = true
	}
	q.changed.Broadcast()
	q.mu.Unlock()
	return err
}

func (e *Engine) writePreparedGroup(group []*preparedBatch, size int) error {
	frames := group[0].batch.frames.Bytes()
	var buffer []byte
	if len(group) > 1 {
		select {
		case buffer = <-commitBuffers:
		default:
		}
		buffer = resize(buffer, size)[:0]
		for _, p := range group {
			buffer = append(buffer, p.batch.frames.Bytes()...)
		}
		frames = buffer
		defer func() {
			select {
			case commitBuffers <- buffer[:0]:
			default:
			}
		}()
	}
	if h := e.parallel.hooks; h != nil && h.beforeWAL != nil {
		h.beforeWAL()
	}
	if err := e.cfg.WAL.WriteFrames(frames); err != nil {
		return err
	}
	if h := e.parallel.hooks; h != nil && h.beforePublish != nil {
		h.beforePublish()
	}
	e.mu.Publish(func() {
		for _, p := range group {
			p.publishTails(e.head)
			e.parallel.pending.Add(-int64(p.batch.samples) * SampleBytes)
			e.parallel.pendingSeries.Add(-int64(p.batch.registered))
		}
	})
	return nil
}
