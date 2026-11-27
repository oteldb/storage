package engine

import "sync/atomic"

// Prepared batches own their slots in the bounded preparation pool until acknowledgement.
// The queue transfers them to one WAL owner in the order of successful submissions.
type readyCommitQueue struct {
	pending atomic.Pointer[preparedBatch]
	owner   atomic.Bool
}

func (e *Engine) commitReady(p *preparedBatch) error {
	q := &e.parallel.ready
	if p.commit.ack == nil {
		p.commit.ack = make(chan error, 1)
	}
	for {
		head := q.pending.Load()
		p.commit.next = head
		if q.pending.CompareAndSwap(head, p) {
			break
		}
	}
	if h := e.parallel.hooks; h != nil && h.queued != nil {
		h.queued()
	}
	if q.owner.CompareAndSwap(false, true) {
		go e.drainReadyCommits(q)
	}
	return <-p.commit.ack
}

func (e *Engine) drainReadyCommits(q *readyCommitQueue) {
	var group [maxPreparedBatches]*preparedBatch
	for {
		ready := reverseReadyCommits(q.pending.Swap(nil))
		if ready == nil {
			q.owner.Store(false)
			// A producer may have queued its batch just before the owner released the slot.
			if q.pending.Load() == nil || !q.owner.CompareAndSwap(false, true) {
				return
			}
			continue
		}
		for ready != nil {
			n, size := 0, 0
			for ready != nil && n < len(group) {
				frames := ready.batch.frames.Len()
				if n > 0 && size+frames > maxPreparedFrameBytes {
					break
				}
				group[n] = ready
				n++
				size += frames
				next := ready.commit.next
				ready.commit.next = nil
				ready = next
			}
			err := e.writePreparedGroup(group[:n], size)
			for i, p := range group[:n] {
				group[i] = nil
				p.commit.ack <- err
			}
		}
	}
}

// A detached stack has one owner. Reversing it restores submission order.
func reverseReadyCommits(p *preparedBatch) (ordered *preparedBatch) {
	for p != nil {
		next := p.commit.next
		p.commit.next = ordered
		ordered, p = p, next
	}
	return ordered
}
