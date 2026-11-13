package engine

import (
	"context"

	"github.com/go-faster/errors"
)

type metadataLoad struct {
	done    chan struct{}
	callers int
	entry   *blockEntry
	err     error
}

func (c *blockCache) metadata(ctx context.Context, key blockKey, load func() (*blockEntry, error)) (*blockEntry, error) {
	for {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		if entry, ok := c.get(key); ok {
			return entry, nil
		}
		shard := c.shard(key)
		shard.mu.Lock()
		if entry, ok := shard.lookup(key); ok {
			entry.refs++
			shard.lru(key).MoveToFront(entry)
			shard.mu.Unlock()
			return entry, nil
		}
		flight := shard.loads[key]
		if flight == nil {
			flight = &metadataLoad{done: make(chan struct{}), callers: 1}
			if shard.loads == nil {
				shard.loads = make(map[blockKey]*metadataLoad)
			}
			shard.loads[key] = flight
			shard.mu.Unlock()

			entry, err := load()
			shard.mu.Lock()
			flight.entry, flight.err = entry, err
			if entry != nil {
				// Transfer one pin to every caller before any can release or evict the result.
				entry.refs += flight.callers - 1
			}
			delete(shard.loads, key)
			close(flight.done)
			shard.mu.Unlock()
			if canceled := ctx.Err(); canceled != nil {
				c.release(entry)
				return nil, canceled
			}
			return entry, err
		}
		flight.callers++
		shard.mu.Unlock()
		select {
		case <-flight.done:
			if canceled := ctx.Err(); canceled != nil {
				c.release(flight.entry)
				return nil, canceled
			}
			if errors.Is(flight.err, context.Canceled) || errors.Is(flight.err, context.DeadlineExceeded) {
				continue
			}
			return flight.entry, flight.err
		case <-ctx.Done():
			shard.mu.Lock()
			select {
			case <-flight.done:
				entry := flight.entry
				shard.mu.Unlock()
				c.release(entry)
			default:
				flight.callers--
				shard.mu.Unlock()
			}
			return nil, ctx.Err()
		}
	}
}
