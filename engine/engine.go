package engine

import (
	"context"
	"sync"

	"github.com/oteldb/storage/query/fetch"
	"github.com/oteldb/storage/signal"
	"github.com/oteldb/storage/wal"
)

// Config configures an [Engine].
type Config struct {
	// OOOWindow rejects samples older than newest-OOOWindow (nanoseconds). 0 disables.
	OOOWindow int64
	// WAL, when non-nil, durably logs series and samples for crash recovery. nil is the
	// ephemeral in-memory engine.
	WAL *wal.SegmentWriter
}

// Engine is a single tenant's storage engine. Safe for concurrent use.
type Engine struct {
	cfg  Config
	mu   sync.RWMutex
	head *head
}

var _ fetch.Fetcher = (*Engine)(nil)

// New returns an engine with an empty head.
func New(cfg Config) *Engine {
	return &Engine{cfg: cfg, head: newHead()}
}

// Append ingests one sample for series s, logging to the WAL when durable. It returns
// whether the sample was accepted (false ⇒ rejected as out-of-order beyond the window).
func (e *Engine) Append(s signal.Series, ts int64, value float64) (bool, error) {
	e.mu.Lock()
	defer e.mu.Unlock()

	id, accepted, isNew := e.head.append(s, ts, value, e.cfg.OOOWindow)
	if !accepted {
		return false, nil
	}

	if e.cfg.WAL != nil {
		if isNew {
			if err := e.cfg.WAL.WriteSeries(id, s); err != nil {
				return true, err
			}
		}

		if err := e.cfg.WAL.WriteSamples(id, []int64{ts}, []float64{value}); err != nil {
			return true, err
		}
	}

	return true, nil
}

// Fetch implements [fetch.Fetcher] over the head: it resolves the request's matchers to
// series and returns one batch per series with its samples in the window.
func (e *Engine) Fetch(_ context.Context, r fetch.Request) (fetch.Iterator, error) {
	e.mu.RLock()
	defer e.mu.RUnlock()

	ids := e.head.resolve(r.Matchers)

	var batches []*fetch.Batch

	for _, id := range ids {
		if b := e.head.batch(id, r.Start, r.End); b != nil {
			batches = append(batches, b)
		}
	}

	return fetch.NewSliceIterator(batches), nil
}

// Replay rebuilds the head from the WAL segments in dir (durable restart).
func (e *Engine) Replay(dir string) error {
	e.mu.Lock()
	defer e.mu.Unlock()

	return wal.ReplayDir(dir, wal.Handlers{
		OnSeries: func(_ signal.SeriesID, s signal.Series) error {
			e.head.registerSeries(s)

			return nil
		},
		OnSamples: func(id signal.SeriesID, ts []int64, values []float64) error {
			e.head.replaySamples(id, ts, values)

			return nil
		},
	})
}

// SeriesCount returns the number of distinct series in the head.
func (e *Engine) SeriesCount() int {
	e.mu.RLock()
	defer e.mu.RUnlock()

	return e.head.series.Len()
}
