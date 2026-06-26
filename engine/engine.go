package engine

import (
	"context"
	"slices"
	"sync"

	"github.com/go-faster/errors"

	"github.com/oteldb/storage/backend"
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
	// Backend stores flushed parts. Required for [Engine.Flush]; nil is a head-only engine.
	Backend backend.Backend
	// Prefix is the backend key prefix under which this engine's parts are written
	// (typically "{tenant}/metrics").
	Prefix string
}

// Engine is a single tenant's storage engine. Safe for concurrent use.
type Engine struct {
	cfg     Config
	mu      sync.RWMutex
	head    *head
	parts   []*part
	nextSeq int
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

// AppendBatch ingests a run of samples whose content ids are already computed (by the
// projection layer, on a reused buffer). ids[i], ts[i], values[i] describe sample i;
// materialize(i) returns sample i's full identity and is called only when its series is new
// (first sight), so a repeat series costs just a map probe and a buffer append, with no
// per-point [signal.Series] construction or hashing. The whole run is appended under a single
// lock. It returns how many samples were accepted (the rest were out-of-order beyond the
// window). Safe for concurrent use.
func (e *Engine) AppendBatch(
	ids []signal.SeriesID, ts []int64, values []float64, materialize func(i int) signal.Series,
) (accepted int, err error) {
	e.mu.Lock()
	defer e.mu.Unlock()

	// One closure for the whole run (not one per point): bi selects the current sample for
	// the lazy materializer, which fires only on a newly-seen series.
	var bi int

	mat := func() signal.Series { return materialize(bi) }

	for i := range ids {
		bi = i

		ok, isNew, s := e.head.appendByID(ids[i], ts[i], values[i], e.cfg.OOOWindow, mat)
		if !ok {
			continue
		}

		accepted++

		if e.cfg.WAL != nil {
			if isNew {
				if err := e.cfg.WAL.WriteSeries(ids[i], s); err != nil {
					return accepted, err
				}
			}

			// Slice the columns in place (no per-sample allocation) for the WAL record.
			if err := e.cfg.WAL.WriteSamples(ids[i], ts[i:i+1], values[i:i+1]); err != nil {
				return accepted, err
			}
		}
	}

	return accepted, nil
}

// Fetch implements [fetch.Fetcher] over the head ∪ flushed parts: it resolves the
// request's matchers to series (the index spans every series ever seen, flushed or not)
// and returns one batch per series with its samples in the window, merged across the head
// buffer and every part by timestamp.
func (e *Engine) Fetch(ctx context.Context, r fetch.Request) (fetch.Iterator, error) {
	e.mu.RLock()
	defer e.mu.RUnlock()

	ids := e.head.resolve(r.Matchers)

	var batches []*fetch.Batch

	for _, id := range ids {
		s, _ := e.head.series.Get(id)

		var m sampleMerge

		// Parts first (oldest → newest), then the head buffer last so the freshest value
		// wins on a duplicate timestamp.
		for _, p := range e.parts {
			if err := p.mergeInto(ctx, id, &m, r.Start, r.End); err != nil {
				return nil, err
			}
		}

		if hb := e.head.batch(id, r.Start, r.End); hb != nil {
			m.add(hb.Timestamps, hb.Values, r.Start, r.End)
		}

		if ts, values := m.collect(); len(ts) > 0 {
			batches = append(batches, &fetch.Batch{ID: id, Series: s, Timestamps: ts, Values: values})
		}
	}

	return fetch.NewSliceIterator(batches), nil
}

// sampleMerge merges samples from multiple sources for one series, deduplicating by
// timestamp. Sources are added oldest → newest, so a later add overwrites an earlier value
// at the same timestamp.
type sampleMerge struct {
	byTs map[int64]float64
}

// add merges the samples whose timestamps fall in [start, end].
func (m *sampleMerge) add(ts []int64, values []float64, start, end int64) {
	if m.byTs == nil {
		m.byTs = make(map[int64]float64, len(ts))
	}

	for i := range ts {
		if ts[i] < start || ts[i] > end {
			continue
		}

		m.byTs[ts[i]] = values[i]
	}
}

// collect returns the merged samples sorted ascending by timestamp.
func (m *sampleMerge) collect() ([]int64, []float64) {
	if len(m.byTs) == 0 {
		return nil, nil
	}

	ts := make([]int64, 0, len(m.byTs))
	for t := range m.byTs {
		ts = append(ts, t)
	}

	slices.Sort(ts)

	values := make([]float64, len(ts))
	for i, t := range ts {
		values[i] = m.byTs[t]
	}

	return ts, values
}

// Flush writes the head's buffered samples to a new immutable part and clears the buffers
// (the series index is retained). It is a no-op if the head holds no samples. Requires a
// [Config.Backend].
func (e *Engine) Flush(ctx context.Context) error {
	e.mu.Lock()
	defer e.mu.Unlock()

	return e.flushLocked(ctx)
}

// Reset discards all of the engine's data — the in-memory head (samples + series index)
// and every flushed part — returning it to the empty state of a freshly [New]'d engine,
// without reallocating the engine itself. Flushed part objects are deleted from the backend
// so none are orphaned. It is destructive (it wipes this engine's parts under
// [Config.Prefix]) and is meant for the ephemeral in-memory engine in tests and benchmarks,
// letting a long-lived engine be reused across runs. Safe for concurrent use.
func (e *Engine) Reset(ctx context.Context) error {
	e.mu.Lock()
	defer e.mu.Unlock()

	e.head = newHead()
	e.parts = nil
	e.nextSeq = 0

	if e.cfg.Backend == nil {
		return nil
	}

	// Delete every object this engine wrote: all part keys are "{Prefix}/{seq}/...", so the
	// "{Prefix}/" scope catches them without touching a sibling engine's keys.
	keys, err := e.cfg.Backend.List(ctx, e.cfg.Prefix+"/")
	if err != nil {
		return errors.Wrap(err, "list parts")
	}

	for _, k := range keys {
		if err := e.cfg.Backend.Delete(ctx, k); err != nil && !errors.Is(err, backend.ErrNotExist) {
			return errors.Wrapf(err, "delete %q", k)
		}
	}

	return nil
}

// PartCount returns the number of flushed parts (testing/introspection).
func (e *Engine) PartCount() int {
	e.mu.RLock()
	defer e.mu.RUnlock()

	return len(e.parts)
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

// ApplyReplicated applies an in-memory WAL-framed write (a replication payload from a peer)
// to the head: it registers each series and appends its samples through the **same
// out-of-order-checked path as a local ingest** (unlike WAL [Engine.Replay], which trusts the
// log unconditionally), so a replica drops samples older than its OOO window just as the
// origin would. It returns the number of samples rejected as out-of-order. A replica holds a
// peer's unflushed head this way; after a flush the shared object store reconciles them. Safe
// for concurrent use.
func (e *Engine) ApplyReplicated(data []byte) (rejected int, err error) {
	e.mu.Lock()
	defer e.mu.Unlock()

	byID := make(map[signal.SeriesID]signal.Series)

	err = wal.Replay(data, wal.Handlers{
		OnSeries: func(id signal.SeriesID, s signal.Series) error {
			byID[id] = s

			return nil
		},
		OnSamples: func(id signal.SeriesID, ts []int64, values []float64) error {
			s := byID[id] // the series record precedes its samples in the frame
			for i := range ts {
				if ok, _, _ := e.head.appendByID(id, ts[i], values[i], e.cfg.OOOWindow,
					func() signal.Series { return s }); !ok {
					rejected++
				}
			}

			return nil
		},
	})

	return rejected, err
}

// SeriesCount returns the number of distinct series in the head.
func (e *Engine) SeriesCount() int {
	e.mu.RLock()
	defer e.mu.RUnlock()

	return e.head.series.Len()
}

func (e *Engine) flushLocked(ctx context.Context) error {
	cols := e.head.drainHead()
	if cols == nil {
		return nil
	}

	prefix := e.partPrefix(e.nextSeq)
	if err := writePart(ctx, e.cfg.Backend, prefix, cols); err != nil {
		return err
	}

	p, err := openPart(ctx, e.cfg.Backend, prefix)
	if err != nil {
		return err
	}

	p.minTime, p.maxTime = colsTimeRange(cols)
	e.parts = append(e.parts, p)
	e.nextSeq++

	if err := e.updateIndexLocked(ctx); err != nil {
		return err
	}

	return e.writeSeriesIndexLocked(ctx)
}
