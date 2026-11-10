package engine

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"

	"github.com/go-faster/errors"

	backendfile "github.com/oteldb/storage/backend/file"
	"github.com/oteldb/storage/index/series"
	"github.com/oteldb/storage/query/fetch"
	"github.com/oteldb/storage/signal"
	"github.com/oteldb/storage/wal"
)

// writableHeads is a test-only composition, with independent existing engine/WAL lanes.
// It has no tenant policy, cluster routing, persistent lane configuration or batch atomicity.
type writableHeads struct {
	root           string
	heads          []*Engine
	cfgs           []Config
	prepared       bool
	buildWork      chan struct{}
	sharedMetadata bool
	contexts       *series.Contexts
	queued         bool
	dispatch       *headDispatcher
	hooks          *headDispatchHooks
}

func newWritableHeads(tb testing.TB, n int) *writableHeads {
	tb.Helper()
	f := &writableHeads{root: tb.TempDir(), heads: make([]*Engine, n), cfgs: make([]Config, n),
		buildWork: make(chan struct{}, 1)}
	be, err := backendfile.New(filepath.Join(f.root, "parts"))
	if err != nil {
		tb.Fatal(err)
	}
	decode := NewDecodeBudget(256 << 20)
	for i := range n {
		f.cfgs[i] = Config{Backend: be, Prefix: fmt.Sprintf("cpu/lane-%02d", i),
			DiskSeries: true, MetricSeriesChunks: true, AggregateStats: true,
			ResidentHeadBytes: headBudget(128<<20, n, i), SeriesCacheBytes: headBudget(512<<20, n, i),
			FlushMemoryBytes: 64 << 20, FlushAdmission: f.admitFlush,
			MergeMemoryBytes: 256 << 20, MergeAdmission: f.admitMerge,
			DecodeCacheBytes: headBudget(16<<20, n, i), DecodeBudget: decode, OOOWindow: 10}
	}
	tb.Cleanup(func() {
		if err := f.close(); err != nil {
			tb.Error(err)
		}
	})
	if err := f.open(tb.Context(), false); err != nil {
		tb.Fatal(err)
	}
	return f
}

func reserveHeadWork(ctx context.Context, bytes, limit int64, slots chan struct{}, wait bool) (func(), bool, error) {
	if bytes > limit {
		return nil, false, errors.Errorf("prototype build workspace %d exceeds shared cap %d", bytes, limit)
	}
	if wait {
		select {
		case slots <- struct{}{}:
		case <-ctx.Done():
			return nil, false, ctx.Err()
		}
	} else {
		select {
		case slots <- struct{}{}:
		default:
			return nil, false, nil
		}
	}
	return func() { <-slots }, true, nil
}

func (f *writableHeads) admitFlush(ctx context.Context, bytes int64) (func(), error) {
	release, _, err := reserveHeadWork(ctx, bytes, 256<<20, f.buildWork, true)
	return release, err
}

func (f *writableHeads) admitMerge(ctx context.Context, bytes int64, wait bool) (func(), bool, error) {
	return reserveHeadWork(ctx, bytes, 256<<20, f.buildWork, wait)
}

func headBudget(total int64, n, lane int) int64 {
	share := total / int64(n)
	if int64(lane) < total%int64(n) {
		share++
	}
	return share
}

func (f *writableHeads) walDir(lane int) string {
	return filepath.Join(f.root, "wal", fmt.Sprintf("lane-%02d", lane))
}

func (f *writableHeads) open(ctx context.Context, replay bool) error {
	if f.sharedMetadata {
		f.contexts = series.NewContexts(32 << 20)
	}
	for i := range f.heads {
		w, err := wal.Create(f.walDir(i), 0)
		if err != nil {
			return err
		}
		w.SetSync(false)
		cfg := f.cfgs[i]
		if f.sharedMetadata {
			cfg.SeriesContexts = f.contexts
			cfg.SeriesCacheBytes = headBudget(480<<20, len(f.heads), i)
		}
		cfg.WAL = w
		f.heads[i] = New(cfg)
		if err := f.heads[i].LoadParts(ctx); err != nil {
			return err
		}
		if replay {
			if err := f.heads[i].Replay(ctx, f.walDir(i)); err != nil {
				return err
			}
		}
	}
	if f.queued {
		f.dispatch = newHeadDispatcher(ctx, f.heads, f.prepared, f.hooks)
	}
	return nil
}

func (f *writableHeads) close() error {
	if f.dispatch != nil {
		f.dispatch.close()
	}
	var result error
	for i, h := range f.heads {
		if h != nil {
			result = errors.Join(result, h.CloseWAL())
			f.heads[i] = nil
		}
	}
	return result
}

// resetFixture discards measured writes and restores the fixed immutable seed, outside timing.
func (f *writableHeads) resetFixture(ctx context.Context) error {
	if err := f.close(); err != nil {
		return err
	}
	if err := os.RemoveAll(filepath.Join(f.root, "wal")); err != nil {
		return err
	}
	return f.open(ctx, false)
}

func (f *writableHeads) flush(ctx context.Context) error {
	for _, h := range f.heads {
		if err := h.Flush(ctx); err != nil {
			return err
		}
		if h.PartCount() > 16 {
			return errors.New("prototype fixture emitted excessive parts; inspect workspace before benchmarking")
		}
	}
	return nil
}

func (f *writableHeads) fetcher() fetch.Fetcher {
	children := make([]fetch.Fetcher, len(f.heads))
	for i, h := range f.heads {
		children[i] = h
	}
	return fetch.Merge(children...)
}

type headRouteBatch struct {
	counts, offsets, cursors []int
	refs                     []int
	ids                      []signal.SeriesID
	ts                       []int64
	values, sf               []float64
	requests                 []headRequest
	weighted                 bool
	materializeMu            sync.Mutex
}

func (b *headRouteBatch) append(
	ctx context.Context, f *writableHeads, ids []signal.SeriesID, ts []int64, values, sf []float64, materialize func(int) signal.Series,
) (AppendResult, error) {
	dispatch := f.dispatch
	if dispatch != nil {
		if len(ids) > maxPreparedBatchSamples {
			return AppendResult{}, errors.New("prototype dispatch accepts at most 4096 points")
		}
		if err := dispatch.begin(ctx); err != nil {
			return AppendResult{}, err
		}
		defer dispatch.end()
	}
	if len(f.heads) == 1 && dispatch == nil {
		return f.appendLane(f.heads[0], ids, ts, values, sf, materialize)
	}
	n := len(f.heads)
	b.counts = resize(b.counts, n)
	clear(b.counts)
	for _, id := range ids {
		b.counts[id.Lo%uint64(n)]++
	}
	b.offsets, b.cursors = resize(b.offsets, n+1), resize(b.cursors, n)
	b.offsets[0] = 0
	for i, count := range b.counts {
		b.offsets[i+1] = b.offsets[i] + count
		b.cursors[i] = b.offsets[i]
	}
	b.ids, b.refs = resize(b.ids, len(ids)), resize(b.refs, len(ids))
	b.ts, b.values = resize(b.ts, len(ids)), resize(b.values, len(ids))
	if sf != nil {
		b.sf = resize(b.sf, len(ids))
	}
	b.weighted = sf != nil
	for i, id := range ids {
		lane := id.Lo % uint64(n)
		j := b.cursors[lane]
		b.cursors[lane]++
		b.ids[j], b.refs[j], b.ts[j], b.values[j] = id, i, ts[i], values[i]
		if sf != nil {
			b.sf[j] = sf[i]
		}
	}
	if dispatch != nil {
		return b.dispatchBatch(ctx, dispatch, materialize)
	}
	var result AppendResult
	for lane, h := range f.heads {
		lo, hi := b.offsets[lane], b.offsets[lane+1]
		if lo == hi {
			continue
		}
		var weights []float64
		if sf != nil {
			weights = b.sf[lo:hi]
		}
		r, err := f.appendLane(h, b.ids[lo:hi], b.ts[lo:hi], b.values[lo:hi], weights,
			func(i int) signal.Series { return materialize(b.refs[lo+i]) })
		result.Accepted += r.Accepted
		result.RejectedOOO += r.RejectedOOO
		if err != nil {
			return result, errors.Wrapf(err, "append head %d", lane)
		}
	}
	return result, nil
}

func (f *writableHeads) appendLane(h *Engine, ids []signal.SeriesID, ts []int64, values, sf []float64,
	materialize func(int) signal.Series) (AppendResult, error) {
	return appendHeadLane(h, f.prepared, ids, ts, values, sf, materialize)
}
