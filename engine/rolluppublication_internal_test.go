package engine

import (
	"context"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/backend"
	"github.com/oteldb/storage/signal"
)

func rollupPublicationCandidate(t *testing.T, be backend.Backend) (*Engine, []*part, []*part, rollupPlan) {
	t.Helper()
	e := New(Config{Backend: be, Prefix: "review739"})
	_, err := e.Append(rollupSeries(), 11, 3)
	require.NoError(t, err)
	require.NoError(t, e.Flush(t.Context()))
	src := liveParts(e)
	tiers := []DownsampleTier{{Before: 100, Interval: 10, Agg: signal.AggSum}}
	plan, err := e.planRollup(t.Context(), src, minInt64, tiers)
	require.NoError(t, err)
	out, err := e.compactStream(t.Context(), src, minInt64, e.mergeCapBytes(t.Context()), plan, MergeOptions{Downsample: tiers})
	require.NoError(t, err)
	planMergeBlocks(src, out)
	return e, src, out, plan
}

func TestRollupPublicationRechecksHeadBeforeFirstCAS(t *testing.T) {
	t.Parallel()
	e, src, out, plan := rollupPublicationCandidate(t, backend.Memory())
	accepted, err := e.Append(rollupSeries(), 11, 3)
	require.NoError(t, err)
	require.True(t, accepted)
	committed, err := e.publishMerge(t.Context(), src, out, src, e.term(), plan)
	require.NoError(t, err)
	assert.False(t, committed, "accepted raw duplicate arrived before rollup publication")
	_, values := rollupSamples(t, e)
	total := 0.0
	for _, v := range values {
		total += v
	}
	assert.InDelta(t, 3.0, total, 0, "the prepublication duplicate must be deduplicated while still raw")
}

type rollupPublicationGate struct {
	backend.Backend

	armed            atomic.Bool
	entered, release chan struct{}
}

func (b *rollupPublicationGate) CompareAndSwap(ctx context.Context, key string, expected backend.Version, data []byte) (backend.Version, bool, error) {
	if b.armed.Swap(false) {
		close(b.entered)
		select {
		case <-b.release:
		case <-ctx.Done():
			return backend.VersionAbsent, false, ctx.Err()
		}
	}
	return b.Backend.CompareAndSwap(ctx, key, expected, data)
}

func TestRollupPublicationExcludesWritersDuringCAS(t *testing.T) {
	t.Parallel()
	be := &rollupPublicationGate{Backend: backend.Memory(), entered: make(chan struct{}), release: make(chan struct{})}
	e, src, out, plan := rollupPublicationCandidate(t, be)
	be.armed.Store(true)
	type result struct {
		committed bool
		err       error
	}
	done := make(chan result, 1)
	go func() { ok, err := e.publishMerge(t.Context(), src, out, src, e.term(), plan); done <- result{ok, err} }()
	<-be.entered
	writerEntered := e.mu.TryLockWriter()
	if writerEntered {
		e.mu.UnlockWriter()
	}
	assert.False(t, writerEntered, "an old duplicate can be admitted while rollup CAS has not committed")
	if writerEntered {
		accepted, err := e.Append(rollupSeries(), 11, 3)
		require.NoError(t, err)
		require.True(t, accepted, "the racing duplicate is acknowledged before CAS")
	}
	close(be.release)
	published := <-done
	require.NoError(t, published.err)
	require.True(t, published.committed)
	if writerEntered {
		_, values := rollupSamples(t, e)
		total := 0.0
		for _, v := range values {
			total += v
		}
		assert.InDelta(t, 3.0, total, 0, "a sample admitted before actual CAS must participate in bucket closure")
	}
}
