package engine

import (
	"context"
	"testing"

	"github.com/go-faster/errors"
	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/backend"
	"github.com/oteldb/storage/backend/bucketindex"
	"github.com/oteldb/storage/block"
	"github.com/oteldb/storage/query/fetch"
	"github.com/oteldb/storage/signal"
)

func TestMemoryPinnedReaderDoesNotCauseTinyPressureFlushes(t *testing.T) {
	t.Parallel()
	e, _ := newFaultWALEngine(t, 0)
	e.cfg.Backend, e.cfg.Prefix = backend.Memory(), "reader-pressure"
	enableMemoryPublicationTest(t, e)
	s := apiSeries()
	ids := make([]signal.SeriesID, 4096)
	ts, values := make([]int64, len(ids)), make([]float64, len(ids))
	for i := range ids {
		ids[i], ts[i] = s.Hash(), int64(i)
	}
	_, err := e.AppendBatch(ids, ts, values, nil, func(int) signal.Series { return s }, AppendLimits{})
	require.NoError(t, err)
	threshold := e.HeadBytes() / 2
	reader, err := e.Fetch(t.Context(), fetch.Request{Start: 0, End: 10000})
	require.NoError(t, err)
	defer func() { require.NoError(t, reader.Close()) }()
	require.NoError(t, e.Flush(t.Context()))
	require.Greater(t, e.ResidentHeadBytes(), threshold)
	require.Zero(t, e.HeadBytes())
	for i := range 10 {
		_, err := e.Append(s, int64(5000+i), 1)
		require.NoError(t, err)
		if e.HeadBytes() >= threshold {
			require.NoError(t, e.Flush(t.Context()))
		}
	}
	require.Equal(t, 1, e.PartCount(), "retired read leases cannot produce a part per small append")
	require.Equal(t, 10, e.HeadSampleCount())
}

type memoryDeleteFailureBackend struct {
	backend.Backend

	fail bool
}

func (b *memoryDeleteFailureBackend) Delete(ctx context.Context, key string) error {
	if b.fail {
		return errors.New("injected delete failure")
	}
	return b.Backend.Delete(ctx, key)
}

func TestMemoryReclaimRetryPreservesReadoptedPrefix(t *testing.T) {
	t.Parallel()
	e, _ := newFaultWALEngine(t, 0)
	files := &memoryDeleteFailureBackend{Backend: backend.Memory()}
	e.cfg.Backend, e.cfg.Prefix = files, "reclaim-retry"
	enableMemoryPublicationTest(t, e)
	_, err := e.Append(apiSeries(), 10, 1)
	require.NoError(t, err)
	require.NoError(t, e.Flush(t.Context()))
	old := e.parts[0]
	entry := bucketindex.Entry{Prefix: old.prefix, MinTime: old.minTime, MaxTime: old.maxTime,
		Blocks: old.blocks, Claim: old.claim, Level: old.level, Term: old.term}
	e.mu.Lock()
	e.parts = nil
	e.syncMemoryPartsLocked()
	e.retireLocked([]*part{old})
	e.mu.Unlock()
	files.fail = true
	e.reclaimRetired(t.Context())
	files.fail = false
	require.Len(t, e.retiring, 1)
	e.partLifetimes.protect(make(map[string]struct{}))
	e.mu.Lock()
	reopened, err := e.livePart(t.Context(), &entry, make(map[string]*part))
	require.NoError(t, err)
	e.parts = []*part{reopened}
	e.syncMemoryPartsLocked()
	e.mu.Unlock()
	e.reclaimRetired(t.Context())
	present, err := block.PartPresent(t.Context(), e.cfg.Backend, old.prefix)
	require.NoError(t, err)
	require.True(t, present, "the queued old wrapper must honor the reopened prefix's current owner")
	e.mu.Lock()
	e.parts = nil
	e.syncMemoryPartsLocked()
	e.mu.Unlock()
	e.reclaimRetired(t.Context())
	present, err = block.PartPresent(t.Context(), e.cfg.Backend, old.prefix)
	require.NoError(t, err)
	require.False(t, present)
	require.Empty(t, e.partLifetimes.byPrefix)
}
