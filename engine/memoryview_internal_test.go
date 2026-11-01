package engine

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/backend"
	"github.com/oteldb/storage/backend/bucketindex"
	"github.com/oteldb/storage/block"
)

func TestMemoryPartOwnershipDoesNotPinIndexView(t *testing.T) {
	t.Parallel()
	e := New(Config{Backend: backend.Memory(), Prefix: "view-index"})
	_, err := e.Append(apiSeries(), 1, 1)
	require.NoError(t, err)
	require.NoError(t, e.Flush(t.Context()))
	p := e.parts[0]
	set := newMemoryPartSet([]*part{p})
	_, err = p.index.paged.entries(t.Context())
	require.NoError(t, err)
	require.NotNil(t, p.index.paged.view.Load())
	p.acquire()
	p.release()
	require.Zero(t, p.refs.Load())
	require.Nil(t, p.index.paged.view.Load(), "directory ownership does not retain the full index between queries")
	require.False(t, p.reclaimable())
	set.release()
	require.True(t, p.reclaimable())
}

func TestMemoryPartMetadataIsOwned(t *testing.T) {
	t.Parallel()
	p := &part{prefix: "p", blocks: bucketindex.Blocks(1, 3), level: 1, term: 2}
	set := newMemoryPartSet([]*part{p})
	p.blocks.Gaps[0] = bucketindex.Gap{}
	p.level, p.term = 10, 20
	require.Equal(t, uint32(1), set.entries[0].level)
	require.Equal(t, uint64(2), set.entries[0].term)
	require.True(t, set.entries[0].blocks.Equal(bucketindex.Blocks(1, 3)))
	set.release()
}

func TestMemoryPartMetadataReplacementSharesLifetime(t *testing.T) {
	t.Parallel()
	e := New(Config{Backend: backend.Memory(), Prefix: "view-replacement"})
	_, err := e.Append(apiSeries(), 1, 1)
	require.NoError(t, err)
	require.NoError(t, e.Flush(t.Context()))
	old := e.parts[0]
	old.acquire()
	entry := bucketindex.Entry{Prefix: old.prefix, MinTime: old.minTime, MaxTime: old.maxTime,
		Blocks: old.blocks, Claim: old.claim, Level: old.level, Term: old.term + 1}
	replacement, err := e.livePart(t.Context(), &entry, map[string]*part{old.prefix: old})
	require.NoError(t, err)
	require.NotSame(t, old, replacement)
	require.Same(t, old.lifetime(), replacement.lifetime())
	e.mu.Lock()
	e.parts = nil
	e.retireLocked([]*part{replacement})
	e.mu.Unlock()
	e.reclaimRetired(t.Context())
	present, err := block.PartPresent(t.Context(), e.cfg.Backend, old.prefix)
	require.NoError(t, err)
	require.True(t, present, "the older metadata wrapper still owns the same physical files")
	old.release()
	e.reclaimRetired(t.Context())
	present, err = block.PartPresent(t.Context(), e.cfg.Backend, old.prefix)
	require.NoError(t, err)
	require.False(t, present)
}

func TestMemoryReadViewSurvivesHeadRotation(t *testing.T) {
	t.Parallel()
	released := 0
	run := testMemoryRun(t, 1, func() { released++ })
	path := newMemoryDirectoryPath(nil)
	view := &memoryReadView{}
	view.inherit(nil)
	view.active = path.insert(nil, 1, run)
	path.close()
	run.release()
	view.acquire()
	next := &memoryReadView{}
	next.inherit(view)
	next.frozen, next.active = next.active, nil
	view.release()
	require.NotNil(t, view.active)
	require.Nil(t, view.frozen)
	require.Nil(t, next.active)
	require.NotNil(t, next.frozen)
	require.Equal(t, uint64(1), next.revision)
	next.release()
	require.Zero(t, released)
	require.NotEmpty(t, view.active.runDataForTest())
	view.release()
	require.Equal(t, 1, released)
}

func (n *memoryDirectoryNode) runDataForTest() []int64 {
	var values []int64
	n.walk(minInt64, maxInt64, func(run *memoryRun) bool {
		values = append(values, run.ts...)
		return true
	})
	return values
}
