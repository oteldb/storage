package recordengine_test

import (
	"context"
	"encoding/binary"
	"iter"
	"slices"
	"strconv"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/backend"
	"github.com/oteldb/storage/recordengine"
	"github.com/oteldb/storage/wal"
)

// fakeSide is a minimal content-addressed [recordengine.SideStore] for testing the hook: a single
// "table" mapping uint64 ids → bytes. A delta and the encoded sidecar share one format
// ([uvarint count] then per entry [uvarint id][uvarint len][bytes], sorted by id), so Union is a
// plain dedup. It records how many times each lifecycle method ran.
type fakeSide struct {
	acc       map[uint64][]byte
	refColumn string
	absorbed  int
	encoded   int
	resets    int
	retains   int
	restores  int
	unions    int
	stores    int
}

func newFakeSide() *fakeSide { return &fakeSide{acc: map[uint64][]byte{}} }

func encodeSide(m map[uint64][]byte) []byte {
	ids := make([]uint64, 0, len(m))
	for id := range m {
		ids = append(ids, id)
	}

	slices.Sort(ids)

	out := binary.AppendUvarint(nil, uint64(len(ids)))
	for _, id := range ids {
		out = binary.AppendUvarint(out, id)
		out = binary.AppendUvarint(out, uint64(len(m[id])))
		out = append(out, m[id]...)
	}

	return out
}

func decodeSide(data []byte, into map[uint64][]byte) error {
	n, off := binary.Uvarint(data)
	for range n {
		id, k := binary.Uvarint(data[off:])
		off += k
		ln, k := binary.Uvarint(data[off:])
		off += k
		into[id] = append([]byte(nil), data[off:off+int(ln)]...)
		off += int(ln)
	}

	return nil
}

func (f *fakeSide) Absorb(delta []byte) error {
	f.absorbed++

	return decodeSide(delta, f.acc)
}

func (f *fakeSide) Encode() map[string][]byte {
	f.encoded++

	return map[string][]byte{"table": encodeSide(f.acc)}
}

func (f *fakeSide) Reset() {
	f.resets++
	f.acc = map[uint64][]byte{}
}

func (f *fakeSide) RefColumn() string {
	if f.refColumn != "" {
		return f.refColumn
	}

	return "id"
}

// Retain keeps the entries whose id is a ref cell in decimal.
func (f *fakeSide) Retain(refs iter.Seq[[]byte]) {
	f.retains++
	kept := map[uint64][]byte{}

	for ref := range refs {
		id, err := strconv.ParseUint(string(ref), 10, 64)
		if entry, ok := f.acc[id]; err == nil && ok {
			kept[id] = entry
		}
	}

	f.acc = kept
}

func (f *fakeSide) Restore(snapshot map[string][]byte) error {
	f.restores++

	data, ok := snapshot["table"]
	if !ok {
		return nil
	}

	return decodeSide(data, f.acc)
}

func (f *fakeSide) Names() []string { return []string{"table"} }

func (f *fakeSide) Union(parts []map[string][]byte, refs iter.Seq[[]byte]) (map[string][]byte, error) {
	f.unions++
	merged := map[uint64][]byte{}

	for _, p := range parts {
		if data, ok := p["table"]; ok {
			if err := decodeSide(data, merged); err != nil {
				return nil, err
			}
		}
	}

	kept := map[uint64][]byte{}

	for ref := range refs {
		id, err := strconv.ParseUint(string(ref), 10, 64)
		if entry, ok := merged[id]; err == nil && ok {
			kept[id] = entry
		}
	}

	return map[string][]byte{"table": encodeSide(kept)}, nil
}

func (f *fakeSide) Stored(tables map[string][]byte) (map[string][]byte, error) {
	f.stores++

	return tables, nil
}

// sideIDs reads every sidecar object under the engine prefix and returns the union of ids it holds.
func sideIDs(t *testing.T, be backend.Backend) []uint64 {
	t.Helper()

	keys, err := be.List(context.Background(), "t/recs/")
	require.NoError(t, err)

	got := map[uint64][]byte{}

	for _, k := range keys {
		if !strings.Contains(k, "/sym-") {
			continue
		}

		data, err := be.Read(context.Background(), k)
		require.NoError(t, err)
		require.NoError(t, decodeSide(data, got))
	}

	ids := make([]uint64, 0, len(got))
	for id := range got {
		ids = append(ids, id)
	}

	slices.Sort(ids)

	return ids
}

func sideEngine(be backend.Backend, fs recordengine.SideStore) *recordengine.Engine {
	return recordengine.New(recordengine.Config{Schema: testSchema, Backend: be, Prefix: "t/recs", SideStore: fs})
}

// TestSideStoreFlushPersistsAndResets verifies the engine absorbs each batch's delta into the live
// accumulator, writes the accumulated table as a part sidecar on flush, and resets afterward.
func TestSideStoreFlushPersistsAndResets(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	be := backend.Memory()
	fs := newFakeSide()
	e := sideEngine(be, fs)

	b1 := mkBatch("api", rrec{ts: 1, body: "x"})
	b1.Side = encodeSide(map[uint64][]byte{1: []byte("a"), 2: []byte("b")})
	ingest(t, e, b1)

	b2 := mkBatch("api", rrec{ts: 2, body: "y"})
	b2.Side = encodeSide(map[uint64][]byte{2: []byte("b"), 3: []byte("c")}) // 2 is a dup
	ingest(t, e, b2)

	require.Equal(t, 2, fs.absorbed)
	require.NoError(t, e.Flush(ctx))
	require.Equal(t, 1, fs.encoded)
	require.Equal(t, 1, fs.resets)

	// The flushed part's sidecar holds the deduped union of both batches' deltas.
	require.Equal(t, []uint64{1, 2, 3}, sideIDs(t, be))
	// The accumulator was reset.
	require.Empty(t, fs.acc)
}

// TestSideStoreReplicates verifies the side delta rides the cluster write path: EncodeWAL carries
// it, ApplyPrimary absorbs it (and forwards it), and ApplyReplicated absorbs it on a secondary — so
// both replicas' accumulators converge.
func TestSideStoreReplicates(t *testing.T) {
	t.Parallel()

	fsP := newFakeSide()
	primary := recordengine.New(recordengine.Config{Schema: testSchema, SideStore: fsP})
	fsS := newFakeSide()
	secondary := recordengine.New(recordengine.Config{Schema: testSchema, SideStore: fsS})

	b := mkBatch("api", rrec{ts: 1, body: "x"})
	b.Side = encodeSide(map[uint64][]byte{1: []byte("a"), 2: []byte("b")})

	accepted, res, err := primary.ApplyPrimary(recordengine.EncodeWAL(b), recordengine.AppendLimits{})
	require.NoError(t, err)
	require.Zero(t, res.Rejected())
	require.NoError(t, secondary.ApplyReplicated(accepted))

	want := []uint64{1, 2}
	require.Equal(t, want, accIDs(fsP), "primary absorbed the symbols")
	require.Equal(t, want, accIDs(fsS), "secondary absorbed the forwarded symbols")
}

func replicateSide(t *testing.T, primary, replica *recordengine.Engine, b *recordengine.Batch) {
	t.Helper()

	accepted, _, err := primary.ApplyPrimary(recordengine.EncodeWAL(b), recordengine.AppendLimits{})
	require.NoError(t, err)
	require.NoError(t, replica.ApplyReplicated(accepted))
}

// TestSideStoreReplicaRefreshRetainsHead: a replica never flushes, so its refresh is what shrinks
// the accumulator, to the entries the records left after the trim reference.
func TestSideStoreReplicaRefreshRetainsHead(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	be := backend.Memory()
	primary := sideEngine(be, newFakeSide())
	fs := newFakeSide()
	replica := sideEngine(be, fs)

	flushed := mkBatch("api", rrec{ts: 1, id: "1"})
	flushed.Side = encodeSide(map[uint64][]byte{1: []byte("a"), 9: []byte("unreferenced")})
	replicateSide(t, primary, replica, flushed)
	require.NoError(t, primary.Flush(ctx))

	pending := mkBatch("api", rrec{ts: 2, id: "2"})
	pending.Side = encodeSide(map[uint64][]byte{2: []byte("b")})
	replicateSide(t, primary, replica, pending)

	require.NoError(t, replica.RefreshReplica(ctx))
	require.Equal(t, 1, replica.HeadRecordCount())
	assert.Equal(t, []uint64{2}, accIDs(fs), "only the unflushed record's entry is kept")
	assert.Equal(t, 1, fs.retains)

	require.NoError(t, replica.RefreshReplica(ctx))
	assert.Equal(t, 1, fs.retains, "a refresh that trims nothing leaves the accumulator alone")
}

// TestSideStoreReplicaRefreshDropsRejectedDeltas: the primary forwards the side delta of a write whose
// records it all rejected. That adds no row for a trim to drop, so the refresh must retain anyway.
func TestSideStoreReplicaRefreshDropsRejectedDeltas(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	be := backend.Memory()
	primary := sideEngine(be, newFakeSide())
	fs := newFakeSide()
	replica := sideEngine(be, fs)

	kept := mkBatch("api", rrec{ts: 1, id: "1"})
	kept.Side = encodeSide(map[uint64][]byte{1: []byte("a")})
	replicateSide(t, primary, replica, kept)

	for i := range 3 {
		shed := mkBatch("shed", rrec{ts: 1, id: "10"})
		shed.Side = encodeSide(map[uint64][]byte{uint64(10 + i): []byte("rejected")})

		accepted, res, err := primary.ApplyPrimary(recordengine.EncodeWAL(shed), recordengine.AppendLimits{MaxSeries: 1})
		require.NoError(t, err)
		require.Equal(t, 1, res.RejectedCardinality)
		require.NoError(t, replica.ApplyReplicated(accepted))

		require.NoError(t, replica.RefreshReplica(ctx))
		assert.Equal(t, []uint64{1}, accIDs(fs), "round %d", i)
	}

	assert.Equal(t, 1, replica.HeadRecordCount())
	assert.Equal(t, 3, fs.retains)
}

// TestSideStoreDemotedPrimaryDropsRejectedDelta: a primary absorbs a write's delta before admission
// rejects its records. Demoted before any flush, its refresh has no part and trims no row, and must
// still drop that delta.
func TestSideStoreDemotedPrimaryDropsRejectedDelta(t *testing.T) {
	t.Parallel()

	fs := newFakeSide()
	e := sideEngine(backend.Memory(), fs)

	kept := mkBatch("api", rrec{ts: 1, id: "1"})
	kept.Side = encodeSide(map[uint64][]byte{1: []byte("a")})
	_, _, err := e.ApplyPrimary(recordengine.EncodeWAL(kept), recordengine.AppendLimits{})
	require.NoError(t, err)

	shed := mkBatch("shed", rrec{ts: 1, id: "10"})
	shed.Side = encodeSide(map[uint64][]byte{10: []byte("rejected")})
	_, res, err := e.ApplyPrimary(recordengine.EncodeWAL(shed), recordengine.AppendLimits{MaxSeries: 1})
	require.NoError(t, err)
	require.Equal(t, 1, res.RejectedCardinality)

	require.NoError(t, e.RefreshReplica(t.Context()))
	assert.Equal(t, []uint64{1}, accIDs(fs))
	assert.Equal(t, 1, fs.retains)
}

// TestSideStoreIdleFlushDropsRejectedDeltas: an owner whose writes are all rejected keeps an empty
// head, so no flush writes a part, and each flush must drop the deltas instead.
func TestSideStoreIdleFlushDropsRejectedDeltas(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	fs := newFakeSide()
	e := recordengine.New(recordengine.Config{
		Schema: testSchema, Backend: backend.Memory(), Prefix: "t/recs", SideStore: fs, OOOWindow: 10,
	})

	newest := mkBatch("api", rrec{ts: 100, id: "1"})
	newest.Side = encodeSide(map[uint64][]byte{1: []byte("a")})
	_, _, err := e.ApplyPrimary(recordengine.EncodeWAL(newest), recordengine.AppendLimits{})
	require.NoError(t, err)
	require.NoError(t, e.Flush(ctx))

	for i := range 3 {
		late := mkBatch("api", rrec{ts: 1, id: "2"})
		late.Side = encodeSide(map[uint64][]byte{uint64(2 + i): []byte("rejected")})

		_, res, err := e.ApplyPrimary(recordengine.EncodeWAL(late), recordengine.AppendLimits{})
		require.NoError(t, err)
		require.Equal(t, 1, res.RejectedOOO)

		require.NoError(t, e.Flush(ctx))
		assert.Empty(t, accIDs(fs), "round %d", i)
	}

	resets := fs.resets
	require.NoError(t, e.Flush(ctx))
	assert.Equal(t, resets, fs.resets, "an idle flush with nothing absorbed leaves the store alone")
}

func TestSideStoreRetainRejectsNonByteRefColumn(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	for _, column := range []string{"sev", "missing"} {
		be := backend.Memory()
		primary := sideEngine(be, newFakeSide())
		fs := newFakeSide()
		fs.refColumn = column
		replica := sideEngine(be, fs)

		replicateSide(t, primary, replica, mkBatch("api", rrec{ts: 1, id: "1"}))
		require.NoError(t, primary.Flush(ctx))

		require.ErrorContains(t, replica.RefreshReplica(ctx), column)
	}
}

// TestSideStoreWALReplay verifies the side delta is logged to the WAL and a fresh engine's Replay
// reconstructs the side store along with the head.
func TestSideStoreWALReplay(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()

	w, err := wal.Create(dir, 0)
	require.NoError(t, err)
	t.Cleanup(func() { _ = w.Close() }) // release the segment handle so the temp WAL dir is removable (Windows)

	e := recordengine.New(recordengine.Config{Schema: testSchema, WAL: w, SideStore: newFakeSide()})

	b := mkBatch("api", rrec{ts: 1, body: "x"})
	b.Side = encodeSide(map[uint64][]byte{1: []byte("a"), 2: []byte("b")})
	ingest(t, e, b)
	require.NoError(t, w.Sync())

	// A fresh engine replays the WAL directory: head records and side store both come back.
	fs := newFakeSide()
	replayed := recordengine.New(recordengine.Config{Schema: testSchema, SideStore: fs})
	require.NoError(t, replayed.Replay(t.Context(), dir))

	require.Equal(t, 1, replayed.HeadRecordCount(), "record replayed")
	require.Equal(t, []uint64{1, 2}, accIDs(fs), "side store replayed")
}

// accIDs returns the sorted ids in a fakeSide accumulator.
func accIDs(f *fakeSide) []uint64 {
	ids := make([]uint64, 0, len(f.acc))
	for id := range f.acc {
		ids = append(ids, id)
	}

	slices.Sort(ids)

	return ids
}

// TestSideStoreMergeUnions verifies a merge unions the sidecars of the compacted parts into the new
// part's sidecar (content-addressed dedup, no remap), keeping what the part's rows reference.
func TestSideStoreMergeUnions(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	be := backend.Memory()
	fs := newFakeSide()
	e := sideEngine(be, fs)

	b1 := mkBatch("api", rrec{ts: 1, body: "x", id: "1"}, rrec{ts: 1, body: "x", id: "2"})
	b1.Side = encodeSide(map[uint64][]byte{1: []byte("a"), 2: []byte("b")})
	ingest(t, e, b1)
	require.NoError(t, e.Flush(ctx))

	b2 := mkBatch("api", rrec{ts: 2, body: "y", id: "2"}, rrec{ts: 2, body: "y", id: "4"})
	b2.Side = encodeSide(map[uint64][]byte{2: []byte("b"), 4: []byte("d")})
	ingest(t, e, b2)
	require.NoError(t, e.Flush(ctx))

	require.Equal(t, 2, e.PartCount())
	require.Equal(t, 2, fs.stores, "each flush stores its sidecars")

	require.Equal(t, []uint64{1, 2, 4}, snapshotIDs(t, e))
	require.Equal(t, 2, fs.stores, "a read stays in memory")

	require.NoError(t, e.Merge(ctx, 0))
	require.Equal(t, 1, e.PartCount())
	require.GreaterOrEqual(t, fs.unions, 1)
	require.Equal(t, 3, fs.stores, "the merge stores its union")

	// One merged part, one sidecar, the union of both parts' symbols.
	require.Equal(t, []uint64{1, 2, 4}, sideIDs(t, be))
}
