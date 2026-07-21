package partsync_test

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/backend"
	"github.com/oteldb/storage/backend/bucketindex"
	"github.com/oteldb/storage/cluster/partsync"
)

// serve mounts the partsync endpoints over be on an httptest server and returns its
// host:port (the addr form the client expects).
func serve(t *testing.T, be backend.Backend) string {
	t.Helper()

	mux := http.NewServeMux()
	mux.Handle(partsync.ListPath, partsync.ListHandler(be))
	mux.Handle(partsync.ObjectPath, partsync.ObjectHandler(be))

	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)

	return strings.TrimPrefix(srv.URL, "http://")
}

// writePart writes a minimal fake part (columns + marks, manifest last) under prefix/seq and
// registers it in ix.
func writePart(t *testing.T, be backend.Backend, ix *bucketindex.Index, prefix string, seq int, minT, maxT int64) {
	t.Helper()
	ctx := context.Background()

	p := prefix + "/000000000" + string(rune('0'+seq))
	require.NoError(t, be.Write(ctx, p+"/c/0", []byte("col0-"+p)))
	require.NoError(t, be.Write(ctx, p+"/marks", []byte("marks-"+p)))
	require.NoError(t, be.Write(ctx, p+"/manifest", []byte("manifest-"+p)))
	ix.Add(bucketindex.Entry{Prefix: p, MinTime: minT, MaxTime: maxT})
}

// saveIndex persists ix as prefix's bucket index.
func saveIndex(t *testing.T, be backend.Backend, prefix string, ix *bucketindex.Index) {
	t.Helper()
	require.NoError(t, ix.Save(context.Background(), be, prefix+"/"+bucketindex.Object))
}

func TestSyncMirrorsNewerPeer(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	owner, replica := backend.Memory(), backend.Memory()

	ix := &bucketindex.Index{}
	writePart(t, owner, ix, "default/metrics", 1, 100, 200)
	writePart(t, owner, ix, "default/metrics", 2, 300, 400)
	saveIndex(t, owner, "default/metrics", ix)
	require.NoError(t, owner.Write(ctx, "default/metrics/series.bin", []byte("series-v1")))

	s := partsync.New(replica, &partsync.Client{})
	st, err := s.Sync(ctx, "default/metrics", []string{serve(t, owner)}, false)
	require.NoError(t, err)
	require.True(t, st.Synced)
	assert.Equal(t, 8, st.Copied, "2 parts × 3 objects + series.bin (mutable) + the index installed last")

	// The replica's backend now mirrors the owner: every object present and byte-identical.
	keys, err := owner.List(ctx, "default/metrics")
	require.NoError(t, err)
	require.NotEmpty(t, keys)

	for _, k := range keys {
		want, err := owner.Read(ctx, k)
		require.NoError(t, err)
		got, err := replica.Read(ctx, k)
		require.NoErrorf(t, err, "replica holds %q", k)
		assert.Equalf(t, want, got, "object %q mirrored verbatim", k)
	}

	// A second pass is the fast path: nothing new.
	st, err = s.Sync(ctx, "default/metrics", []string{serve(t, owner)}, false)
	require.NoError(t, err)
	assert.False(t, st.Synced, "identical index ⇒ no-op")
	assert.Zero(t, st.Copied)
}

func TestSyncSkipsOlderPeer(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	stale, local := backend.Memory(), backend.Memory()

	// The peer only has part 1; the local copy already has parts 1 and 2.
	pix := &bucketindex.Index{}
	writePart(t, stale, pix, "default/metrics", 1, 100, 200)
	saveIndex(t, stale, "default/metrics", pix)

	lix := &bucketindex.Index{}
	writePart(t, local, lix, "default/metrics", 1, 100, 200)
	writePart(t, local, lix, "default/metrics", 2, 300, 400)
	saveIndex(t, local, "default/metrics", lix)

	s := partsync.New(local, &partsync.Client{})

	for _, strict := range []bool{false, true} {
		st, err := s.Sync(ctx, "default/metrics", []string{serve(t, stale)}, strict)
		require.NoError(t, err)
		assert.Falsef(t, st.Synced, "strict=%v: an older peer never overwrites a newer local copy", strict)
	}

	// Local part 2 survived.
	_, err := local.Read(ctx, "default/metrics/0000000002/manifest")
	require.NoError(t, err)
}

func TestSyncStrictRequiresStrictlyNewer(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	peer, local := backend.Memory(), backend.Memory()

	ix := &bucketindex.Index{}
	writePart(t, peer, ix, "default/metrics", 1, 100, 200)
	saveIndex(t, peer, "default/metrics", ix)

	// Local has the same generation (same index bytes).
	s := partsync.New(local, &partsync.Client{})
	st, err := s.Sync(ctx, "default/metrics", []string{serve(t, peer)}, false)
	require.NoError(t, err)
	require.True(t, st.Synced, "bootstrap mirror")

	// Same-generation peer: strict (owner) skips, non-strict is the byte-equal fast path.
	st, err = s.Sync(ctx, "default/metrics", []string{serve(t, peer)}, true)
	require.NoError(t, err)
	assert.False(t, st.Synced, "strict: equal generation is not newer")
}

func TestSyncPicksNewestPeer(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	old, cur, local := backend.Memory(), backend.Memory(), backend.Memory()

	oix := &bucketindex.Index{}
	writePart(t, old, oix, "t/metrics", 1, 100, 200)
	saveIndex(t, old, "t/metrics", oix)

	cix := &bucketindex.Index{}
	writePart(t, cur, cix, "t/metrics", 1, 100, 200)
	writePart(t, cur, cix, "t/metrics", 3, 500, 600)
	saveIndex(t, cur, "t/metrics", cix)

	s := partsync.New(local, &partsync.Client{})
	st, err := s.Sync(ctx, "t/metrics", []string{serve(t, old), serve(t, cur), "127.0.0.1:1"}, false)
	require.NoError(t, err)
	require.True(t, st.Synced)

	// The newest peer's copy won (part 3 present); the unreachable peer was skipped.
	_, err = local.Read(ctx, "t/metrics/0000000003/manifest")
	require.NoError(t, err, "mirrored from the newest peer")
}

func TestSyncPrunesAfterTwoMisses(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	owner, replica := backend.Memory(), backend.Memory()

	ix := &bucketindex.Index{}
	writePart(t, owner, ix, "t/metrics", 1, 100, 200)
	writePart(t, owner, ix, "t/metrics", 2, 300, 400)
	saveIndex(t, owner, "t/metrics", ix)
	addr := serve(t, owner)

	s := partsync.New(replica, &partsync.Client{})
	_, err := s.Sync(ctx, "t/metrics", []string{addr}, false)
	require.NoError(t, err)

	// The owner merges parts 1+2 into part 3: old objects go away, a new index appears.
	for _, k := range []string{"t/metrics/0000000001/c/0", "t/metrics/0000000001/marks", "t/metrics/0000000001/manifest",
		"t/metrics/0000000002/c/0", "t/metrics/0000000002/marks", "t/metrics/0000000002/manifest"} {
		require.NoError(t, owner.Delete(ctx, k))
	}

	mix := &bucketindex.Index{}
	writePart(t, owner, mix, "t/metrics", 3, 100, 400)
	saveIndex(t, owner, "t/metrics", mix)

	// Pass 1 after the merge: the replica mirrors part 3; stale objects are counted, not deleted.
	st, err := s.Sync(ctx, "t/metrics", []string{addr}, false)
	require.NoError(t, err)
	require.True(t, st.Synced)
	assert.Zero(t, st.Pruned, "first absence: quarantined, not pruned")
	_, err = replica.Read(ctx, "t/metrics/0000000001/manifest")
	require.NoError(t, err, "stale part objects still present after one miss")

	// The index changed again (epoch-less same-generation edge is fine: same maxSeq but the
	// owner rewrote nothing — force a differing index by another flush).
	fix := &bucketindex.Index{}
	fix.Add(bucketindex.Entry{Prefix: "t/metrics/0000000003", MinTime: 100, MaxTime: 400})
	writePart(t, owner, fix, "t/metrics", 4, 500, 600)
	saveIndex(t, owner, "t/metrics", fix)

	// Pass 2: second consecutive absence ⇒ pruned.
	st, err = s.Sync(ctx, "t/metrics", []string{addr}, false)
	require.NoError(t, err)
	require.True(t, st.Synced)
	assert.Equal(t, 6, st.Pruned, "both stale parts' objects deleted on the second miss")

	_, err = replica.Read(ctx, "t/metrics/0000000001/manifest")
	require.ErrorIs(t, err, backend.ErrNotExist, "stale object gone")
	_, err = replica.Read(ctx, "t/metrics/0000000003/manifest")
	require.NoError(t, err, "live objects untouched")
}

func TestFetchChecksumMismatch(t *testing.T) {
	t.Parallel()

	// A server that lies: valid frame, wrong checksum.
	mux := http.NewServeMux()
	mux.Handle(partsync.ObjectPath, http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("X-Checksum-Xxh3", "deadbeef")
		_, _ = w.Write([]byte("payload"))
	}))
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)

	c := &partsync.Client{}
	_, err := c.Fetch(context.Background(), strings.TrimPrefix(srv.URL, "http://"), "k")
	require.ErrorContains(t, err, "checksum mismatch")
}

func TestClientFetchNotExist(t *testing.T) {
	t.Parallel()

	be := backend.Memory()
	c := &partsync.Client{}
	_, err := c.Fetch(context.Background(), serve(t, be), "missing")
	require.ErrorIs(t, err, partsync.ErrNotExist)
}

func TestClientListRoundTrip(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	be := backend.Memory()
	require.NoError(t, be.Write(ctx, "a/1", []byte("x")))
	require.NoError(t, be.Write(ctx, "a/2", []byte("y")))
	require.NoError(t, be.Write(ctx, "b/1", []byte("z")))

	c := &partsync.Client{}
	addr := serve(t, be)

	keys, err := c.List(ctx, addr, "a/")
	require.NoError(t, err)
	assert.ElementsMatch(t, []string{"a/1", "a/2"}, keys)

	keys, err = c.List(ctx, addr, "")
	require.NoError(t, err)
	assert.Len(t, keys, 3)
}

func TestSyncNoPeersIsNoop(t *testing.T) {
	t.Parallel()

	s := partsync.New(backend.Memory(), &partsync.Client{})
	st, err := s.Sync(context.Background(), "t/metrics", nil, false)
	require.NoError(t, err)
	assert.False(t, st.Synced)

	// Unreachable-only peers are also a clean no-op.
	st, err = s.Sync(context.Background(), "t/metrics", []string{"127.0.0.1:1"}, false)
	require.NoError(t, err)
	assert.False(t, st.Synced)
}

func TestHandlersRejectHostileKeys(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	be := backend.Memory()
	require.NoError(t, be.Write(ctx, "t/metrics/secret", []byte("data")))
	addr := serve(t, be)
	c := &partsync.Client{}

	// Traversal, absolute, backslash, and empty keys are rejected at the boundary — before any
	// backend touch — regardless of the backend's own validation.
	for _, key := range []string{"../etc/passwd", "t/../../etc/passwd", "/etc/passwd", `t\metrics`, ""} {
		_, err := c.Fetch(ctx, addr, key)
		require.Errorf(t, err, "key %q rejected", key)
		require.NotErrorIsf(t, err, partsync.ErrNotExist, "key %q is a 400, not a 404", key)
	}

	for _, prefix := range []string{"../", "a/../../b", "/abs", `a\b`} {
		_, err := c.List(ctx, addr, prefix)
		require.Errorf(t, err, "prefix %q rejected", prefix)
	}

	// The empty prefix (full listing) stays allowed.
	keys, err := c.List(ctx, addr, "")
	require.NoError(t, err)
	assert.Len(t, keys, 1)
}

func TestClientNotify(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	var got []string

	mux := http.NewServeMux()
	mux.Handle(partsync.NotifyPath, http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		got = append(got, req.URL.Query().Get("prefix"))
		w.WriteHeader(http.StatusAccepted)
	}))
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)
	addr := strings.TrimPrefix(srv.URL, "http://")

	c := &partsync.Client{}
	require.NoError(t, c.Notify(ctx, addr, "default/metrics"))
	assert.Equal(t, []string{"default/metrics"}, got)

	// An erroring peer surfaces (the caller treats it as advisory).
	bad := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		http.Error(w, "nope", http.StatusInternalServerError)
	}))
	t.Cleanup(bad.Close)
	require.Error(t, c.Notify(ctx, strings.TrimPrefix(bad.URL, "http://"), "default/metrics"))
}
