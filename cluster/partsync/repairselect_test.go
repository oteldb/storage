package partsync_test

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/backend"
	"github.com/oteldb/storage/backend/bucketindex"
	"github.com/oteldb/storage/cluster/partsync"
)

// probePeer is a peer server that counts the object fetches it answers, and can be made to lag or
// to fail its bucket index — the two shapes selection has to stay correct under.
type probePeer struct {
	addr string

	mu      sync.Mutex
	fetches map[string]int
	lists   int
}

type probeOpts struct {
	// delay is added to every response, to vary which peer answers first.
	delay time.Duration
	// failIndex makes the bucket index answer 500: a peer we could not ask, not one that answered
	// it has nothing.
	failIndex bool
}

func servePeer(t *testing.T, be backend.Backend, prefix string, opts probeOpts) *probePeer {
	t.Helper()

	p := &probePeer{fetches: make(map[string]int)}
	indexKey := prefix + "/" + bucketindex.Object
	object := partsync.ObjectHandler(be)
	list := partsync.ListHandler(be)

	mux := http.NewServeMux()
	mux.HandleFunc(partsync.ListPath, func(w http.ResponseWriter, req *http.Request) {
		p.mu.Lock()
		p.lists++
		p.mu.Unlock()

		list.ServeHTTP(w, req)
	})
	mux.HandleFunc(partsync.ObjectPath, func(w http.ResponseWriter, req *http.Request) {
		key := req.URL.Query().Get("key")

		p.mu.Lock()
		p.fetches[key]++
		p.mu.Unlock()

		if opts.delay > 0 {
			time.Sleep(opts.delay)
		}

		if opts.failIndex && key == indexKey {
			http.Error(w, "index unavailable", http.StatusInternalServerError)

			return
		}

		object.ServeHTTP(w, req)
	})

	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)

	p.addr = strings.TrimPrefix(srv.URL, "http://")

	return p
}

func (p *probePeer) count(key string) int {
	p.mu.Lock()
	defer p.mu.Unlock()

	return p.fetches[key]
}

func (p *probePeer) listCalls() int {
	p.mu.Lock()
	defer p.mu.Unlock()

	return p.lists
}

func (p *probePeer) indexFetches(prefix string) int {
	return p.count(prefix + "/" + bucketindex.Object)
}

// wantAt is a want for one block, which a wider part containing that block satisfies.
func wantAt(prefix string, block uint64) bucketindex.Want {
	return bucketindex.Want{
		Prefix: prefix,
		Blocks: bucketindex.Interval{Min: block, Max: block},
	}
}

// TestFetchWantsReadsEachIndexOncePerCycle pins the per-cycle cost: a cycle's wants share one read
// of each peer's index, so the fetch count is O(peers), not O(peers × wants).
func TestFetchWantsReadsEachIndexOncePerCycle(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	const prefix = "default/logs"

	peers := make([]*probePeer, 3)
	addrs := make([]string, len(peers))

	for i := range peers {
		be := backend.Memory()
		ix := &bucketindex.Index{}
		writeBlockPart(t, be, ix, prefix, "0100", bucketindex.Interval{Min: 1, Max: 8}, 1)
		saveIndex(t, be, prefix, ix)

		peers[i] = servePeer(t, be, prefix, probeOpts{})
		addrs[i] = peers[i].addr
	}

	wants := []bucketindex.Want{
		wantAt(prefix+"/0001", 1),
		wantAt(prefix+"/0002", 2),
		wantAt(prefix+"/0003", 3),
		wantAt(prefix+"/0004", 4),
	}

	s := partsync.New(backend.Memory(), &partsync.Client{}, partsync.WithRandSeed(7))

	for _, r := range s.FetchWants(ctx, prefix, addrs, wants) {
		require.NoError(t, r.Err)
		require.True(t, r.OK)
	}

	total := 0
	for _, p := range peers {
		n := p.indexFetches(prefix)
		assert.LessOrEqual(t, n, 1, "a peer's index is read at most once for the whole cycle")

		total += n
	}

	assert.Equal(t, len(peers), total, "one index read per peer, not one per peer per want")
}

// TestFetchWantsCopiesASharedSuccessorOnce pins the fetch-side dedupe: two wants answered by the
// same merged successor move that part over the network once.
func TestFetchWantsCopiesASharedSuccessorOnce(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	const prefix = "default/logs"

	be := backend.Memory()
	ix := &bucketindex.Index{}
	merged := writeBlockPart(t, be, ix, prefix, "0100", bucketindex.Interval{Min: 1, Max: 4}, 1)
	saveIndex(t, be, prefix, ix)

	peer := servePeer(t, be, prefix, probeOpts{})

	s := partsync.New(backend.Memory(), &partsync.Client{}, partsync.WithRandSeed(1))

	res := s.FetchWants(ctx, prefix, []string{peer.addr}, []bucketindex.Want{
		wantAt(prefix+"/0001", 1),
		wantAt(prefix+"/0002", 2),
	})

	require.Len(t, res, 2)

	for _, r := range res {
		require.NoError(t, r.Err)
		require.True(t, r.OK)
		assert.Equal(t, merged, r.Entry.Prefix)
	}

	assert.Equal(t, 1, peer.count(merged+"/manifest"), "one copy discharges both wants")
	assert.Equal(t, 1, peer.count(merged+"/c/0"))
	assert.Equal(t, 1, peer.listCalls(),
		"the part is enumerated once too — per-want copies would each list it, and concurrent ones "+
			"would each find it locally absent and re-fetch every object")
}

// TestFetchWantsSelectionIsOrderIndependent stages peers that answer at very different speeds and
// pins that the widest entry still wins. Taking the first good answer would make the winner a
// function of the network.
func TestFetchWantsSelectionIsOrderIndependent(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	const prefix = "default/logs"

	// The peer holding the best part is the slowest, then the fastest, then in between.
	for _, slow := range []time.Duration{0, 20 * time.Millisecond, 5 * time.Millisecond} {
		for seed := range uint64(4) {
			narrow := backend.Memory()
			nix := &bucketindex.Index{}
			writeBlockPart(t, narrow, nix, prefix, "0002", bucketindex.Interval{Min: 2, Max: 2}, 0)
			saveIndex(t, narrow, prefix, nix)

			wide := backend.Memory()
			wix := &bucketindex.Index{}
			big := writeBlockPart(t, wide, wix, prefix, "0200", bucketindex.Interval{Min: 1, Max: 8}, 2)
			saveIndex(t, wide, prefix, wix)

			fast := servePeer(t, narrow, prefix, probeOpts{})
			lagging := servePeer(t, wide, prefix, probeOpts{delay: slow})

			s := partsync.New(backend.Memory(), &partsync.Client{}, partsync.WithRandSeed(seed))

			ent, ok, err := s.FetchWant(ctx, prefix, []string{fast.addr, lagging.addr}, wantAt(prefix+"/0002", 2))
			require.NoError(t, err)
			require.True(t, ok)
			assert.Equal(t, big, ent.Prefix, "the widest entry wins whoever answered first")
		}
	}
}

// TestFetchWantsSpreadsEqualCandidates pins the load-spreading half: peers offering an identical
// entry are picked in shuffled order, so recovering nodes do not all pull from the same one. The
// same seed reproduces the same choice.
func TestFetchWantsSpreadsEqualCandidates(t *testing.T) {
	t.Parallel()

	const prefix = "default/logs"

	// pick runs one repair against three peers holding the very same part and reports which one
	// served the copy.
	pick := func(t *testing.T, seed uint64) int {
		t.Helper()

		peers, part := stageIdenticalPeers(t, prefix, 3)

		addrs := make([]string, len(peers))
		for i, p := range peers {
			addrs[i] = p.addr
		}

		s := partsync.New(backend.Memory(), &partsync.Client{}, partsync.WithRandSeed(seed))

		ent, ok, err := s.FetchWant(context.Background(), prefix, addrs, wantAt(prefix+"/0001", 1))
		require.NoError(t, err)
		require.True(t, ok)
		require.Equal(t, part, ent.Prefix)

		served := -1

		for i, p := range peers {
			if p.count(part+"/manifest") > 0 {
				require.Equal(t, -1, served, "exactly one peer serves the copy")

				served = i
			}
		}

		require.NotEqual(t, -1, served)

		return served
	}

	chosen := make(map[int]int)
	for seed := range uint64(24) {
		chosen[pick(t, seed)]++
	}

	assert.Greater(t, len(chosen), 1,
		"equal candidates must not always resolve to the same peer: got %v", chosen)

	first, again := pick(t, 3), pick(t, 3)
	assert.Equal(t, first, again, "a seed reproduces its choice")
}

// stageIdenticalPeers serves n peers whose indexes offer the very same entry, which is the case
// betterCandidate cannot separate and the shuffle has to.
func stageIdenticalPeers(t *testing.T, prefix string, n int) (peers []*probePeer, part string) {
	t.Helper()

	peers = make([]*probePeer, n)

	for i := range peers {
		be := backend.Memory()
		ix := &bucketindex.Index{}
		part = writeBlockPart(t, be, ix, prefix, "0100", bucketindex.Interval{Min: 1, Max: 4}, 1)
		saveIndex(t, be, prefix, ix)

		peers[i] = servePeer(t, be, prefix, probeOpts{})
	}

	return peers, part
}

// TestFetchWantsAggregatesPeerErrors is the guard #533's hole commit rests on: with one peer that
// could not be asked and the rest legitimately lacking the part, the answer must be a transient
// failure. Clean absence here is what lets the engine acknowledge a hole over live data.
func TestFetchWantsAggregatesPeerErrors(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	const prefix = "default/logs"

	// The seed decides where the broken peer lands in the shuffle; every position must report it.
	for seed := range uint64(8) {
		addrs := make([]string, 0, 4)

		for i := range 4 {
			be := backend.Memory()
			ix := &bucketindex.Index{}
			writeBlockPart(t, be, ix, prefix, "0009", bucketindex.Interval{Min: 9, Max: 9}, 0)
			saveIndex(t, be, prefix, ix)

			addrs = append(addrs, servePeer(t, be, prefix, probeOpts{failIndex: i == 0}).addr)
		}

		s := partsync.New(backend.Memory(), &partsync.Client{}, partsync.WithRandSeed(seed))

		res := s.FetchWants(ctx, prefix, addrs, []bucketindex.Want{
			wantAt(prefix+"/0001", 1),
			wantAt(prefix+"/0002", 2),
		})
		require.Len(t, res, 2)

		for _, r := range res {
			assert.False(t, r.OK)
			require.Error(t, r.Err, "a peer we could not ask must never read as definitive absence")
		}
	}
}

// TestFetchWantsRejectsBadPrefixForEveryWant keeps the positional contract under the early return.
func TestFetchWantsRejectsBadPrefixForEveryWant(t *testing.T) {
	t.Parallel()

	s := partsync.New(backend.Memory(), &partsync.Client{})

	res := s.FetchWants(context.Background(), "../escape", nil, []bucketindex.Want{
		{Prefix: "a"}, {Prefix: "b"},
	})
	require.Len(t, res, 2)

	for _, r := range res {
		assert.False(t, r.OK)
		assert.Error(t, r.Err)
	}
}

// TestFetchWantsEmptyBatch asks nothing of the peers.
func TestFetchWantsEmptyBatch(t *testing.T) {
	t.Parallel()

	s := partsync.New(backend.Memory(), &partsync.Client{})
	assert.Empty(t, s.FetchWants(context.Background(), "default/logs", []string{"127.0.0.1:1"}, nil))
}
