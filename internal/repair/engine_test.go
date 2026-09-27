package repair

import (
	"context"
	"sync"
	"testing"

	"github.com/go-faster/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/backend/bucketindex"
	"github.com/oteldb/storage/internal/obs"
)

type fakePart struct{ ent bucketindex.Entry }

// host is an engine reduced to its part set and index commit.
type host struct {
	mu, flush sync.Mutex

	wants   []bucketindex.Want
	holes   []bucketindex.Entry
	parts   []*fakePart
	pending []bucketindex.Want
	// disk is what Open finds, by prefix.
	disk      map[string]bucketindex.Entry
	commitErr error
	retired   []*fakePart
	commits   int
	adoptErr  error
}

func (h *host) Locks() (state, flush sync.Locker) { return &h.mu, &h.flush }

func (h *host) ObligationsLocked() ([]bucketindex.Want, []bucketindex.Entry) { return h.wants, h.holes }

func (h *host) LiveLocked() ([]*fakePart, []bucketindex.Entry) { return h.parts, nil }

func (h *host) Identity(p *fakePart) bucketindex.Entry { return p.ent }

func (h *host) Open(_ context.Context, prefix string) (*fakePart, error) {
	ent, ok := h.disk[prefix]
	if !ok {
		return nil, errors.New("not found")
	}

	return &fakePart{ent: ent}, nil
}

func (h *host) AdoptLocked(_ context.Context, p *fakePart, ent *bucketindex.Entry) error {
	p.ent = *ent

	return h.adoptErr
}

func (h *host) SwapLocked(parts []*fakePart, holes []bucketindex.Want) {
	h.parts, h.pending = parts, holes
}

func (h *host) CommitLocked(context.Context) error {
	h.commits++
	if h.commitErr != nil {
		return h.commitErr
	}

	// The commit discharges every want a live part now satisfies.
	ix := bucketindex.Index{}
	for _, p := range h.parts {
		ix.Entries = append(ix.Entries, p.ent)
	}

	kept := h.wants[:0]
	for i := range h.wants {
		w := h.wants[i]
		if _, ok := ix.Satisfying(w); !ok {
			kept = append(kept, w)
		}
	}

	h.wants = kept

	return nil
}

func (h *host) RetireLocked(parts []*fakePart) { h.retired = append(h.retired, parts...) }

func (h *host) Reclaim(context.Context) {}

func (h *host) prefixes() []string {
	out := make([]string, len(h.parts))
	for i, p := range h.parts {
		out[i] = p.ent.Prefix
	}

	return out
}

func drive(t *testing.T, s *State, h *host, f bucketindex.PartFetcher) {
	t.Helper()

	Drive(context.Background(), s, Config{Fetcher: f, Obs: obs.NewNop().Repair, Prefix: "test"}, h)
}

func TestDriveCommitsGroupAndRetiresAncestor(t *testing.T) {
	t.Parallel()

	ancestor := bucketindex.Entry{Prefix: "other", Blocks: bucketindex.Blocks(2)}
	members := group("f", 10, 3, bucketindex.Interval{Min: 1, Max: 2})
	h := &host{
		wants: []bucketindex.Want{want("lost", 1)},
		parts: []*fakePart{{ent: ancestor}},
		disk:  map[string]bucketindex.Entry{},
	}

	for _, m := range members {
		h.disk[m.Prefix] = m
	}

	var s State

	drive(t, &s, h, &peer{ix: bucketindex.Index{Entries: members}})

	assert.Equal(t, []string{"f00", "f01", "f02"}, h.prefixes())
	require.Len(t, h.retired, 1)
	assert.Equal(t, "other", h.retired[0].ent.Prefix, "the completed group subsumes the local ancestor")
	assert.Empty(t, h.wants)
	assert.Equal(t, int64(3), s.Stats().Fetched)

	drive(t, &s, h, &peer{})
	assert.Equal(t, 1, h.commits, "nothing is owed, so the next pass commits nothing")
}

func TestDriveCommitFailureRollsBack(t *testing.T) {
	t.Parallel()

	var s State

	h := &host{
		wants:     []bucketindex.Want{want("lost", 1)},
		commitErr: errors.New("cas lost"),
		adoptErr:  errors.New("identities"),
		disk:      map[string]bucketindex.Entry{"lost": want("lost", 1).Entry()},
	}

	drive(t, &s, h, nil)

	assert.Empty(t, h.parts, "the swap is undone")
	assert.Nil(t, h.pending)
	assert.Len(t, h.wants, 1)
	assert.Equal(t, int64(1), s.Stats().Local, "the part was held locally")
}

func TestDriveAcknowledgesLoss(t *testing.T) {
	t.Parallel()

	var s State

	h := &host{wants: []bucketindex.Want{want("lost", 1)}}

	for range HoleConfirmations {
		drive(t, &s, h, &peer{})
	}

	assert.Equal(t, []bucketindex.Want{want("lost", 1)}, h.pending)
	assert.Equal(t, int64(1), s.Stats().Lost)
	assert.Equal(t, int64(HoleConfirmations), s.Stats().Unsatisfiable)
}

func TestDriveHonorsContextWhileGated(t *testing.T) {
	t.Parallel()

	var s State

	s.once.Do(func() { s.gate = make(chan struct{}, 1) })
	s.gate <- struct{}{}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	h := &host{wants: []bucketindex.Want{want("lost", 1)}}
	Drive(ctx, &s, Config{Obs: obs.NewNop().Repair}, h)

	assert.Zero(t, s.Stats(), "a canceled waiter runs no pass")
}
