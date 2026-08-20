package etcd

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	clientv3 "go.etcd.io/etcd/client/v3"
)

// fenceMembership builds a Membership with just the state the fence deadline reads, so the
// deadline arithmetic is testable without an etcd server.
func fenceMembership(ttl time.Duration, lease clientv3.LeaseID, lastKeepAlive time.Time) *Membership {
	m := &Membership{ttl: ttl, evicted: make(chan struct{}, 1)}
	m.leaseID.Store(int64(lease))

	if !lastKeepAlive.IsZero() {
		m.lastKeepAlive.Store(lastKeepAlive.UnixNano())
	}

	return m
}

func TestFenceDeadlineTracksTheLastKeepAlive(t *testing.T) {
	t.Parallel()

	base := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)

	for _, tt := range []struct {
		name  string
		ttl   time.Duration
		lease clientv3.LeaseID
		last  time.Time
		zero  bool
		want  time.Time
	}{
		{
			name: "no lease is already fenced",
			ttl:  DefaultTTL, lease: clientv3.NoLease, last: base, zero: true,
		},
		{
			name: "no keep-alive yet is already fenced",
			ttl:  DefaultTTL, lease: 7, zero: true,
		},
		{
			name: "the margin comes out of the window",
			ttl:  DefaultTTL, lease: 7, last: base, want: base.Add(DefaultTTL - FenceMargin),
		},
		{
			// A TTL below three margins would otherwise fence the node before its lease could
			// ever be renewed, i.e. permanently.
			name: "a short TTL clamps the margin to a third",
			ttl:  3 * time.Second, lease: 7, last: base, want: base.Add(2 * time.Second),
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got := fenceMembership(tt.ttl, tt.lease, tt.last).FenceDeadline()
			if tt.zero {
				assert.True(t, got.IsZero(), "deadline is the zero time")

				return
			}

			assert.Truef(t, tt.want.Equal(got), "deadline %s, want %s", got, tt.want)
		})
	}
}

func TestFencedComparesAgainstTheInjectedClock(t *testing.T) {
	t.Parallel()

	base := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	m := fenceMembership(DefaultTTL, 7, base)

	now := base
	m.SetClock(func() time.Time { return now })

	assert.False(t, m.Fenced(), "a lease just confirmed is not fenced")

	now = base.Add(DefaultTTL - FenceMargin - time.Nanosecond)
	assert.False(t, m.Fenced(), "the last instant before the deadline is still inside the lease")

	now = base.Add(DefaultTTL - FenceMargin)
	assert.True(t, m.Fenced(), "the deadline itself is out: another node may already have acquired")

	// A keep-alive answered again lifts the fence without any rejoin.
	m.noteKeepAlive()
	now = time.Now().Add(time.Second)
	assert.False(t, m.Fenced(), "a confirmed lease un-fences the node in place")

	// Losing the registration fences immediately, without waiting for the deadline.
	m.selfAbsent.Store(true)
	assert.True(t, m.Fenced(), "a node that knows it is unregistered holds nothing")
}

func TestOwnershipFenceDisclaimsEverything(t *testing.T) {
	t.Parallel()

	o := &Ownership{held: map[string]uint64{"tenant-a": 42}}

	term, ok := o.Term("tenant-a")
	require.True(t, ok)
	assert.Equal(t, uint64(42), term)
	assert.Equal(t, []string{"tenant-a"}, o.Owned())

	fenced := true
	o.SetFence(func() bool { return fenced })

	_, ok = o.Term("tenant-a")
	assert.False(t, ok, "a fenced claim cannot be stamped into an index")
	assert.Empty(t, o.Owned(), "a fenced node owns nothing, so it flushes and merges nothing")

	owned, err := o.Reconcile(t.Context(), nil, []string{"tenant-a"})
	require.NoError(t, err)
	assert.Empty(t, owned)

	// The held set survives the fence, so a lease confirmed again resumes the claims in place.
	fenced = false
	term, ok = o.Term("tenant-a")
	require.True(t, ok)
	assert.Equal(t, uint64(42), term)
}
