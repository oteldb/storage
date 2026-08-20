package etcd

import (
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
)

// FenceMargin is how far before a lease's nominal expiry a node stops trusting it. It has to
// cover the clock error between this node and etcd plus the delay between the deadline passing
// and the node noticing, and it comes out of the useful window, so it is a few seconds against
// [DefaultTTL]'s thirty. A TTL too small for it uses a third of the TTL instead, so a short-TTL
// cluster is fenced late in its window rather than permanently.
const FenceMargin = 5 * time.Second

// fenceMargin is [FenceMargin] clamped to this cluster's TTL.
func (m *Membership) fenceMargin() time.Duration { return min(FenceMargin, m.ttl/3) }

// SetClock overrides the clock the fence deadline is compared against ([Membership.Fenced]).
// nil restores [time.Now]. It exists because the deadline is the only wall-clock decision in
// this package, and a lease-fencing test that cannot move the clock has to sleep out a TTL.
func (m *Membership) SetClock(now func() time.Time) {
	if now == nil {
		m.clock.Store(nil)

		return
	}

	m.clock.Store(&now)
}

func (m *Membership) nowTime() time.Time {
	if fn := m.clock.Load(); fn != nil {
		return (*fn)()
	}

	return time.Now()
}

// noteKeepAlive records that etcd has just confirmed this node's lease.
func (m *Membership) noteKeepAlive() { m.lastKeepAlive.Store(time.Now().UnixNano()) }

// FenceDeadline is the instant after which this node can no longer prove it still holds its
// membership lease: the last keep-alive etcd answered, plus the TTL, less [FenceMargin]. Past
// it another node's [Ownership.Acquire] may already have succeeded, so everything the lease
// backs — every compaction claim, and with it the right to act as a shard's primary — must be
// treated as lost.
//
// The zero time means "already fenced": the node holds no lease, or knows its registration is
// gone and has not yet re-registered.
func (m *Membership) FenceDeadline() time.Time {
	if m.LeaseID() == clientv3.NoLease || m.selfAbsent.Load() {
		return time.Time{}
	}

	last := m.lastKeepAlive.Load()
	if last == 0 {
		return time.Time{}
	}

	return time.Unix(0, last).Add(m.ttl - m.fenceMargin())
}

// Fenced reports whether this node is past [Membership.FenceDeadline] and so can prove nothing
// about what it owns. It is deliberately not "can I reach etcd": a node unable to reach etcd but
// still inside a live lease is not wrong to serve as primary, and a node whose lease has lapsed
// is wrong whether or not etcd is reachable.
func (m *Membership) Fenced() bool {
	d := m.FenceDeadline()

	return d.IsZero() || !m.nowTime().Before(d)
}
