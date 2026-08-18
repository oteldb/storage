package etcd

import (
	"context"
	"path"

	clientv3 "go.etcd.io/etcd/client/v3"
)

// Watch returns a read-only [Membership]: it snapshots the member set and follows it, exposing the
// same live ring [Join] does, but registers nothing. The caller takes no lease, appears in no
// other node's ring, and is therefore never placed as an owner — which is what a stateless tier
// (a query or ingest process) needs to route by the ring without holding data for it.
//
// The returned value is otherwise an ordinary Membership: [Membership.Ring], [Membership.AddrOf]
// and [Membership.Members] behave identically. [Membership.LeaseID] is zero, and [Membership.Close]
// stops the watch without revoking anything.
func Watch(ctx context.Context, client *clientv3.Client, root string) (*Membership, error) {
	prefix := path.Join(root, "members") + "/"

	m := &Membership{
		client:  client,
		prefix:  prefix,
		members: make(map[string]Member),
	}

	// Snapshot first, then watch from the snapshot revision, so no change falls into the gap
	// between the two — the same ordering [Join] relies on.
	rev, err := m.resync(ctx)
	if err != nil {
		return nil, err
	}

	bg, cancel := context.WithCancel(context.Background())
	m.cancel = cancel
	m.wg.Add(1)

	go m.watch(bg, rev) //nolint:contextcheck // lifetime-scoped, as in Join

	return m, nil
}
