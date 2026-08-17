package etcd

import (
	"context"
	"path"

	"github.com/go-faster/errors"
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
	resp, err := client.Get(ctx, prefix, clientv3.WithPrefix())
	if err != nil {
		return nil, errors.Wrap(err, "list members")
	}

	for _, kv := range resp.Kvs {
		if mem, err := decodeMember(kv.GetValue()); err == nil {
			m.members[mem.ID] = mem
		}
	}

	m.rebuild()

	bg, cancel := context.WithCancel(context.Background())
	m.cancel = cancel
	m.wg.Add(1)

	go m.watch(bg, resp.Header.GetRevision()+1) //nolint:contextcheck // lifetime-scoped, as in Join

	return m, nil
}
