package cluster

import (
	"context"
	"encoding/binary"

	"github.com/go-faster/errors"

	"github.com/oteldb/storage/cluster/replica"
	"github.com/oteldb/storage/cluster/ring"
)

// A replicated write is framed as a tenant id followed by a WAL-encoded run of series+samples
// (applied by engine.ApplyReplicated). EncodeWrite/DecodeWrite are the wire form the replica
// transport carries; the tenant prefix tells the receiving node which engine to apply it to.

// EncodeWrite frames tenant ‖ walBytes into a replication payload.
func EncodeWrite(tenant string, walBytes []byte) []byte {
	buf := binary.AppendUvarint(make([]byte, 0, len(tenant)+len(walBytes)+4), uint64(len(tenant)))
	buf = append(buf, tenant...)

	return append(buf, walBytes...)
}

// DecodeWrite splits a payload made by [EncodeWrite] into the tenant id and the WAL bytes.
func DecodeWrite(data []byte) (tenant string, walBytes []byte, err error) {
	n, m := binary.Uvarint(data)
	if m <= 0 || n > uint64(len(data)-m) {
		return "", nil, errors.New("cluster: malformed write payload")
	}

	return string(data[m : m+int(n)]), data[m+int(n):], nil
}

// RingSource provides the current ring (e.g. an etcd-backed Membership).
type RingSource interface {
	Ring() *ring.Ring
}

// Writer is the cluster write path: it routes a tenant's write to that tenant's ring-owners
// and replicates it to a write quorum via the [replica.Replicator]. Sharding is by tenant —
// a whole tenant's stream is pinned to its owner set (locality); finer per-series sharding is
// a later refinement.
type Writer struct {
	rf     int
	ring   RingSource
	addrOf func(nodeID string) string // resolve a ring node ID to its network address
	repl   *replica.Replicator
}

// NewWriter constructs a cluster writer. addrOf maps a ring node ID to the network address the
// transport reaches it on (typically backed by the membership's Member.Addr).
func NewWriter(rf int, r RingSource, addrOf func(nodeID string) string, repl *replica.Replicator) *Writer {
	return &Writer{rf: rf, ring: r, addrOf: addrOf, repl: repl}
}

// Write replicates payload to the owners of tenant at write quorum. payload must be the
// [EncodeWrite] framing of this tenant's write (so a receiving node can apply it).
func (w *Writer) Write(ctx context.Context, tenant string, payload []byte) error {
	owners := w.ring.Ring().Lookup([]byte(tenant), w.rf)
	if len(owners) == 0 {
		return errors.New("cluster: no owners for tenant (empty ring)")
	}

	targets := make([]replica.Target, len(owners))
	for i, o := range owners {
		targets[i] = replica.Target{Addr: w.addrOf(o.ID)}
	}

	if err := w.repl.Replicate(ctx, targets, payload); err != nil {
		return errors.Wrapf(err, "replicate tenant %q", tenant)
	}

	return nil
}
