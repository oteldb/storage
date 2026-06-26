package cluster

import (
	"encoding/binary"

	"github.com/go-faster/errors"
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
