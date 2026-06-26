package cluster

import (
	"encoding/binary"

	"github.com/go-faster/errors"

	"github.com/oteldb/storage/signal"
)

// A replicated write is framed as a signal discriminator and tenant id followed by a WAL-encoded
// run of records (applied by the addressed signal's engine — metrics samples or log records).
// EncodeWrite/DecodeWrite are the wire form the replica transport carries; the signal + tenant
// prefix tells the receiving node which engine to apply it to.

// EncodeWrite frames signal ‖ tenant ‖ walBytes into a replication payload.
func EncodeWrite(sig signal.Signal, tenant string, walBytes []byte) []byte {
	buf := make([]byte, 0, len(tenant)+len(walBytes)+5)
	buf = append(buf, byte(sig))
	buf = binary.AppendUvarint(buf, uint64(len(tenant)))
	buf = append(buf, tenant...)

	return append(buf, walBytes...)
}

// DecodeWrite splits a payload made by [EncodeWrite] into the signal, tenant id, and WAL bytes.
func DecodeWrite(data []byte) (sig signal.Signal, tenant string, walBytes []byte, err error) {
	if len(data) < 1 {
		return 0, "", nil, errors.New("cluster: empty write payload")
	}

	sig = signal.Signal(data[0])
	data = data[1:]

	n, m := binary.Uvarint(data)
	if m <= 0 || n > uint64(len(data)-m) {
		return 0, "", nil, errors.New("cluster: malformed write payload")
	}

	return sig, string(data[m : m+int(n)]), data[m+int(n):], nil
}
