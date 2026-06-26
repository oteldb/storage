package profile

import (
	"context"
	"encoding/binary"
	"errors"
	"time"
)

// Active reports whether a profile collector is installed in ctx — i.e. whether the caller asked
// for EXPLAIN ANALYZE. A cluster client uses it to request a profile from a peer only when one is
// being collected.
func Active(ctx context.Context) bool {
	_, ok := ctx.Value(ctxKey{}).(*Handle)

	return ok
}

// Encode appends the binary encoding of the subtree rooted at n to dst (a peer's read RPC returns
// it so the requester can graft it). The shape is, recursively: name, duration (nanos, varint),
// sorted counters, then children.
func (n *Node) Encode(dst []byte) []byte {
	dst = appendStr(dst, n.Name)
	dst = binary.AppendVarint(dst, int64(n.Dur))

	keys := sortedKeys(n.Counters)
	dst = binary.AppendUvarint(dst, uint64(len(keys)))

	for _, k := range keys {
		dst = appendStr(dst, k)
		dst = binary.AppendVarint(dst, n.Counters[k])
	}

	dst = binary.AppendUvarint(dst, uint64(len(n.Children)))
	for _, c := range n.Children {
		dst = c.Encode(dst)
	}

	return dst
}

// Decode parses a [Node.Encode] payload, returning the node and the unconsumed tail. It bounds-checks
// every length so a corrupt or hostile payload never panics or over-allocates.
func Decode(data []byte) (*Node, []byte, error) {
	name, data, err := takeStr(data)
	if err != nil {
		return nil, nil, err
	}

	dur, m := binary.Varint(data)
	if m <= 0 {
		return nil, nil, errBad
	}

	data = data[m:]

	nc, m := binary.Uvarint(data)
	if m <= 0 || nc > uint64(len(data)) {
		return nil, nil, errBad
	}

	data = data[m:]

	n := &Node{Name: name, Dur: time.Duration(dur)}
	if nc > 0 {
		n.Counters = make(map[string]int64, nc)
		for range nc {
			var k string
			if k, data, err = takeStr(data); err != nil {
				return nil, nil, err
			}

			v, mm := binary.Varint(data)
			if mm <= 0 {
				return nil, nil, errBad
			}

			data = data[mm:]
			n.Counters[k] = v
		}
	}

	kids, m := binary.Uvarint(data)
	if m <= 0 || kids > uint64(len(data)) {
		return nil, nil, errBad
	}

	data = data[m:]
	for range kids {
		var child *Node
		if child, data, err = Decode(data); err != nil {
			return nil, nil, err
		}

		n.Children = append(n.Children, child)
	}

	return n, data, nil
}

var errBad = errors.New("profile: malformed encoding")

func appendStr(dst []byte, s string) []byte {
	dst = binary.AppendUvarint(dst, uint64(len(s)))

	return append(dst, s...)
}

func takeStr(data []byte) (string, []byte, error) {
	n, m := binary.Uvarint(data)
	if m <= 0 || n > uint64(len(data)-m) {
		return "", nil, errBad
	}

	return string(data[m : m+int(n)]), data[m+int(n):], nil
}
