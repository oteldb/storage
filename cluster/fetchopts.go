package cluster

import (
	"encoding/binary"

	"github.com/go-faster/errors"

	"github.com/oteldb/storage/query/fetch"
)

func remoteFetchRequest(r fetch.Request) FetchRequest {
	out := FetchRequest{
		Signal: r.Signal, Tenant: string(r.Tenant), Start: r.Start, End: r.End,
		Equal:      fetch.EqualitySpecs(r.Matchers),
		Conditions: ConditionHints(r.Conditions, r.AllConditions),
	}
	// A residual condition may read a schema-defined attributes column; SecondPass can read
	// arbitrary columns. Without dependency metadata their input must remain fully materialized.
	if len(r.Conditions) == 0 && r.SecondPass == nil {
		out.Projection = r.Projection
	}
	// Equality hints on conditions are necessary predicates, not equivalent predicates. Limiting
	// their superset before the requester's recheck could discard rows that satisfy the full test.
	if len(out.Equal) == len(r.Matchers) && len(r.Conditions) == 0 && r.SecondPass == nil && r.Limit > 0 {
		out.Limit, out.Reverse = r.Limit, r.Reverse
	}

	return out
}

func (r FetchRequest) appendOptions(dst []byte) []byte {
	dst = binary.AppendUvarint(dst, uint64(len(r.Projection)))
	for _, name := range r.Projection {
		dst = appendString(dst, name)
	}
	dst = binary.AppendUvarint(dst, uint64(max(r.Limit, 0)))
	if r.Reverse {
		return append(dst, 1)
	}

	return append(dst, 0)
}

func takeFetchOptions(data []byte, r *FetchRequest) error {
	if len(data) == 0 {
		return nil
	}
	n, m := binary.Uvarint(data)
	if m <= 0 || n > uint64(len(data)-m) {
		return errors.New("cluster: malformed projection count")
	}
	data = data[m:]
	columns := data
	for range n {
		length, size := binary.Uvarint(data)
		if size <= 0 || length > uint64(len(data)-size) {
			return errors.New("cluster: malformed projection column")
		}
		data = data[size+int(length):]
	}
	limit, m := binary.Uvarint(data)
	if m <= 0 || limit > uint64(int(^uint(0)>>1)) {
		return errors.New("cluster: malformed fetch limit")
	}
	data = data[m:]
	if len(data) == 0 || data[0] > 1 {
		return errors.New("cluster: malformed fetch direction")
	}
	// Validate the complete extension before allocating its projection metadata.
	if n > 0 {
		r.Projection = make([]string, 0, n)
		for range n {
			name, rest, _ := takeString(columns)
			r.Projection = append(r.Projection, name)
			columns = rest
		}
	}
	r.Limit, r.Reverse = int(limit), limit > 0 && data[0] == 1

	return nil
}
