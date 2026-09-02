package cluster

import (
	"bytes"
	"encoding/binary"

	"github.com/go-faster/errors"

	"github.com/oteldb/storage/query/fetch"
	"github.com/oteldb/storage/signal"
)

// FetchRequest is the decoded cluster read RPC: what a peer is asked for, and the serializable
// subset of the predicate it may push down. Closures (a [fetch.Matcher]'s Match, a
// [fetch.Condition]'s Match) never cross the wire, so a peer answers with a superset the
// requester re-narrows through [fetch.Filter].
//
// The same encoding backs the enumeration RPCs (series, keys, side store), which carry no
// conditions.
type FetchRequest struct {
	Signal     signal.Signal
	Tenant     string
	Start, End int64

	// Equal are the series-identity equality matchers ([fetch.Matcher.Spec]): a peer resolves
	// postings with them, so they select *streams*.
	Equal []fetch.EqualMatcher
	// Conditions are the columnar equality hints ([fetch.Condition.Equal]): a peer prunes parts by
	// bloom and filters rows with them, so they select *records within* a stream. They are a
	// separate field because feeding a column equality into Equal would resolve it against the
	// identity index and match no stream at all.
	Conditions []ConditionHint
}

// ConditionHint is the serializable part of a [fetch.Condition]: the column (or record-attribute
// key) it filters and the exact value, or set of values, it requires.
type ConditionHint struct {
	Column string
	Equal  fetch.EqualMatcher
	// AnyEqual is [fetch.Condition.AnyEqual]: the set the column's value must belong to. It is
	// carried for the same reason Equal is — it is exact, so a peer applying it alone still answers
	// with a superset — and is what keeps an N-value equality (a resolved trace-id set) from being
	// dropped at the wire and degrading the peer's read to its whole window.
	AnyEqual [][]byte
}

// ConditionHints extracts the pushable hints of conds. Only an equality hint is carried: it is
// exact, and a condition that has one matches no row lacking that value (which is what already
// makes the bloom prune sound), so a peer applying it alone answers with a superset.
//
// Conditions are pushable only under [fetch.Request.AllConditions]: without it a producer may
// ignore conditions entirely, so a peer ANDing them could drop rows the requester wanted.
func ConditionHints(conds []fetch.Condition, all bool) []ConditionHint {
	if !all {
		return nil
	}

	var out []ConditionHint

	for i := range conds {
		h := ConditionHint{Column: conds[i].Column, AnyEqual: conds[i].AnyEqual}
		if eq := conds[i].Equal; eq != nil {
			h.Equal = *eq
		}

		if !h.hasEqual() && len(h.AnyEqual) == 0 {
			continue
		}

		out = append(out, h)
	}

	return out
}

// Condition rebuilds the peer-side [fetch.Condition]: the equality (or the set) both prunes parts
// by bloom and filters rows, standing in for the Match closure that could not be sent.
func (h ConditionHint) Condition() fetch.Condition {
	c := fetch.Condition{Column: h.Column, AnyEqual: h.AnyEqual}

	if h.hasEqual() {
		eq := h.Equal
		c.Equal, c.Match = &eq, eq.Predicate()

		return c
	}

	c.Match = fetch.AnyEqualPredicate(h.AnyEqual)

	return c
}

// hasEqual reports whether the hint carries an equality (as opposed to being set-only).
func (h ConditionHint) hasEqual() bool { return h.Equal.Name != "" || h.Equal.Value != "" }

// Encode frames the request as signal+tenant+window+matchers, then the condition hints.
//
// The hints are an **append-only tail**: a peer that predates them stops after the matchers and
// ignores the rest (it answers with the superset it always did), and a request written without
// them decodes here as no conditions. So neither side needs a version byte.
func (r FetchRequest) Encode() []byte {
	buf := []byte{byte(r.Signal)}
	buf = appendString(buf, r.Tenant)
	buf = binary.AppendVarint(buf, r.Start)
	buf = binary.AppendVarint(buf, r.End)
	buf = binary.AppendUvarint(buf, uint64(len(r.Equal)))

	for _, m := range r.Equal {
		buf = appendString(buf, m.Name)
		buf = appendString(buf, m.Value)
	}

	if len(r.Conditions) == 0 {
		return buf
	}

	buf = binary.AppendUvarint(buf, uint64(len(r.Conditions)))
	for _, c := range r.Conditions {
		buf = appendString(buf, c.Column)
		buf = appendString(buf, c.Equal.Name)
		buf = appendString(buf, c.Equal.Value)
	}

	return appendConditionSets(buf, r.Conditions)
}

// appendConditionSets writes the [ConditionHint.AnyEqual] sets as a second append-only tail —
// (condition index, members) pairs for the conditions that carry one, written only when at least
// one does. A peer that predates sets stops after the hints above and ignores the rest (it answers
// with the superset it always did, which [fetch.Filter] re-narrows), and a payload written without
// them decodes here as no sets. So, as with the hints themselves, neither side needs a version byte.
func appendConditionSets(dst []byte, conds []ConditionHint) []byte {
	n := 0

	for _, c := range conds {
		if len(c.AnyEqual) > 0 {
			n++
		}
	}

	if n == 0 {
		return dst
	}

	dst = binary.AppendUvarint(dst, uint64(n))

	for i, c := range conds {
		if len(c.AnyEqual) == 0 {
			continue
		}

		dst = binary.AppendUvarint(dst, uint64(i))
		dst = binary.AppendUvarint(dst, uint64(len(c.AnyEqual)))

		for _, v := range c.AnyEqual {
			dst = binary.AppendUvarint(dst, uint64(len(v)))
			dst = append(dst, v...)
		}
	}

	return dst
}

// takeConditionSets reads the tail [appendConditionSets] wrote back onto conds.
func takeConditionSets(data []byte, conds []ConditionHint) error {
	if len(data) == 0 {
		return nil // a request from a peer that predates set pushdown.
	}

	n, m := binary.Uvarint(data)
	if m <= 0 {
		return errors.New("cluster: malformed condition set count")
	}

	data = data[m:]

	for range n {
		idx, m := binary.Uvarint(data)
		if m <= 0 || idx >= uint64(len(conds)) {
			return errors.New("cluster: malformed condition set index")
		}

		data = data[m:]

		count, m := binary.Uvarint(data)
		if m <= 0 || count > uint64(len(data)) {
			return errors.New("cluster: malformed condition set size")
		}

		data = data[m:]

		set := make([][]byte, 0, count)

		for range count {
			l, m := binary.Uvarint(data)
			if m <= 0 || l > uint64(len(data)-m) {
				return errors.New("cluster: malformed condition set member")
			}

			data = data[m:]
			set = append(set, bytes.Clone(data[:l]))
			data = data[l:]
		}

		// Normalized here rather than trusted: the members drive a binary search on the peer side,
		// and the sender is another process.
		conds[idx].AnyEqual = fetch.AnyEqualSet(set)
	}

	return nil
}

// ParseFetchRequest decodes a request written by [FetchRequest.Encode].
func ParseFetchRequest(data []byte) (FetchRequest, error) {
	var r FetchRequest

	if len(data) < 1 {
		return FetchRequest{}, errors.New("cluster: empty fetch request")
	}

	r.Signal = signal.Signal(data[0])
	data = data[1:]

	var err error
	if r.Tenant, data, err = takeString(data); err != nil {
		return FetchRequest{}, errors.Wrap(err, "tenant")
	}

	var m int
	if r.Start, m = binary.Varint(data); m <= 0 {
		return FetchRequest{}, errors.New("cluster: malformed fetch request start")
	}
	data = data[m:]

	if r.End, m = binary.Varint(data); m <= 0 {
		return FetchRequest{}, errors.New("cluster: malformed fetch request end")
	}
	data = data[m:]

	count, m := binary.Uvarint(data)
	if m <= 0 {
		return FetchRequest{}, errors.New("cluster: malformed matcher count")
	}
	data = data[m:]

	r.Equal = make([]fetch.EqualMatcher, 0, count)

	for range count {
		var name, value string
		if name, data, err = takeString(data); err != nil {
			return FetchRequest{}, errors.Wrap(err, "matcher name")
		}

		if value, data, err = takeString(data); err != nil {
			return FetchRequest{}, errors.Wrap(err, "matcher value")
		}

		r.Equal = append(r.Equal, fetch.EqualMatcher{Name: name, Value: value})
	}

	if len(data) == 0 { // a request from a peer that predates condition pushdown.
		return r, nil
	}

	count, m = binary.Uvarint(data)
	if m <= 0 {
		return FetchRequest{}, errors.New("cluster: malformed condition count")
	}
	data = data[m:]

	r.Conditions = make([]ConditionHint, 0, count)

	for range count {
		var h ConditionHint
		if h.Column, data, err = takeString(data); err != nil {
			return FetchRequest{}, errors.Wrap(err, "condition column")
		}

		if h.Equal.Name, data, err = takeString(data); err != nil {
			return FetchRequest{}, errors.Wrap(err, "condition name")
		}

		if h.Equal.Value, data, err = takeString(data); err != nil {
			return FetchRequest{}, errors.Wrap(err, "condition value")
		}

		r.Conditions = append(r.Conditions, h)
	}

	if err := takeConditionSets(data, r.Conditions); err != nil {
		return FetchRequest{}, err
	}

	return r, nil
}

// Request rebuilds the [fetch.Request] a peer serves from its local store: the pushed-down
// equalities as identity matchers and column conditions, over the requested window.
func (r FetchRequest) Request() fetch.Request {
	out := fetch.Request{
		Signal: r.Signal, Tenant: signal.TenantID(r.Tenant), Start: r.Start, End: r.End,
		Matchers: make([]fetch.Matcher, len(r.Equal)),
	}

	for i := range r.Equal {
		eq := &r.Equal[i]
		out.Matchers[i] = fetch.Matcher{Name: []byte(eq.Name), Match: eq.Predicate(), Spec: eq}
	}

	if len(r.Conditions) > 0 {
		out.Conditions = make([]fetch.Condition, len(r.Conditions))
		for i, h := range r.Conditions {
			out.Conditions[i] = h.Condition()
		}

		out.AllConditions = true
	}

	return out
}
