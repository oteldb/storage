package cluster

import (
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
// key) it filters and the exact value it requires.
type ConditionHint struct {
	Column string
	Equal  fetch.EqualMatcher
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
		if eq := conds[i].Equal; eq != nil {
			out = append(out, ConditionHint{Column: conds[i].Column, Equal: *eq})
		}
	}

	return out
}

// Condition rebuilds the peer-side [fetch.Condition]: the equality both prunes parts by bloom and
// filters rows, standing in for the Match closure that could not be sent.
func (h ConditionHint) Condition() fetch.Condition {
	eq := h.Equal

	return fetch.Condition{Column: h.Column, Match: eq.Predicate(), Equal: &eq}
}

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

	return buf
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
