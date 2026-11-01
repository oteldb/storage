package engine

import (
	"github.com/oteldb/storage/index/postings"
	"github.com/oteldb/storage/query/fetch"
	"github.com/oteldb/storage/signal"
)

func (h *head) stringPostings(nameID uint32, matcher fetch.Matcher) (postings.Postings, bool) {
	// Text equality can match typed values too; only an all-string label can skip the value scan.
	if _, mixed := h.nonStringLabels[nameID]; mixed {
		return nil, false
	}
	values := matcher.StringValues
	if matcher.Spec != nil {
		values = [][]byte{[]byte(matcher.Spec.Value)}
	}
	if len(values) == 0 {
		return nil, false
	}
	var scratch [256]byte
	var its []postings.Postings
	for _, text := range values {
		value := signal.StringValue(text)
		if !matcher.Match(value) {
			continue
		}
		encoded := signal.AppendValue(scratch[:0], value)
		if valueID, ok := h.sym.Lookup(encoded); ok {
			its = append(its, h.post.Get(nameID, uint32(valueID)))
		}
	}
	return postings.Merge(its...), true
}
