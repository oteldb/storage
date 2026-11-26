package engine

import (
	"github.com/oteldb/storage/index/postings"
	"github.com/oteldb/storage/internal/buildbudget"
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

// Exact string matchers reuse borrowed posting lists. Their intersection owns at
// most the shortest input's references.
func (h *head) matcherWorkspace(matchers []fetch.Matcher) int64 {
	var workspace int64
	perRef := buildbudget.Mul(int64(len(matchers))+1, 256)
	for owner := h; owner != nil; owner = owner.frozen {
		n := owner.exactMatcherBound(matchers)
		workspace = buildbudget.Add(workspace, buildbudget.Mul(int64(n), perRef))
	}
	return workspace
}

func (h *head) exactMatcherBound(matchers []fetch.Matcher) int {
	n := h.series.Len()
	if n == 0 {
		return 0
	}
	var scratch [256]byte
	for _, m := range matchers {
		if m.Spec == nil || len(m.Spec.Value) > 128 || m.Match(signal.EmptyValue()) {
			return h.series.Len()
		}
		name, ok := h.sym.Lookup(m.Name)
		if !ok {
			n = 0
			continue
		}
		if _, mixed := h.nonStringLabels[uint32(name)]; mixed {
			return h.series.Len()
		}
		encoded := signal.AppendValue(scratch[:0], signal.StringValue([]byte(m.Spec.Value)))
		value, ok := h.sym.Lookup(encoded)
		if !ok {
			n = 0
			continue
		}
		n = min(n, h.post.PostingCount(uint32(name), uint32(value)))
	}
	return max(n, 1)
}
