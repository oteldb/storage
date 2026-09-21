// Package mergestreamtest is the conformance suite for a streaming merge: the invariants both
// engines must hold, expressed once so neither tests them its own way.
package mergestreamtest

import (
	"slices"
	"testing"

	"github.com/oteldb/storage/internal/mergestream"
	"github.com/oteldb/storage/signal"
)

// Union returns the ascending deduplicated union of src, built the obvious way. It is the reference
// [mergestream.Keys] must match.
func Union(src [][]signal.SeriesID) []signal.SeriesID {
	set := make(map[signal.SeriesID]struct{})
	for _, s := range src {
		for _, id := range s {
			set[id] = struct{}{}
		}
	}

	out := make([]signal.SeriesID, 0, len(set))
	for id := range set {
		out = append(out, id)
	}

	slices.SortFunc(out, signal.SeriesID.Compare)

	return out
}

// CheckKeys fails tb unless [mergestream.Keys] yields exactly [Union] over src. Each source must
// already be ascending. It is testify-free so a fuzz target can call it per input.
func CheckKeys(tb testing.TB, src [][]signal.SeriesID) {
	tb.Helper()

	sources := make([]mergestream.Source, len(src))
	for i, s := range src {
		sources[i] = mergestream.SeriesIDs(s)
	}

	var k mergestream.Keys

	k.Reset(sources)

	got := k.Append(nil)

	want := Union(src)
	if !slices.Equal(got, want) {
		tb.Fatalf("Keys: got %v, want %v (sources %v)", got, want, src)
	}
}
