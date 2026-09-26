package profile

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/signal"
)

// buildStack adds a one-frame stack (function name, file) to d and returns its stack index.
func buildStack(d *Dictionary, fn, file string) int32 {
	f := d.AddFunction(Function{
		NameStrindex:     d.InternString([]byte(fn)),
		FilenameStrindex: d.InternString([]byte(file)),
	})
	loc := d.AddLocation(Location{Lines: []Line{{FunctionIndex: f, Line: 10}}})

	return d.AddStack(loc)
}

// TestContentAddressStable verifies that identical stack content yields the same stack id regardless
// of the surrounding dictionary's index layout — the property that makes the symbol-store union a
// pure dedup.
func TestContentAddressStable(t *testing.T) {
	t.Parallel()

	d1 := &Dictionary{}
	id1 := newBuilder(d1).stackID(buildStack(d1, "main", "main.go"))

	// A different dictionary with unrelated padding entries before the same stack.
	d2 := &Dictionary{}
	buildStack(d2, "other", "other.go") // padding ⇒ different indices
	id2 := newBuilder(d2).stackID(buildStack(d2, "main", "main.go"))

	assert.Equal(t, id1, id2, "same stack content ⇒ same id across dictionaries")

	// Different content ⇒ different id.
	d3 := &Dictionary{}
	id3 := newBuilder(d3).stackID(buildStack(d3, "main", "other.go"))
	assert.NotEqual(t, id1, id3)
}

// TestBuilderDeltaDedups verifies the builder records each transitively-referenced symbol once, and
// that two stacks sharing a function share its string/function entries.
func TestBuilderDeltaDedups(t *testing.T) {
	t.Parallel()

	d := &Dictionary{}
	shared := d.AddFunction(Function{NameStrindex: d.InternString([]byte("shared"))})
	loc1 := d.AddLocation(Location{Lines: []Line{{FunctionIndex: shared}}})
	loc2 := d.AddLocation(Location{Lines: []Line{{FunctionIndex: shared}}, Address: 0x40})
	st1 := d.AddStack(loc1)
	st2 := d.AddStack(loc2)

	b := newBuilder(d)
	b.stackID(st1)
	b.stackID(st2)

	assert.Len(t, b.tables.t[2], 1, "shared function recorded once")
	assert.Len(t, b.tables.t[3], 2, "two distinct locations")
	assert.Len(t, b.tables.t[4], 2, "two distinct stacks")
}

// TestSymbolStoreAbsorbEncodeUnion exercises the SideStore lifecycle: absorb two batch deltas,
// encode sidecars, then union two parts' sidecars and confirm the merged tables hold every entry.
func TestSymbolStoreAbsorbEncodeUnion(t *testing.T) {
	t.Parallel()

	d := &Dictionary{}
	stA := buildStack(d, "a", "a.go")
	stB := buildStack(d, "b", "b.go")

	ba := newBuilder(d)
	ba.stackID(stA)
	bb := newBuilder(d)
	bb.stackID(stB)

	// Two engines' worth of accumulators → two parts' sidecars.
	s1 := NewSymbolStore()
	require.NoError(t, s1.Absorb(encodeDelta(ba.tables)))
	part1 := s1.Encode()

	s2 := NewSymbolStore()
	require.NoError(t, s2.Absorb(encodeDelta(bb.tables)))
	part2 := s2.Encode()

	merged, err := NewSymbolStore().Union([]map[string][]byte{part1, part2})
	require.NoError(t, err)

	stacks := map[signal.SeriesID][]byte{}
	require.NoError(t, decodeTable(stacks, merged["stacks"]))
	assert.Len(t, stacks, 2, "union holds both stacks")

	strings := map[signal.SeriesID][]byte{}
	require.NoError(t, decodeTable(strings, merged["strings"]))
	// a/a.go/b/b.go plus the "" sentinel (referenced via each function's unset system-name) ⇒ 5.
	assert.Len(t, strings, 5)
}

// FuzzAbsorbDelta: arbitrary deltas (the cluster/replication path decodes these) must never panic.
func FuzzAbsorbDelta(f *testing.F) {
	d := &Dictionary{}
	b := newBuilder(d)
	b.stackID(buildStack(d, "f", "f.go"))
	f.Add(encodeDelta(b.tables))
	f.Add([]byte{0x00})

	f.Fuzz(func(_ *testing.T, data []byte) {
		_ = NewSymbolStore().Absorb(data)
	})
}
