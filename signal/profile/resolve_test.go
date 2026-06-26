package profile

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// frameStack adds a leaf-first stack of (function, file, line) frames and returns its index.
func twoFrameStack(d *Dictionary) int32 {
	main := d.AddLocation(Location{Lines: []Line{{
		FunctionIndex: d.AddFunction(Function{NameStrindex: d.InternString([]byte("main")), FilenameStrindex: d.InternString([]byte("main.go"))}),
		Line:          10,
	}}})
	work := d.AddLocation(Location{Lines: []Line{{
		FunctionIndex: d.AddFunction(Function{NameStrindex: d.InternString([]byte("work")), FilenameStrindex: d.InternString([]byte("work.go"))}),
		Line:          20,
	}}})

	return d.AddStack(work, main) // leaf first
}

// TestResolverResolvesStack round-trips a stack through the symbol store and resolves its id to the
// leaf-first frames (function name, file, line).
func TestResolverResolvesStack(t *testing.T) {
	t.Parallel()

	d := &Dictionary{}
	st := twoFrameStack(d)

	b := newBuilder(d)
	stackID := b.stackID(st).AppendBinary(nil)

	store := NewSymbolStore()
	require.NoError(t, store.Absorb(encodeDelta(b.tables)))

	r, err := NewResolver(store.Encode())
	require.NoError(t, err)

	frames := r.Resolve(stackID)
	require.Len(t, frames, 2)
	assert.Equal(t, Frame{Function: "work", File: "work.go", Line: 20}, frames[0])
	assert.Equal(t, Frame{Function: "main", File: "main.go", Line: 10}, frames[1])
}

// TestResolverUnknownStack: a wrong-length id or an unknown stack resolves to nil, never panicking.
func TestResolverUnknownStack(t *testing.T) {
	t.Parallel()

	r, err := NewResolver(map[string][]byte{})
	require.NoError(t, err)

	assert.Nil(t, r.Resolve([]byte("short")))
	assert.Nil(t, r.Resolve(make([]byte, 16))) // valid length, absent
}

// TestResolverIgnoresCorruptTables: NewResolver surfaces a corrupt table as an error (no panic).
func TestResolverCorruptTable(t *testing.T) {
	t.Parallel()

	_, err := NewResolver(map[string][]byte{"stacks": []byte("garbage")})
	require.Error(t, err)
}
