package profile

import (
	"runtime"
	"slices"
	"testing"

	"github.com/stretchr/testify/require"
)

func allocated(fn func()) uint64 {
	var before, after runtime.MemStats

	runtime.GC()
	runtime.ReadMemStats(&before)
	fn()
	runtime.ReadMemStats(&after)

	return after.TotalAlloc - before.TotalAlloc
}

// TestUnionDecodesOnce pins that a merge's sidecar union decodes its inputs once and encodes only
// what the refs reach: against unioning the whole set, encoding it and decoding it again to retain,
// a union of a real profile's tables that keeps one stack allocates far less.
//
//nolint:paralleltest // measures the process's allocations; a concurrent test would skew them
func TestUnionDecodesOnce(t *testing.T) {
	corpus := pprofCorpus(t)
	part := NewSymbolStore()
	part.acc = corpus
	parts := []map[string][]byte{part.Encode(), part.Encode()}

	var stack []byte
	for id := range corpus.t[tableStacks] {
		stack = id.AppendBinary(nil)

		break
	}

	refs := slices.Values([][]byte{stack})

	retainAfterUnion := func() {
		merged := newSymTables()
		for _, p := range parts {
			for i, name := range tableNames {
				require.NoError(t, decodeTable(merged.t[i], p[name]))
			}
		}

		full := (&SymbolStore{acc: merged}).Encode()

		src := NewSymbolStore()
		require.NoError(t, src.Restore(full))
		src.Retain(refs)
		_ = src.Encode()
	}

	var got map[string][]byte

	single := allocated(func() {
		var err error
		got, err = NewSymbolStore().Union(parts, refs)
		require.NoError(t, err)
	})
	double := allocated(retainAfterUnion)

	t.Logf("union+retain in one pass: %d B; union, encode, decode again, retain: %d B", single, double)
	require.Less(t, 3*single, 2*double, "the union is decoded once, and its whole set never encoded")

	stacks := map[string]struct{}{}
	src := NewSymbolStore()
	require.NoError(t, src.Restore(got))

	for id := range src.acc.t[tableStacks] {
		stacks[string(id.AppendBinary(nil))] = struct{}{}
	}

	require.Equal(t, map[string]struct{}{string(stack): {}}, stacks)
}
