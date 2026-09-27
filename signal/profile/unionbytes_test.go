//go:build !race

package profile

import (
	"crypto/rand"
	"encoding/binary"
	"runtime"
	"strconv"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/internal/heaptest"
	"github.com/oteldb/storage/signal"
)

// TestUnionBytesCoversUnion: merging large incompressible stored tables — Union, then Stored, as a
// merge does for every part it writes — holds no more, sampled at every point the union's peak can
// fall and with the stored union live, than UnionBytes reserves for it beside the loaded sidecars, for
// entries from the smallest a table can hold to large ones, when the refs keep every entry.
//
//nolint:paralleltest // samples the process-wide heap
func TestUnionBytesCoversUnion(t *testing.T) {
	for _, size := range []int{1, 64, 1024} {
		t.Run(strconv.Itoa(size), func(t *testing.T) { unionBytesCoversUnion(t, size) })
	}
}

// reachableTables returns tables of n chains, a stack reaching a location, its mapping and function,
// and their strings, each entry padded with random bytes to at least size; and the chains' stack ids.
func reachableTables(n, size int) (symTables, [][]byte) {
	tables := newSymTables()
	stacks := make([][]byte, 0, n)

	newID := func() signal.SeriesID {
		var b [16]byte

		_, _ = rand.Read(b[:])

		return signal.SeriesID{Hi: binary.BigEndian.Uint64(b[:]), Lo: binary.BigEndian.Uint64(b[8:])}
	}
	put := func(table int, entry []byte) signal.SeriesID {
		if pad := size - len(entry); pad > 0 {
			tail := make([]byte, pad)
			_, _ = rand.Read(tail)
			entry = append(entry, tail...)
		}

		id := newID()
		tables.t[table][id] = entry

		return id
	}
	appendID := func(dst []byte, id signal.SeriesID) []byte {
		return binary.BigEndian.AppendUint64(binary.BigEndian.AppendUint64(dst, id.Hi), id.Lo)
	}

	for range n {
		str := put(tableStrings, nil)
		fn := put(tableFunctions, appendID(appendID(appendID(nil, str), str), str))
		mapping := put(tableMappings, appendID([]byte{0, 0, 0}, str))
		loc := put(tableLocations, append(appendID(append(appendID(nil, mapping), 0, 1), fn), 0, 0))
		stack := put(tableStacks, appendID([]byte{1}, loc))

		stacks = append(stacks, appendID(nil, stack))
	}

	return tables, stacks
}

func unionBytesCoversUnion(t *testing.T, size int) {
	t.Helper()

	const (
		sources = 4
		chains  = 8 << 10
	)

	s := NewSymbolStore()

	parts := make([]map[string][]byte, 0, sources)
	refs := make([][]byte, 0, sources*chains)
	sizes := make([]int64, 0, sources*len(tableNames))
	heads := make([][]byte, 0, sources*len(tableNames))

	for range sources {
		tables, stacks := reachableTables(chains, size)
		refs = append(refs, stacks...)

		stored, err := s.Stored(encodeTables(tables, memoryCompressor))
		require.NoError(t, err)

		for _, name := range tableNames {
			data := stored[name]
			sizes = append(sizes, int64(len(data)))
			heads = append(heads, data[:min(len(data), 32)])
		}

		parts = append(parts, stored)
	}

	bound, err := s.UnionBytes(sizes, heads)
	require.NoError(t, err)

	var total int64
	for i, h := range heads {
		n, err := tableBodyLen(h, sizes[i])
		require.NoError(t, err)

		total += n
	}

	each := func(yield func([]byte) bool) {
		for _, ref := range refs {
			if !yield(ref) {
				return
			}
		}
	}

	var merged map[string][]byte

	peak := heaptest.Peak(func(sample func()) {
		unionSample = sample

		defer func() { unionSample = nil }()

		merged, err = s.Union(parts, each)
		require.NoError(t, err)

		sample()

		stored, err := s.Stored(merged)
		require.NoError(t, err)

		sample()
		runtime.KeepAlive(stored)
	})

	kept := NewSymbolStore()
	require.NoError(t, kept.Restore(merged))

	for i, name := range tableNames {
		require.Len(t, kept.acc.t[i], sources*chains, "the refs must keep every %s entry", name)
	}

	t.Logf("%d sources × %d chains, entries of at least %d B: bodies %.1f MiB, union peak %.1f MiB (%.1f×), bound %.1f MiB",
		sources, chains, size, float64(total)/(1<<20), float64(peak)/(1<<20), float64(peak)/float64(total), float64(bound)/(1<<20))
	assert.LessOrEqual(t, int64(peak), bound, "the union outgrew what UnionBytes reserves")
}
