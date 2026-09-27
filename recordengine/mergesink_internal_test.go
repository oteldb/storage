package recordengine

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/signal"
)

// streamedBloom feeds values to a [bloomAccum] a row at a time, the way a merge does.
func streamedBloom(mode BloomMode, values *byteCol) []byte {
	var s columnSink

	s.bloom = &bloomAccum{mode: mode}
	s.seen = make(map[uint64]int32)

	for i := range values.rows() {
		s.add(values.at(i))
	}

	return s.bloom.encode()
}

// TestBloomAccumMatchesBuild: a bloom built from values streamed to it is bit-identical to the one the
// flush builds over the same column, in every mode, so a part's filter does not depend on which path
// wrote it.
func TestBloomAccumMatchesBuild(t *testing.T) {
	t.Parallel()

	for _, tt := range bloomCorpus(t) {
		if tt.mode == BloomNone {
			continue
		}

		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			assert.Equal(t, buildColumnBloom(tt.mode, tt.values), streamedBloom(tt.mode, tt.values))
		})
	}
}

// FuzzBloomAccumMatchesBuild is [TestBloomAccumMatchesBuild] over generated columns.
func FuzzBloomAccumMatchesBuild(f *testing.F) {
	f.Add([]byte("hello world"), []byte("Second ROW here"), byte(3))
	f.Add([]byte(""), []byte(""), byte(1))
	f.Add([]byte{0x01, 0x01, 0x61, 0x00}, []byte{0xff}, byte(200))

	f.Fuzz(func(t *testing.T, a, b []byte, repeat byte) {
		var values byteCol

		for i := range int(repeat) + 1 {
			values.appendCell(a)

			if i%3 == 0 {
				values.appendCell(b)
			}
		}

		for _, mode := range []BloomMode{BloomFullText, BloomEquality, BloomAttrs} {
			if want, got := buildColumnBloom(mode, &values), streamedBloom(mode, &values); !bytes.Equal(want, got) {
				t.Fatalf("mode %v: streamed filter diverged from the flush build", mode)
			}
		}
	})
}

// TestColumnSinkKeys: the attributes column's sink collects each distinct key once, however many
// rows repeat its blob, and a malformed blob contributes none.
func TestColumnSinkKeys(t *testing.T) {
	t.Parallel()

	s := newColumnSink(streamedSchema, 1)
	require.NotNil(t, s)
	require.NotNil(t, s.keys)

	kv := func(k, v string) signal.KeyValue {
		return signal.KeyValue{Key: []byte(k), Value: signal.StringValue([]byte(v))}
	}

	blob := attrsCell(t, kv("b", "1"), kv("a", "2"))
	for range 3 {
		s.add(blob)
	}

	s.add(attrsCell(t, kv("c", "3")))
	s.add([]byte{0xff, 0xff})

	assert.Equal(t, [][]byte{[]byte("a"), []byte("b"), []byte("c")}, s.keys.sorted())
	assert.Nil(t, newColumnSink(streamedSchema, 5), "a column with no bloom that is not the attributes column needs none")
	assert.Nil(t, recordKeySet{}.sorted())
}

func TestHashPairs(t *testing.T) {
	t.Parallel()

	var s hashPairs

	assert.True(t, s.add(0, 0))
	assert.False(t, s.add(0, 0))

	const n = 5000

	for i := range uint64(n) {
		require.True(t, s.add(i*7919+1, i))
	}

	for i := range uint64(n) {
		require.False(t, s.add(i*7919+1, i), "pair %d added twice", i)
	}

	seen := map[hashPair]bool{}
	s.each(func(h1, h2 uint64) { seen[hashPair{h1, h2}] = true })

	assert.Len(t, seen, n+1)
	assert.True(t, seen[hashPair{}])
}

func TestUpTo(t *testing.T) {
	t.Parallel()

	ts := []int64{1, 3, 3, 3, 7}

	for _, tt := range []struct {
		bound     int64
		inclusive bool
		want      int
	}{
		{0, true, 0},
		{1, false, 0},
		{1, true, 1},
		{3, false, 1},
		{3, true, 4},
		{7, false, 4},
		{9, false, 5},
	} {
		assert.Equal(t, tt.want, upTo(ts, tt.bound, tt.inclusive), "bound %d inclusive %v", tt.bound, tt.inclusive)
	}
}
