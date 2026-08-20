package bucketindex_test

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/backend/bucketindex"
)

func TestWriterEpochSlots(t *testing.T) {
	t.Parallel()

	var ix bucketindex.Index

	assert.Zero(t, ix.WriterEpoch("a"), "a writer with no slot has no watermark")
	assert.Zero(t, ix.WriterEpoch(""), "and neither has the anonymous one")

	ix.SetWriterEpoch("b", 7, bucketindex.Generation{Term: 1, Counter: 2})
	ix.SetWriterEpoch("a", 3, bucketindex.Generation{Term: 1, Counter: 1})

	assert.EqualValues(t, 3, ix.WriterEpoch("a"))
	assert.EqualValues(t, 7, ix.WriterEpoch("b"))
	assert.Equal(t, []string{"a", "b"}, []string{ix.Epochs[0].Writer, ix.Epochs[1].Writer},
		"kept sorted by writer")

	ix.SetWriterEpoch("a", 4, bucketindex.Generation{Term: 1, Counter: 3})
	require.Len(t, ix.Epochs, 2, "a writer's own slot is replaced, not appended")
	assert.EqualValues(t, 4, ix.WriterEpoch("a"))
}

// TestWriterEpochAnonymousSlot pins the anonymous writer to the pre-v4 scalar, so a single-writer
// engine keeps reading and writing the field older formats carry.
func TestWriterEpochAnonymousSlot(t *testing.T) {
	t.Parallel()

	var ix bucketindex.Index
	ix.SetWriterEpoch("", 9, bucketindex.Generation{Term: 1, Counter: 1})

	assert.EqualValues(t, 9, ix.FlushedEpoch)
	assert.Empty(t, ix.Epochs, "the anonymous writer takes no slot")
	assert.EqualValues(t, 9, ix.WriterEpoch(""))
	assert.Zero(t, ix.WriterEpoch("a"),
		"a named writer must not read the anonymous scalar: it counts another node's flushes")
}

func TestTrimWriters(t *testing.T) {
	t.Parallel()

	var ix bucketindex.Index
	for i := range 5 {
		ix.SetWriterEpoch(fmt.Sprintf("n%d", i), uint64(i), bucketindex.Generation{Term: 1, Counter: uint64(i)})
	}

	// n0 is the oldest by generation, but it is the trimming writer, so it survives.
	got := bucketindex.TrimWriters(ix.Epochs, "n0", 3)
	require.Len(t, got, 3)

	names := make([]string, len(got))
	for i, w := range got {
		names[i] = w.Writer
	}

	assert.Equal(t, []string{"n0", "n3", "n4"}, names)

	assert.Len(t, bucketindex.TrimWriters(ix.Epochs, "n0", 9), 5, "under the cap nothing is dropped")
}

func TestWriterEpochRoundTrip(t *testing.T) {
	t.Parallel()

	in := &bucketindex.Index{
		Entries:      []bucketindex.Entry{{Prefix: "p", MinTime: 1, MaxTime: 2}},
		FlushedEpoch: 11,
		Generation:   bucketindex.Generation{Term: 2, Counter: 3},
	}
	in.SetWriterEpoch("node-a", 1<<40, bucketindex.Generation{Term: 2, Counter: 3})
	in.SetWriterEpoch("node-b", 5, bucketindex.Generation{Term: 1, Counter: 9})

	out, err := bucketindex.Decode(in.AppendBinary(nil))
	require.NoError(t, err)
	assert.Equal(t, in.Epochs, out.Epochs)
	assert.EqualValues(t, 11, out.FlushedEpoch)
}

// TestDecodeV3Compat verifies a v3 index (no per-writer slots) decodes with none, so every named
// writer over such a prefix starts from a zero watermark rather than from a scalar that is not its
// own.
func TestDecodeV3Compat(t *testing.T) {
	t.Parallel()

	got, err := bucketindex.Decode([]byte{'B', 'I', 3, 1, 1, 'a', 2, 4, 3, 4, 5, 0})
	require.NoError(t, err)
	require.Len(t, got.Entries, 1)
	assert.EqualValues(t, 3, got.FlushedEpoch)
	assert.Empty(t, got.Epochs)
	assert.EqualValues(t, 3, got.WriterEpoch(""))
	assert.Zero(t, got.WriterEpoch("node-a"))
}

func TestDecodeRejectsCorruptWriterEpochs(t *testing.T) {
	t.Parallel()

	// A v4 index of one entry, epoch 3, generation 4/5, no removals, then a truncated slot list.
	head := []byte{'B', 'I', 4, 1, 1, 'a', 2, 4, 3, 4, 5, 0}
	cases := map[string][]byte{
		"missing count":  head,
		"count exceeds":  append(append([]byte{}, head...), 200),
		"bad writer len": append(append([]byte{}, head...), 1, 200),
		"missing epoch":  append(append([]byte{}, head...), 1, 1, 'x'),
		"missing term":   append(append([]byte{}, head...), 1, 1, 'x', 1),
	}

	for name, data := range cases {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			_, err := bucketindex.Decode(data)
			require.ErrorIs(t, err, bucketindex.ErrCorrupt)
		})
	}
}
