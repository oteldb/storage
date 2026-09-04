package bucketindex_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/backend"
	"github.com/oteldb/storage/backend/bucketindex"
)

func TestAddKeepsSortedAndReplaces(t *testing.T) {
	t.Parallel()

	var ix bucketindex.Index
	ix.Add(bucketindex.Entry{Prefix: "c", MinTime: 1, MaxTime: 2})
	ix.Add(bucketindex.Entry{Prefix: "a", MinTime: 3, MaxTime: 4})
	ix.Add(bucketindex.Entry{Prefix: "b", MinTime: 5, MaxTime: 6})

	prefixes := []string{ix.Entries[0].Prefix, ix.Entries[1].Prefix, ix.Entries[2].Prefix}
	assert.Equal(t, []string{"a", "b", "c"}, prefixes, "kept sorted by prefix")

	// Re-adding the same prefix replaces in place (no duplicate).
	ix.Add(bucketindex.Entry{Prefix: "b", MinTime: 50, MaxTime: 60})
	require.Len(t, ix.Entries, 3)
	assert.Equal(t, int64(50), ix.Entries[1].MinTime)
}

func TestRemove(t *testing.T) {
	t.Parallel()

	var ix bucketindex.Index
	ix.Add(bucketindex.Entry{Prefix: "a"})
	ix.Add(bucketindex.Entry{Prefix: "b"})

	assert.True(t, ix.Remove("a"))
	assert.False(t, ix.Remove("a"), "already gone")
	require.Len(t, ix.Entries, 1)
	assert.Equal(t, "b", ix.Entries[0].Prefix)
}

func TestOverlapping(t *testing.T) {
	t.Parallel()

	var ix bucketindex.Index
	ix.Add(bucketindex.Entry{Prefix: "p0", MinTime: 0, MaxTime: 100})
	ix.Add(bucketindex.Entry{Prefix: "p1", MinTime: 200, MaxTime: 300})
	ix.Add(bucketindex.Entry{Prefix: "p2", MinTime: 250, MaxTime: 400})

	got := ix.Overlapping(280, 500)
	require.Len(t, got, 2)
	assert.Equal(t, "p1", got[0].Prefix)
	assert.Equal(t, "p2", got[1].Prefix)

	assert.Empty(t, ix.Overlapping(101, 199), "gap between parts prunes everything")
	assert.Len(t, ix.Overlapping(100, 100), 1, "inclusive boundary touches p0")
}

func TestRoundTrip(t *testing.T) {
	t.Parallel()

	in := &bucketindex.Index{Entries: []bucketindex.Entry{
		{Prefix: "default/metrics/0000000000", MinTime: -5, MaxTime: 1_700_000_000_000_000_000},
		{Prefix: "default/metrics/0000000001", MinTime: 0, MaxTime: 10},
	}}

	out, err := bucketindex.Decode(in.AppendBinary(nil))
	require.NoError(t, err)
	assert.Equal(t, in.Entries, out.Entries)
}

func TestGoldenEncoding(t *testing.T) {
	t.Parallel()

	ix := &bucketindex.Index{
		Entries:      []bucketindex.Entry{{Prefix: "a", MinTime: 1, MaxTime: 2}},
		FlushedEpoch: 3,
		Generation:   bucketindex.Generation{Term: 4, Counter: 5},
	}
	// magic 'B','I', version 5, count 1, len 1, 'a', zigzag(1)=2, zigzag(2)=4, block min 0,
	// block max 0, level 0, flushedEpoch 3, generation term 4, generation counter 5,
	// removal count 0, writer-epoch count 0, want count 0.
	assert.Equal(t, []byte{'B', 'I', 5, 1, 1, 'a', 2, 4, 0, 0, 0, 3, 4, 5, 0, 0, 0}, ix.AppendBinary(nil))
}

// TestDecodeV2Compat verifies a v2 index (epoch, no generation) decodes with a zero generation,
// which orders below every generation a writer produces.
func TestDecodeV2Compat(t *testing.T) {
	t.Parallel()

	got, err := bucketindex.Decode([]byte{'B', 'I', 2, 1, 1, 'a', 2, 4, 3})
	require.NoError(t, err)
	require.Len(t, got.Entries, 1)
	assert.EqualValues(t, 3, got.FlushedEpoch)
	assert.True(t, got.Generation.Zero())
}

// TestGenerationRoundTrip verifies the generation survives encode∘decode.
func TestGenerationRoundTrip(t *testing.T) {
	t.Parallel()

	in := &bucketindex.Index{
		Entries:    []bucketindex.Entry{{Prefix: "p", MinTime: 1, MaxTime: 2}},
		Generation: bucketindex.Generation{Term: 1 << 40, Counter: 1 << 33},
	}

	out, err := bucketindex.Decode(in.AppendBinary(nil))
	require.NoError(t, err)
	assert.Equal(t, in.Generation, out.Generation)
}

// TestDecodeV1Compat verifies a v1 index (no flush epoch) decodes with FlushedEpoch 0.
func TestDecodeV1Compat(t *testing.T) {
	t.Parallel()

	got, err := bucketindex.Decode([]byte{'B', 'I', 1, 1, 1, 'a', 2, 4})
	require.NoError(t, err)
	require.Len(t, got.Entries, 1)
	assert.Equal(t, "a", got.Entries[0].Prefix)
	assert.Zero(t, got.FlushedEpoch)
}

// TestFlushedEpochRoundTrip verifies the watermark survives encode∘decode.
func TestFlushedEpochRoundTrip(t *testing.T) {
	t.Parallel()

	ix := &bucketindex.Index{Entries: []bucketindex.Entry{{Prefix: "p", MinTime: 5, MaxTime: 9}}, FlushedEpoch: 42}
	got, err := bucketindex.Decode(ix.AppendBinary(nil))
	require.NoError(t, err)
	assert.Equal(t, uint64(42), got.FlushedEpoch)
	assert.Equal(t, ix.Entries, got.Entries)
}

func TestDecodeRejectsCorrupt(t *testing.T) {
	t.Parallel()

	cases := map[string][]byte{
		"empty":          {},
		"bad magic":      {'X', 'Y', 1, 0},
		"bad version":    {'B', 'I', 9, 0},
		"truncated body": {'B', 'I', 1, 5}, // claims 5 entries, none follow
		"bad prefix len": {'B', 'I', 1, 1, 200},
		"missing max":    {'B', 'I', 1, 1, 1, 'a', 2}, // prefix+min present, max truncated
	}
	for name, data := range cases {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			_, err := bucketindex.Decode(data)
			require.Error(t, err)
		})
	}
}

func TestLoadSave(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	b := backend.Memory()
	key := "default/metrics/" + bucketindex.Object

	// Missing index loads as empty.
	empty, err := bucketindex.Load(ctx, b, key)
	require.NoError(t, err)
	assert.Empty(t, empty.Entries)

	ix := &bucketindex.Index{Entries: []bucketindex.Entry{{Prefix: "default/metrics/0", MinTime: 1, MaxTime: 9}}}
	_, err = ix.Save(ctx, b, key, backend.VersionAbsent)
	require.NoError(t, err)

	got, err := bucketindex.Load(ctx, b, key)
	require.NoError(t, err)
	assert.Equal(t, ix.Entries, got.Entries)
}

func TestLoadCorruptErrors(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	b := backend.Memory()
	key := "k/" + bucketindex.Object
	require.NoError(t, b.Write(ctx, key, []byte("not an index")))

	_, err := bucketindex.Load(ctx, b, key)
	require.ErrorIs(t, err, bucketindex.ErrCorrupt)
}

// failWrite is a backend whose conditional write always fails, to exercise Save's error path.
type failWrite struct{ backend.Backend }

func (failWrite) CompareAndSwap(
	context.Context, string, backend.Version, []byte,
) (backend.Version, bool, error) {
	return backend.VersionAbsent, false, assert.AnError
}

func TestSaveError(t *testing.T) {
	t.Parallel()

	ix := &bucketindex.Index{Entries: []bucketindex.Entry{{Prefix: "p"}}}
	_, err := ix.Save(context.Background(), failWrite{backend.Memory()}, "k", backend.VersionAbsent)
	require.Error(t, err)
}

func FuzzDecode(f *testing.F) {
	f.Add([]byte{'B', 'I', 1, 0})
	f.Add([]byte{'B', 'I', 1, 1, 1, 'a', 2, 4})
	f.Add((&bucketindex.Index{Entries: []bucketindex.Entry{{Prefix: "x", MinTime: -1, MaxTime: 1}}}).AppendBinary(nil))
	f.Add((&bucketindex.Index{
		Entries: []bucketindex.Entry{{Prefix: "x", MinTime: -1, MaxTime: 1}},
		Epochs: []bucketindex.WriterEpoch{
			{Writer: "node-a", Epoch: 3, Generation: bucketindex.Generation{Term: 1, Counter: 2}},
		},
	}).AppendBinary(nil))

	f.Fuzz(func(t *testing.T, data []byte) {
		ix, err := bucketindex.Decode(data)
		if err != nil {
			return // malformed input must error, not panic
		}

		// A successful decode must re-encode to bytes that decode identically (canonical form).
		again, err := bucketindex.Decode(ix.AppendBinary(nil))
		require.NoError(t, err)
		assert.Equal(t, ix.Entries, again.Entries)
		assert.Equal(t, ix.Epochs, again.Epochs)
		assert.Equal(t, ix.Wanted, again.Wanted)
	})
}
