package wal

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/index/postings"
	"github.com/oteldb/storage/index/series"
	"github.com/oteldb/storage/index/symbols"
	"github.com/oteldb/storage/signal"
)

// head is the in-memory index reconstructed from the WAL: the symbol table, the series
// index, and the inverted index. Adding a series mirrors what the engine does on ingest
// and on replay.
type head struct {
	sym    *symbols.Table
	series *series.Index
	post   *postings.MemPostings
}

func newHead() *head {
	return &head{sym: symbols.New(), series: series.New(), post: postings.NewMemPostings()}
}

func (h *head) add(id signal.SeriesID, a signal.Attributes) error {
	h.series.Add(a) // recomputes the same content-addressed id
	for _, kv := range a {
		nameID := uint32(h.sym.Intern(kv.Key))
		valueID := uint32(h.sym.Intern(signal.AppendValue(nil, kv.Value)))
		h.post.Add(id, nameID, valueID)
	}

	return nil
}

// query resolves name=value to series ids on the reconstructed head.
func (h *head) query(t *testing.T, name, value string) []signal.SeriesID {
	t.Helper()
	nameID, ok := h.sym.Lookup([]byte(name))
	require.True(t, ok)
	valueID, ok := h.sym.Lookup(signal.AppendValue(nil, signal.StringValue([]byte(value))))
	require.True(t, ok)

	got, err := postings.ToSlice(h.post.Get(uint32(nameID), uint32(valueID)))
	require.NoError(t, err)

	return got
}

// TestSegmentReplayReconstructsHead is the M2 exit: write series across several
// segments, then replay the directory to rebuild the index and query it.
func TestSegmentReplayReconstructsHead(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()

	input := []signal.Attributes{
		attrs("job", "api", "env", "prod"),
		attrs("job", "api", "env", "dev"),
		attrs("job", "web", "env", "prod"),
		attrs("job", "web", "env", "dev"),
		attrs("job", "db", "env", "prod"),
	}

	// Tiny segments force rotation, so replay must stitch multiple files together.
	sw, err := Create(dir, 48)
	require.NoError(t, err)
	for _, a := range input {
		require.NoError(t, sw.WriteSeries(a.Hash(), a))
	}
	require.NoError(t, sw.Close())

	// More than one segment was produced.
	entries, err := os.ReadDir(dir)
	require.NoError(t, err)
	segs := 0
	for _, e := range entries {
		if strings.HasSuffix(e.Name(), ".wal") {
			segs++
		}
	}
	require.GreaterOrEqual(t, segs, 2, "small maxBytes should rotate segments")

	// Replay reconstructs the head.
	h := newHead()
	require.NoError(t, ReplayDir(dir, Handlers{OnSeries: h.add}))

	assert.Equal(t, len(input), h.series.Len(), "every series recovered")
	for _, a := range input {
		_, ok := h.series.Get(a.Hash())
		assert.Truef(t, ok, "series %v missing after replay", a)
	}

	// matcher → SeriesIDs on the reconstructed index.
	apiSeries := h.query(t, "job", "api")
	assert.Len(t, apiSeries, 2)
	for _, want := range []signal.Attributes{input[0], input[1]} {
		assert.Contains(t, apiSeries, want.Hash())
	}

	prod := h.query(t, "env", "prod")
	assert.Len(t, prod, 3)
}

func TestCreateDefaultMaxBytes(t *testing.T) {
	t.Parallel()

	sw, err := Create(t.TempDir(), 0) // 0 ⇒ default
	require.NoError(t, err)
	assert.Equal(t, DefaultMaxSegmentBytes, sw.maxBytes)
	require.NoError(t, sw.Close())
}

func TestReplayDirMissing(t *testing.T) {
	t.Parallel()

	err := ReplayDir(t.TempDir()+"/nope", Handlers{})
	require.Error(t, err)
}

func TestSyncAndDoubleClose(t *testing.T) {
	t.Parallel()

	sw, err := Create(t.TempDir(), 0)
	require.NoError(t, err)
	a := attrs("a", "b")
	require.NoError(t, sw.WriteSeries(a.Hash(), a))
	require.NoError(t, sw.Sync())
	require.NoError(t, sw.Close())
	require.NoError(t, sw.Close(), "double close is a no-op")
}

func TestReplayDirCorruptSegment(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	frame := appendFrame(nil, recordSeries, append(make([]byte, seriesIDLen), 0x00))
	frame[3] ^= 0xFF // corrupt the body so the CRC fails
	require.NoError(t, os.WriteFile(filepath.Join(dir, "00000001.wal"), frame, 0o600))

	err := ReplayDir(dir, Handlers{OnSeries: func(signal.SeriesID, signal.Attributes) error { return nil }})
	require.ErrorIs(t, err, ErrCorrupt)
}
