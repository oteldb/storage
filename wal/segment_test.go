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
// and on replay — it indexes the resource, scope and point attributes as queryable labels.
type head struct {
	sym    *symbols.Table
	series *series.Index
	post   *postings.MemPostings
}

func newHead() *head {
	return &head{sym: symbols.New(), series: series.New(), post: postings.NewMemPostings()}
}

func (h *head) add(id signal.SeriesID, s signal.Series) error {
	h.series.Add(s) // recomputes the same content-addressed id

	h.indexAttrs(id, s.Resource.Attributes)
	if len(s.Scope.Name) > 0 {
		h.addLabel(id, []byte("otel.scope.name"), signal.StringValue(s.Scope.Name))
	}
	h.indexAttrs(id, s.Attributes)

	return nil
}

func (h *head) indexAttrs(id signal.SeriesID, a signal.Attributes) {
	for _, kv := range a {
		h.addLabel(id, kv.Key, kv.Value)
	}
}

func (h *head) addLabel(id signal.SeriesID, name []byte, v signal.Value) {
	nameID := uint32(h.sym.Intern(name))
	valueID := uint32(h.sym.Intern(signal.AppendValue(nil, v)))
	h.post.Add(id, nameID, valueID)
}

// query resolves name=value (a string-valued label) to series ids on the reconstructed
// head.
func (h *head) query(t *testing.T, name, value string) []signal.SeriesID {
	t.Helper()
	nameID, ok := h.sym.Lookup([]byte(name))
	require.True(t, ok, "name %q", name)
	valueID, ok := h.sym.Lookup(signal.AppendValue(nil, signal.StringValue([]byte(value))))
	require.True(t, ok, "value %q", value)

	got, err := postings.ToSlice(h.post.Get(uint32(nameID), uint32(valueID)))
	require.NoError(t, err)

	return got
}

func mkInput(service, job, env string) signal.Series {
	return signal.Series{
		Resource:   signal.Resource{Attributes: attrs("service.name", service)},
		Scope:      signal.Scope{Name: []byte("lib")},
		Attributes: attrs("job", job, "env", env),
	}
}

// TestSegmentReplayReconstructsHead is the M2 exit: write series across several
// segments, then replay the directory to rebuild the index and query it — including by a
// Resource label, proving identity spans Resource + Scope + attributes.
func TestSegmentReplayReconstructsHead(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()

	input := []signal.Series{
		mkInput("api", "ingest", "prod"),
		mkInput("api", "ingest", "dev"),
		mkInput("web", "ingest", "prod"),
		mkInput("web", "render", "dev"),
		mkInput("db", "compact", "prod"),
	}

	// Tiny segments force rotation, so replay must stitch multiple files together.
	sw, err := Create(dir, 64)
	require.NoError(t, err)
	for _, s := range input {
		require.NoError(t, sw.WriteSeries(s.Hash(), s))
	}
	require.NoError(t, sw.Close())

	segs := 0
	entries, err := os.ReadDir(dir)
	require.NoError(t, err)
	for _, e := range entries {
		if strings.HasSuffix(e.Name(), ".wal") {
			segs++
		}
	}
	require.GreaterOrEqual(t, segs, 2, "small maxBytes should rotate segments")

	h := newHead()
	require.NoError(t, ReplayDir(dir, Handlers{OnSeries: h.add}))

	assert.Equal(t, len(input), h.series.Len(), "every series recovered")
	for _, s := range input {
		_, ok := h.series.Get(s.Hash())
		assert.Truef(t, ok, "series %v missing after replay", s)
	}

	// Query by a Resource label, a point label, and the scope name.
	assert.Len(t, h.query(t, "service.name", "api"), 2)
	assert.Len(t, h.query(t, "service.name", "web"), 2)
	assert.Len(t, h.query(t, "job", "ingest"), 3)
	assert.Len(t, h.query(t, "otel.scope.name", "lib"), len(input))
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
	s := mkSeries("a", "b")
	require.NoError(t, sw.WriteSeries(s.Hash(), s))
	require.NoError(t, sw.Sync())
	require.NoError(t, sw.Close())
	require.NoError(t, sw.Close(), "double close is a no-op")
}

func TestSegmentSamplesRoundTrip(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	s := mkSeries("a", "b")
	id := s.Hash()

	sw, err := Create(dir, 0)
	require.NoError(t, err)
	require.NoError(t, sw.WriteSeries(id, s))
	require.NoError(t, sw.WriteSamples(id, []int64{1, 2}, []float64{10, 20}))
	require.NoError(t, sw.WriteSamples(id, []int64{3}, []float64{30}))
	require.NoError(t, sw.Close())

	var gotTs []int64

	var gotVal []float64

	err = ReplayDir(dir, Handlers{
		OnSamples: func(_ signal.SeriesID, ts []int64, v []float64) error {
			gotTs = append(gotTs, ts...)
			gotVal = append(gotVal, v...)

			return nil
		},
	})
	require.NoError(t, err)
	assert.Equal(t, []int64{1, 2, 3}, gotTs)
	assert.Equal(t, []float64{10, 20, 30}, gotVal)
}

func TestReplayDirCorruptSegment(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	frame := appendFrame(nil, recordSeries, append(make([]byte, seriesIDLen), 0x00))
	frame[3] ^= 0xFF // corrupt the body so the CRC fails
	require.NoError(t, os.WriteFile(filepath.Join(dir, segmentName(1, 1)), frame, 0o600))

	err := ReplayDir(dir, Handlers{OnSeries: func(signal.SeriesID, signal.Series) error { return nil }})
	require.ErrorIs(t, err, ErrCorrupt)
}
