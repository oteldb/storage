package engine

import (
	"bytes"
	"encoding/binary"
	"hash/crc32"
	"math"
	"slices"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/go-faster/errors"

	"github.com/oteldb/storage/backend"
	"github.com/oteldb/storage/backend/faultbackend"
	"github.com/oteldb/storage/index/series"
	"github.com/oteldb/storage/internal/mergestream"
	"github.com/oteldb/storage/internal/seriesindex"
	"github.com/oteldb/storage/signal"
)

func mergeIdentityFixture(tb testing.TB, identities []signal.Series, covered int64, legacy bool) *diskMergeSource {
	tb.Helper()
	if legacy {
		out := &diskMergeSource{legacy: series.New()}
		for i := range identities {
			out.legacy.Add(identities[i])
			out.metadata = append(out.metadata, seriesindex.Metadata{Covered: covered})
		}
		return out
	}
	var builder seriesindex.Builder
	for i := range identities {
		require.NoError(tb, builder.Add(identities[i].Hash(), identities[i], seriesindex.Metadata{Covered: covered}))
	}
	b := backend.Memory()
	require.NoError(tb, builder.WriteObject(tb.Context(), b, "index"))
	idx, err := seriesindex.Open(tb.Context(), b, "index")
	require.NoError(tb, err)
	return &diskMergeSource{cursor: idx.SeriesCursor(tb.Context())}
}

func rawMergeIdentityFixture(tb testing.TB, id signal.SeriesID, raw []byte) *diskMergeSource {
	tb.Helper()
	var out bytes.Buffer
	w := seriesindex.NewTableWriter(&out)
	require.NoError(tb, w.Append([]byte{'c'}, binary.BigEndian.AppendUint32(nil, 1)))
	meta := binary.BigEndian.AppendUint64(nil, 100)
	meta = append(meta, 0)
	key := id.AppendBinary([]byte{'i'})
	require.NoError(tb, w.Append(key, append(binary.BigEndian.AppendUint32(nil, 0), meta...)))
	value := id.AppendBinary(nil)
	value = append(value, meta...)
	value = append(value, raw...)
	require.NoError(tb, w.Append([]byte{'s', 0, 0, 0, 0}, value))
	require.NoError(tb, w.Finish())
	data := out.Bytes()
	footer := data[len(data)-72:]
	binary.BigEndian.PutUint32(footer[28:], 1)
	binary.BigEndian.PutUint32(footer[68:], crc32.Checksum(footer[:68], crc32.MakeTable(crc32.Castagnoli)))
	b := backend.Memory()
	require.NoError(tb, b.Write(tb.Context(), "index", data))
	idx, err := seriesindex.Open(tb.Context(), b, "index")
	require.NoError(tb, err)
	return &diskMergeSource{cursor: idx.SeriesCursor(tb.Context())}
}

func mergedIdentitySeries() signal.Series {
	return signal.Series{
		Resource: signal.Resource{SchemaURL: []byte("resource"), Attributes: signal.NewAttributes(
			signal.KeyValue{Key: []byte("service.name"), Value: signal.StringValue([]byte("api"))},
		)},
		Scope: signal.Scope{Name: []byte("scope"), Version: []byte("v1"), SchemaURL: []byte("scope-schema")},
		Attributes: signal.NewAttributes(signal.KeyValue{Key: []byte("nested"), Value: signal.MapValue(
			signal.KeyValue{Key: []byte("zero"), Value: signal.DoubleValue(math.Float64frombits(1 << 63))},
			signal.KeyValue{Key: []byte("nan"), Value: signal.DoubleValue(math.Float64frombits(0x7ff8000000000042))},
			signal.KeyValue{Key: []byte("values"), Value: signal.SliceValue(signal.IntValue(5), signal.StringValue([]byte("5")), signal.DoubleValue(5))},
		)})}
}

func TestMergeDiskIdentityMixedSources(t *testing.T) {
	t.Parallel()
	s := mergedIdentitySeries()
	for _, modes := range [][]bool{{false}, {true}, {false, false, false}, {true, true, true}, {false, true}, {true, false}} {
		var sources []*diskMergeSource
		var positions []mergestream.Position
		for i, legacy := range modes {
			sources = append(sources, mergeIdentityFixture(t, []signal.Series{s}, int64(100-i*10), legacy))
			positions = append(positions, mergestream.Position{Source: i, Index: 0})
		}
		got, covered, err := mergeDiskIdentity(s.Hash(), positions, sources)
		require.NoError(t, err)
		require.EqualValues(t, 100, covered)
		require.Equal(t, s.AppendHashInput(nil), got.AppendHashInput(nil))
		var builder seriesindex.Builder
		require.NoError(t, builder.Add(s.Hash(), got, seriesindex.Metadata{Covered: covered}))
		require.Equal(t, s.AppendHashInput(nil), builder.View().AppendCanonical(nil, 0))
	}
}

func TestMergeDiskIdentityValidatesEveryContributor(t *testing.T) {
	t.Parallel()
	s := mergedIdentitySeries()
	bad := bytes.Clone(s.AppendHashInput(nil))
	bad[len(bad)-1] ^= 1
	for _, losing := range []*diskMergeSource{
		rawMergeIdentityFixture(t, s.Hash(), bad),
		mergeIdentityFixture(t, []signal.Series{benchSeries(999)}, 1, true),
	} {
		good := mergeIdentityFixture(t, []signal.Series{s}, 100, false)
		_, _, err := mergeDiskIdentity(s.Hash(), []mergestream.Position{{Source: 0, Index: 0}, {Source: 1, Index: 0}}, []*diskMergeSource{losing, good})
		require.ErrorIs(t, err, seriesindex.ErrCorrupt)
	}
}

func TestMergeDiskIdentityMalformedCanonical(t *testing.T) {
	t.Parallel()
	raw := []byte{255}
	id := signal.HashBytes(raw)
	first, second := rawMergeIdentityFixture(t, id, raw), rawMergeIdentityFixture(t, id, raw)
	_, _, err := mergeDiskIdentity(id, []mergestream.Position{{Source: 0, Index: 0}, {Source: 1, Index: 0}}, []*diskMergeSource{first, second})
	require.ErrorIs(t, err, seriesindex.ErrCorrupt)
	_, _, err = mergeDiskIdentity(id, nil, nil)
	require.ErrorIs(t, err, seriesindex.ErrCorrupt)
}

func TestMergeDiskIdentityCopiedBeforeCursorAdvance(t *testing.T) {
	t.Parallel()
	input := []signal.Series{mergedIdentitySeries(), benchSeries(500)}
	slices.SortFunc(input, func(a, b signal.Series) int { return a.Hash().Compare(b.Hash()) })
	sources := []*diskMergeSource{mergeIdentityFixture(t, input, 200, false), mergeIdentityFixture(t, input, 100, false)}
	var builder seriesindex.Builder
	for i := range input {
		id := input[i].Hash()
		got, covered, err := mergeDiskIdentity(id, []mergestream.Position{{Source: 0, Index: i}, {Source: 1, Index: i}}, sources)
		require.NoError(t, err)
		require.NoError(t, builder.Add(id, got, seriesindex.Metadata{Covered: covered}))
	}
	for i := range input {
		require.Equal(t, input[i].AppendHashInput(nil), builder.View().AppendCanonical(nil, i))
	}
}

func TestMergeDiskIdentityReadFailureInLosingSource(t *testing.T) {
	t.Parallel()
	s := mergedIdentitySeries()
	be := faultbackend.Wrap(backend.Memory())
	var builder seriesindex.Builder
	require.NoError(t, builder.Add(s.Hash(), s, seriesindex.Metadata{Covered: 500}))
	require.NoError(t, builder.WriteObject(t.Context(), be, "index"))
	idx, err := seriesindex.Open(t.Context(), be, "index")
	require.NoError(t, err)
	injected := errors.New("descriptor read failed")
	be.Add(faultbackend.Rule{Kind: faultbackend.Read, Err: injected})
	first := &diskMergeSource{cursor: idx.SeriesCursor(t.Context())}
	last := mergeIdentityFixture(t, []signal.Series{s}, 100, false)
	_, _, err = mergeDiskIdentity(s.Hash(), []mergestream.Position{{Source: 0, Index: 0}, {Source: 1, Index: 0}}, []*diskMergeSource{first, last})
	require.ErrorIs(t, err, injected)
}

func TestMergeDiskIdentityOwnsAcrossDescriptorPages(t *testing.T) {
	t.Parallel()
	input := make([]signal.Series, 3)
	for i := range input {
		input[i] = signal.Series{Attributes: signal.NewAttributes(
			signal.KeyValue{Key: []byte("blob"), Value: signal.BytesValue(bytes.Repeat([]byte{byte(i + 1)}, 40<<10))},
			signal.KeyValue{Key: []byte("id"), Value: signal.IntValue(int64(i))},
		)}
	}
	slices.SortFunc(input, func(a, b signal.Series) int { return a.Hash().Compare(b.Hash()) })
	sources := []*diskMergeSource{mergeIdentityFixture(t, input, 200, false), mergeIdentityFixture(t, input, 100, false)}
	_, borrowed, _, err := sources[1].cursor.At(0)
	require.NoError(t, err)
	original := bytes.Clone(borrowed)
	var builder seriesindex.Builder
	for i := range input {
		id := input[i].Hash()
		got, covered, err := mergeDiskIdentity(id, []mergestream.Position{{Source: 0, Index: i}, {Source: 1, Index: i}}, sources)
		require.NoError(t, err)
		require.NoError(t, builder.Add(id, got, seriesindex.Metadata{Covered: covered}))
	}
	require.NotEqual(t, original, borrowed, "the source decoder overwrote the first borrowed page")
	for i := range input {
		require.Equal(t, input[i].AppendHashInput(nil), builder.View().AppendCanonical(nil, i))
	}
}

func TestMergeDiskIdentityReusesDifferentAttributeSets(t *testing.T) {
	t.Parallel()
	input := []signal.Series{mergedIdentitySeries(), benchSeries(500), {}}
	slices.SortFunc(input, func(a, b signal.Series) int { return a.Hash().Compare(b.Hash()) })
	sources := []*diskMergeSource{mergeIdentityFixture(t, input, 200, false), mergeIdentityFixture(t, input, 100, false)}
	var builder seriesindex.Builder
	for i := range input {
		id := input[i].Hash()
		positions := []mergestream.Position{{Source: i % 2, Index: i}, {Source: (i + 1) % 2, Index: i}}
		got, covered, err := mergeDiskIdentity(id, positions, sources)
		require.NoError(t, err)
		require.Equal(t, input[i].AppendHashInput(nil), got.AppendHashInput(nil))
		require.NoError(t, builder.Add(id, got, seriesindex.Metadata{Covered: covered}))
	}
	for i := range input {
		require.Equal(t, input[i].AppendHashInput(nil), builder.View().AppendCanonical(nil, i))
	}
}
