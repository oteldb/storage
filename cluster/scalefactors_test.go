package cluster_test

import (
	"encoding/binary"
	"math"
	"testing"

	"github.com/go-faster/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/cluster"
	"github.com/oteldb/storage/query/fetch"
	"github.com/oteldb/storage/signal"
)

// weighted returns a batch carrying lossy-sampling weights.
func weighted(job string, sf []float64, samples ...[2]int64) *fetch.Batch {
	b := batch(job, samples...)
	b.ScaleFactors = sf

	return b
}

// decodeBatchesPreTrailer is the decoder as it stood before the scale-factor trailer, kept to
// prove what a peer running the old build does with a new frame. It returns the bytes it leaves
// unread, which the old build discarded.
func decodeBatchesPreTrailer(data []byte) ([]*fetch.Batch, []byte, error) {
	count, m := binary.Uvarint(data)
	if m <= 0 {
		return nil, nil, errors.New("malformed batches")
	}
	data = data[m:]

	out := make([]*fetch.Batch, 0, count)

	for range count {
		sl, m := binary.Uvarint(data)
		if m <= 0 || sl > uint64(len(data)-m) {
			return nil, nil, errors.New("malformed batch identity")
		}
		data = data[m:]

		s, _, err := signal.DecodeSeries(data[:sl])
		if err != nil {
			return nil, nil, err
		}
		data = data[sl:]

		ns, m := binary.Uvarint(data)
		if m <= 0 {
			return nil, nil, errors.New("malformed sample count")
		}
		data = data[m:]

		b := &fetch.Batch{ID: s.Hash(), Series: s}
		for range ns {
			ts, m := binary.Varint(data)
			if m <= 0 || len(data)-m < 8 {
				return nil, nil, errors.New("malformed sample")
			}
			data = data[m:]
			b.Timestamps = append(b.Timestamps, ts)
			b.Values = append(b.Values, math.Float64frombits(binary.BigEndian.Uint64(data)))
			data = data[8:]
		}

		out = append(out, b)
	}

	return out, data, nil
}

// TestBatchesCodecKeepsScaleFactors is the cluster-hop counterpart of
// TestMergeFederatedKeepsScaleFactors: a sampled series read from a peer must arrive with the
// weights it was stored with, or the requester counts every kept sample once and under-reports any
// rate over a sampled tenant.
func TestBatchesCodecKeepsScaleFactors(t *testing.T) {
	t.Parallel()

	in := []*fetch.Batch{
		weighted("api", []float64{8, 4}, [2]int64{100, 1}, [2]int64{200, 2}),
		batch("web", [2]int64{100, 9}),
		weighted("db", []float64{2}, [2]int64{300, 3}),
	}

	out, err := cluster.DecodeBatches(cluster.EncodeBatches(in))
	require.NoError(t, err)
	require.Len(t, out, 3)

	assert.Equal(t, []float64{8, 4}, out[0].ScaleFactors)
	assert.InDelta(t, 8.0, out[0].ScaleFactor(0), 0)
	assert.InDelta(t, 4.0, out[0].ScaleFactor(1), 0)

	// A nil ScaleFactors is the "no sampling occurred" signal, so it must come back nil — not an
	// empty slice, and not a run of 1s.
	assert.Nil(t, out[1].ScaleFactors, "an unsampled batch stays unsampled")
	assert.InDelta(t, 1.0, out[1].ScaleFactor(0), 0)

	assert.Equal(t, []float64{2}, out[2].ScaleFactors)
	assert.Equal(t, []int64{300}, out[2].Timestamps, "the weights are not confused with the samples")
}

// TestBatchesCodecUnsampledPaysNothing pins the cost of the presence flag: a wholly-unsampled frame
// carries no trailer at all, so the common path is byte-identical to what a peer that predates the
// trailer wrote, and a weight costs 8 bytes only where one exists.
func TestBatchesCodecUnsampledPaysNothing(t *testing.T) {
	t.Parallel()

	samples := [][2]int64{{100, 1}, {200, 2}}

	unsampled := cluster.EncodeBatches([]*fetch.Batch{batch("api", samples...)})
	sampled := cluster.EncodeBatches([]*fetch.Batch{weighted("api", []float64{8, 4}, samples...)})

	require.Equal(t, unsampled, sampled[:len(unsampled)], "the trailer is an append-only tail")
	// count + index + length, each a one-byte uvarint here, then one float64 per weight.
	assert.Len(t, sampled, len(unsampled)+3+2*8)
}

// TestBatchesCodecMixedVersion covers the rolling upgrade in both directions. The trailer is an
// append-only tail, so neither side misparses: an old peer's frame decodes here as unsampled, and a
// new peer's frame decodes on an old requester as the batches it always read, the trailer ignored.
func TestBatchesCodecMixedVersion(t *testing.T) {
	t.Parallel()

	samples := [][2]int64{{100, 1}, {200, 2}}

	// New decoder, old encoder: a frame written without a trailer reads as unsampled.
	oldFrame := cluster.EncodeBatches([]*fetch.Batch{batch("api", samples...)})

	out, err := cluster.DecodeBatches(oldFrame)
	require.NoError(t, err)
	require.Len(t, out, 1)
	assert.Equal(t, []int64{100, 200}, out[0].Timestamps)
	assert.Nil(t, out[0].ScaleFactors, "an old peer's frame reads as unsampled, never as garbage")

	// Old decoder, new encoder: the pre-trailer decode stops after the batch count, so it reads the
	// samples correctly and leaves the trailer unread — the pre-fix behavior, until both ends
	// upgrade, rather than a misparse.
	newFrame := cluster.EncodeBatches([]*fetch.Batch{weighted("api", []float64{8, 4}, samples...)})

	got, rest, err := decodeBatchesPreTrailer(newFrame)
	require.NoError(t, err)
	require.Len(t, got, 1)
	assert.Equal(t, []int64{100, 200}, got[0].Timestamps)
	assert.Equal(t, []float64{1, 2}, got[0].Values)
	assert.Nil(t, got[0].ScaleFactors)
	assert.Len(t, rest, 3+2*8, "what the old decoder leaves unread is exactly the trailer")
}

// TestBatchesCodecRejectsMalformedTrailer: a corrupt trailer is a clean error, never a silent
// mis-weighting.
func TestBatchesCodecRejectsMalformedTrailer(t *testing.T) {
	t.Parallel()

	base := cluster.EncodeBatches([]*fetch.Batch{batch("api", [2]int64{100, 1})})

	with := func(trailer ...byte) []byte {
		return append(append([]byte(nil), base...), trailer...)
	}

	for name, corrupt := range map[string][]byte{
		"truncated weight":   with(0x01, 0x00, 0x01, 0, 0, 0, 0, 0, 0, 0),
		"truncated trailer":  with(0x01),
		"batch out of range": with(0x01, 0x09, 0x01, 0, 0, 0, 0, 0, 0, 0, 0),
		"weight count":       with(0x01, 0x00, 0x02, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0),
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			_, err := cluster.DecodeBatches(corrupt)
			require.Error(t, err)
		})
	}
}
