package cluster

import (
	"encoding/binary"
	"math"

	"github.com/go-faster/errors"

	"github.com/oteldb/storage/query/fetch"
)

// The scale-factor trailer of the metric fan-out frame ([EncodeBatches]).
//
// [fetch.Batch.ScaleFactors] is a sample's lossy-sampling weight, and a nil slice is the distinct
// "no sampling occurred" signal that query/fetch's merge and query/scale's cache both read. So it
// must come back nil rather than as a run of 1s, and the trailer names only the batches that carry
// weights — by index — and is omitted entirely when none do.
//
// It sits after the last batch because that is the one position an old peer skips: [DecodeBatches]
// stops as soon as it has read the batch count and ignores whatever follows, the same append-only
// tail property [FetchRequest.Encode]'s condition hints already rely on. So mixed-version fan-out
// degrades to the pre-fix behavior (the weights default to 1) rather than failing or, worse,
// misparsing: a version byte or a per-batch field would be read by an old decoder as a batch count
// or an identity length, decoding garbage samples.

// appendScaleFactors writes the trailer for batches, or nothing when none of them is sampled.
func appendScaleFactors(dst []byte, batches []*fetch.Batch) []byte {
	var weighted int

	for _, b := range batches {
		if b.ScaleFactors != nil {
			weighted++
		}
	}

	if weighted == 0 {
		return dst
	}

	dst = binary.AppendUvarint(dst, uint64(weighted))

	for i, b := range batches {
		if b.ScaleFactors == nil {
			continue
		}

		dst = binary.AppendUvarint(dst, uint64(i))
		dst = binary.AppendUvarint(dst, uint64(len(b.ScaleFactors)))

		for _, sf := range b.ScaleFactors {
			dst = binary.BigEndian.AppendUint64(dst, math.Float64bits(sf))
		}
	}

	return dst
}

// decodeScaleFactors applies the trailer to the already-decoded batches. Empty data is a peer that
// predates the trailer: every batch keeps the nil that means "unsampled".
func decodeScaleFactors(data []byte, batches []*fetch.Batch) error {
	if len(data) == 0 {
		return nil
	}

	count, m := binary.Uvarint(data)
	if m <= 0 {
		return errors.New("cluster: malformed scale factor trailer")
	}
	data = data[m:]

	for range count {
		idx, m := binary.Uvarint(data)
		if m <= 0 || idx >= uint64(len(batches)) {
			return errors.New("cluster: malformed scale factor batch index")
		}
		data = data[m:]

		n, m := binary.Uvarint(data)
		if m <= 0 || n > uint64(len(data)-m)/8 {
			return errors.New("cluster: malformed scale factor count")
		}
		data = data[m:]

		b := batches[idx]
		if n != uint64(len(b.Timestamps)) {
			return errors.New("cluster: scale factor count does not match sample count")
		}

		sf := make([]float64, n)
		for i := range sf {
			sf[i] = math.Float64frombits(binary.BigEndian.Uint64(data))
			data = data[8:]
		}

		b.ScaleFactors = sf
	}

	return nil
}
