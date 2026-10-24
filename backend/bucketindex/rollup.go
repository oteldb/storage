package bucketindex

import (
	"encoding/binary"
	"math"
)

// Rollup is the downsampling layout a part's rows have had applied: per tier, rows older than Before
// reduced to one per Interval-wide bucket by Agg, an aggregation id this package does not interpret.
// No tiers is a raw part.
type Rollup struct {
	Tiers []RollupTier
}

// RollupTier is one tier of a [Rollup].
type RollupTier struct {
	Before   int64
	Interval int64
	Agg      uint8
}

// appendRollup writes a known layout; an unknown one is only the absence of its flag bit.
func appendRollup(dst []byte, r *Rollup) []byte {
	if r == nil {
		return dst
	}

	dst = binary.AppendUvarint(dst, uint64(len(r.Tiers)))
	for _, t := range r.Tiers {
		dst = binary.AppendVarint(dst, t.Before)
		dst = binary.AppendVarint(dst, t.Interval)
		dst = binary.AppendUvarint(dst, uint64(t.Agg))
	}

	return dst
}

func readRollup(buf []byte) (*Rollup, []byte, bool) {
	n, buf, ok := readUvarint(buf)
	if !ok || n > uint64(len(buf)) {
		return nil, nil, false
	}

	r := &Rollup{}
	if n > 0 {
		r.Tiers = make([]RollupTier, 0, n)
	}

	for range n {
		var (
			t   RollupTier
			agg uint64
		)

		if t.Before, buf, ok = readVarint(buf); !ok {
			return nil, nil, false
		}

		if t.Interval, buf, ok = readVarint(buf); !ok {
			return nil, nil, false
		}

		if agg, buf, ok = readUvarint(buf); !ok || agg > math.MaxUint8 {
			return nil, nil, false
		}

		t.Agg = uint8(agg)
		r.Tiers = append(r.Tiers, t)
	}

	return r, buf, true
}
