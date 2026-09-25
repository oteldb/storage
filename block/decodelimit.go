package block

import (
	"math"

	"github.com/go-faster/errors"

	"github.com/oteldb/storage/encoding/compress"
)

// unlimited marks a decode with no recorded size to hold it to: a part whose manifest predates
// RawBytes and sizing.
const unlimited = -1

// Slack in the bounds a manifest without sizing implies. A chunk stream is at most its column's
// share of RawBytes plus streamRowSlack per row (dictionary ids and length prefixes, raw lengths, a
// Gorilla value's worst case) plus streamSlack: T64 writes its last 64-row block's bit planes in
// full, up to 538 bytes for one row. A leading dictionary adds at most a 5-byte prefix per entry.
const (
	streamRowSlack = 16
	streamSlack    = 1 << 10
	dictEntrySlack = 5
)

// decodeLimits are the sizes a column's decompression is held to. Frames carry their own exact size
// in the directory; these cover what the directory does not.
type decodeLimits struct {
	// stream bounds an unframed stream and each legacy frame; exact when the manifest records it.
	stream int64
	exact  bool
	// frames bounds a framed column's decompressed total, plus framesPerGranule per granule.
	frames           int64
	framesPerGranule int64
	// dict bounds a leading-layout dictionary.
	dict int64
}

// columnLimits derives desc's decode limits from its manifest: exact with sizing, bounded by the
// part's RawBytes without, and unlimited when the part records neither.
func columnLimits(desc ColumnDesc, rawBytes int64, rows int) decodeLimits {
	l := decodeLimits{stream: unlimited, frames: unlimited, dict: unlimited}

	if rawBytes > 0 {
		bound := satAdd(rawBytes, satMul(streamRowSlack, int64(rows)))
		l.stream, l.frames, l.framesPerGranule = satAdd(bound, streamSlack), bound, streamSlack
		l.dict = satAdd(rawBytes, dictEntrySlack*sharedEntriesCeiling)
	}

	if desc.HasSizing {
		l.stream, l.exact = desc.Sizing.StreamRaw, true
		l.frames, l.framesPerGranule = desc.Sizing.StreamRaw, 0
	}

	return l
}

// decompressBounded decompresses src onto dst held to limit bytes, exactly limit when exact. A
// negative limit decodes unbounded. A bound violation is [ErrCorrupt] wrapping the compress error.
func decompressBounded(comp *compress.Compressor, dst, src []byte, limit int64, exact bool) ([]byte, error) {
	if limit < 0 {
		return comp.Decompress(dst, src)
	}

	out, err := comp.DecompressLimit(dst, src, int(min(limit, math.MaxInt)))
	if err != nil {
		return nil, errors.Errorf("%w: %w", ErrCorrupt, err)
	}

	if got := int64(len(out) - len(dst)); exact && got != limit {
		return nil, errors.Wrapf(ErrCorrupt, "decompressed %d bytes, want %d", got, limit)
	}

	return out, nil
}

func satAdd(a, b int64) int64 {
	if a > math.MaxInt64-b {
		return math.MaxInt64
	}

	return a + b
}

// satMul multiplies non-negative a and b, saturating at MaxInt64.
func satMul(a, b int64) int64 {
	if a != 0 && b > math.MaxInt64/a {
		return math.MaxInt64
	}

	return a * b
}
