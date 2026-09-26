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

// decompressBounded decompresses src into dst's capacity held to limit bytes, exactly limit when
// exact. A negative limit decodes unbounded. A bound violation is [ErrCorrupt] wrapping the compress
// error.
func decompressBounded(comp *compress.Compressor, dst, src []byte, limit int64, exact bool) ([]byte, error) {
	if limit < 0 {
		return comp.Decompress(dst[:0], src)
	}

	out, err := comp.DecompressLimit(dst, src, int(min(limit, math.MaxInt)))
	if err != nil {
		return nil, errors.Errorf("%w: %w", ErrCorrupt, err)
	}

	if exact && int64(len(out)) != limit {
		return nil, errors.Wrapf(ErrCorrupt, "decompressed %d bytes, want %d", len(out), limit)
	}

	return out, nil
}

// decompressKept is [decompressBounded] for a buffer the caller keeps. A bounded zstd decode needs a
// block of slack past the content, which a kept buffer would hold for its whole life, so unless dst
// already has that room the frame decodes into a pooled scratch and is copied into dst, or into an
// exact-size buffer when dst is short.
func decompressKept(comp *compress.Compressor, dst, src []byte, limit int64, exact bool) ([]byte, error) {
	slack := int64(comp.OutputSlack())
	if slack == 0 || limit < 0 || int64(cap(dst)) >= satAdd(limit, slack) {
		return decompressBounded(comp, dst, src, limit, exact)
	}

	scratch := getScratch()

	out, err := decompressBounded(comp, scratch, src, limit, exact)
	if err != nil {
		putScratch(scratch)

		return nil, err
	}

	kept := dst[:0]
	if cap(kept) < len(out) {
		kept = make([]byte, 0, len(out))
	}

	kept = append(kept, out...)
	putScratch(out)

	return kept, nil
}

// Scratch buffers for [decompressKept], held in a fixed set that survives collections: a
// [sync.Pool] is emptied by every other collection, and a scratch rebuilt on each miss costs the
// frame plus its slack, which is what a kept buffer avoids holding. The set holds at most
// scratchBufs × maxScratchBuf for the process.
const (
	scratchBufs   = 4
	maxScratchBuf = 1 << 20
)

var scratches = make(chan []byte, scratchBufs)

func getScratch() []byte {
	select {
	case buf := <-scratches:
		return buf
	default:
		return nil
	}
}

func putScratch(buf []byte) {
	if buf == nil || cap(buf) > maxScratchBuf {
		return
	}

	select {
	case scratches <- buf[:0]:
	default:
	}
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
