package block

import (
	"context"
	"encoding/binary"

	"github.com/go-faster/errors"

	"github.com/oteldb/storage/backend"
)

// A block-framed column is read one compression frame at a time rather than whole: the directory
// locates every frame, so a query touching a few granules can fetch a few frames instead of the
// object holding them. Without it a selector matching 16 of 210k series still pays for all 210k,
// because the column is one object — which is what makes part size a bound on process memory
// instead of on disk.
//
// The directory itself is always read up front. It is two integers per frame and one per granule —
// roughly 1.4 MB for an 833 MB column at the default granule and frame sizes — and every later read
// is derived from it, so it is read once per column open and kept.
//
// Where it sits depends on the layout the column was written under ([ColumnDesc.Footer]), and that
// changes only how it is found, never how it is parsed.

// dirProbeBytes is the first read of a directory-leading column: enough for the three header
// uvarints, from which the directory's exact extent follows. Sized at a filesystem block rather
// than the ~15 bytes actually needed, since no backend charges less for a smaller read.
const dirProbeBytes = 4096

// ColumnBlocks returns a per-block decoder for the named column that reads only the block directory
// up front and fetches each compression frame by ranged read as blocks are decoded.
//
// It is the read counterpart of the streaming writer: the whole-object [PartReader.Column] is right
// when a caller decodes the whole column, and this is right when it decodes a fraction of it — the
// query path, where the matched series' rows lie in a handful of granules. The returned decoder
// holds ctx for its frame reads, so it must not outlive the operation that opened it.
//
// The column must be block-framed and not constant-collapsed; [PartReader.Column] handles those.
func (r *PartReader) ColumnBlocks(ctx context.Context, name string) (*Decoder, error) {
	i, ok := r.byName[name]
	if !ok {
		return nil, errors.Errorf("block: no column %q", name)
	}

	var err error

	desc := r.manifest.Columns[i]
	switch {
	case desc.Const:
		return nil, errors.Errorf("block: column %q is constant-collapsed and has no blocks", name)
	case !desc.Blocked:
		return nil, errors.Errorf("block: column %q is not blocked", name)
	}

	key := columnKey(r.prefix, i)

	// The manifest records the object's size, so opening a column normally costs no round trip
	// beyond the directory read. A part written before it did falls back to asking the backend —
	// whose own fallback, for a backend without [backend.Sizer], is to read the whole object, which
	// is exactly what this path exists to avoid. Wrappers must forward Sizer.
	size := desc.Bytes
	if size == 0 {
		if size, err = backend.SizeOf(ctx, r.b, key); err != nil {
			return nil, errors.Wrapf(err, "size column %q", name)
		}
	}

	dir, err := readBlockDir(ctx, r.b, key, desc, size)
	if err != nil {
		return nil, errors.Wrapf(err, "column %q", name)
	}

	cr := newColumnReader(desc, nil, r.compressorFor(desc.Compress), r.manifest.RowCount)

	return &Decoder{
		rows:    r.manifest.RowCount,
		i64:     cr.int64Decoder(),
		f64:     cr.float64Decoder(),
		streams: newBlockStreams(dir, cr.comp),
	}, nil
}

// readBlockDir reads and parses a block-framed column's directory without reading its frames,
// leaving the returned [blockDir] pointed at the backend for those.
func readBlockDir(
	ctx context.Context, b backend.Backend, key string, desc ColumnDesc, size int64,
) (blockDir, error) {
	if !desc.Framed {
		// The legacy one-compressed-block-per-granule directory has no length it can be found by
		// without walking it, and no part written since predates the framed form.
		return blockDir{}, errors.Wrap(ErrCorrupt, "the legacy blocked layout cannot be read by range")
	}

	var (
		d   blockDir
		err error
	)

	if desc.Footer {
		d, err = readFooterDir(ctx, b, key, size)
	} else {
		d, err = readLeadingDir(ctx, b, key, size)
	}

	if err != nil {
		return blockDir{}, err
	}

	d.src = &frameSource{ctx: ctx, b: b, key: key, base: d.dataOff}

	return d, nil
}

// readFooterDir reads the directory of a column that carries it after its frames. The trailing
// length is at a known offset from the end, so one read of the tail usually lands the whole
// directory and the second read is needed only for a directory larger than the probe.
func readFooterDir(ctx context.Context, b backend.Backend, key string, size int64) (blockDir, error) {
	probe := min(size, dirProbeBytes)

	tail, err := backend.ReadAt(ctx, b, key, size-probe, probe)
	if err != nil {
		return blockDir{}, err
	}

	if int64(len(tail)) != probe || len(tail) < footerLenBytes {
		return blockDir{}, errors.Wrap(ErrCorrupt, "block dir footer truncated")
	}

	dirLen := int64(binary.LittleEndian.Uint32(tail[len(tail)-footerLenBytes:]))
	if dirLen > size-footerLenBytes {
		return blockDir{}, errors.Wrapf(ErrCorrupt, "block dir footer len %d exceeds object", dirLen)
	}

	dataOff := size - footerLenBytes - dirLen

	raw := tail[:len(tail)-footerLenBytes]
	if dirLen > int64(len(raw)) {
		if raw, err = backend.ReadAt(ctx, b, key, dataOff, dirLen); err != nil {
			return blockDir{}, err
		}

		if int64(len(raw)) != dirLen {
			return blockDir{}, errors.Wrap(ErrCorrupt, "block dir footer truncated")
		}
	}

	raw = raw[int64(len(raw))-dirLen:]

	d, pos, total, err := parseFramedDirFields(raw, int(dataOff))
	if err != nil {
		return blockDir{}, err
	}

	if int64(pos) != dirLen {
		return blockDir{}, errors.Wrapf(ErrCorrupt, "block dir footer has %d trailing bytes", dirLen-int64(pos))
	}

	if int64(total) != dataOff {
		return blockDir{}, errors.Wrapf(ErrCorrupt, "block dir frames total %d, want %d", total, dataOff)
	}

	// Frames start at the object's head under this layout.
	d.dataOff = 0

	return d, nil
}

// readLeadingDir reads the directory of a column that carries it before its frames. Its length is
// not recorded anywhere, so it is bounded from the counts in its own header: every frame costs at
// most a granule-count varint plus a length varint, and every granule at most a length varint. That
// bound is tight enough to read in one further request — ~1.4 MB for a directory over an 833 MB
// column, against the 15 MB a worst-case-varint bound would ask for.
func readLeadingDir(ctx context.Context, b backend.Backend, key string, size int64) (blockDir, error) {
	raw, err := backend.ReadAt(ctx, b, key, 0, min(size, dirProbeBytes))
	if err != nil {
		return blockDir{}, err
	}

	nGranules, nFrames, err := peekDirCounts(raw)
	if err != nil {
		return blockDir{}, err
	}

	bound := dirBound(nGranules, nFrames, size)
	if bound > int64(len(raw)) {
		if raw, err = backend.ReadAt(ctx, b, key, 0, min(bound, size)); err != nil {
			return blockDir{}, err
		}
	}

	d, pos, total, err := parseFramedDirFields(raw, int(size))
	if err != nil {
		return blockDir{}, err
	}

	if int64(pos)+int64(total) != size {
		return blockDir{}, errors.Wrapf(ErrCorrupt, "block dir describes %d bytes, object holds %d", int64(pos)+int64(total), size)
	}

	d.dataOff = int64(pos)

	return d, nil
}

// peekDirCounts reads the granule and frame counts from a directory's header.
func peekDirCounts(raw []byte) (int64, int64, error) {
	c := dirCursor{object: raw}

	nGranules, err := c.uvarint("nGranules")
	if err != nil {
		return 0, 0, err
	}

	if _, err := c.uvarint("blockRows"); err != nil {
		return 0, 0, err
	}

	nFrames, err := c.uvarint("nFrames")
	if err != nil {
		return 0, 0, err
	}

	return int64(nGranules), int64(nFrames), nil
}

// dirBound is the largest a directory over the given counts can be. A frame's granule count cannot
// exceed the granule total and its compressed length cannot exceed the object, so both take a bound
// from what they describe. A *granule's* length cannot: it is measured in the decompressed frame,
// which is larger than the compressed bytes on disk by the compression ratio, so it gets the
// unconditional uvarint bound instead. Loose by a few bytes per granule — megabytes against a
// column of hundreds of them — and, unlike a tighter guess, never short.
func dirBound(nGranules, nFrames, size int64) int64 {
	frameBytes := int64(varintLen(uint64(nGranules))) + int64(varintLen(uint64(size)))

	return dirProbeBytes + nFrames*frameBytes + nGranules*binary.MaxVarintLen64
}

// varintLen is the encoded length of v as a uvarint.
func varintLen(v uint64) int {
	n := 1
	for v >= 0x80 {
		v >>= 7
		n++
	}

	return n
}

// frameSource fetches a column's compression frames from the backend as they are needed, so a
// decoder holds one frame rather than the column.
type frameSource struct {
	// ctx belongs to the operation that opened the decoder and bounds its reads; the decoder is
	// built per fetch and must not outlive it. Held because the decode calls it serves take no ctx
	// of their own — they are per block, well inside one fetch.
	ctx  context.Context //nolint:containedctx // scoped to the fetch that opened the decoder
	b    backend.Backend
	key  string
	base int64 // absolute offset of the frame data region within the object
}

// frame returns frame [off, off+n) of the data region, read from the backend. The bytes are taken
// as a read-only view where the backend can offer one: a frame is decompressed on arrival and never
// retained or mutated, so ranging costs the in-memory backend no copy it did not pay before.
func (s *frameSource) frame(off, n int64) ([]byte, error) {
	buf, err := backend.ReadViewAt(s.ctx, s.b, s.key, s.base+off, n)
	if err != nil {
		return nil, errors.Wrapf(err, "read frame at [%d,+%d)", s.base+off, n)
	}

	if int64(len(buf)) != n {
		return nil, errors.Wrapf(ErrCorrupt, "frame at [%d,+%d) is %d bytes", s.base+off, n, len(buf))
	}

	return buf, nil
}
