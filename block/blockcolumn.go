package block

import (
	"encoding/binary"
	"math"

	"github.com/go-faster/errors"

	"github.com/oteldb/storage/encoding/chunk"
	"github.com/oteldb/storage/encoding/compress"
)

// errCursorEOF is returned by a blocked cursor's Next when every block has been consumed. The
// streaming merge drives each cursor with a known row count and never over-reads, so this only
// surfaces a caller bug.
var errCursorEOF = errors.New("block: blocked cursor exhausted")

// Block-framed columns split a column into fixed-size row blocks (granules), each an independent
// codec stream (the chunk codecs reset their running state — delta-of-delta, Gorilla
// leading/trailing — at row 0 of every Encode call, so a granule over rows [lo,hi) is decodable
// without touching any earlier row). A small directory locates each granule, so a reader can decode
// a single one — the basis for sub-part seek (decode only the granules a query's time window or
// matched series' row range touches) without decoding the whole column.
//
// The decode granule and the compression frame are separate: a granule is ~1.6 KB of stream, far too
// little context for an entropy coder, so consecutive granules are concatenated into a compression
// frame of at least compressBytes and the frame is compressed as a unit (ClickHouse's
// index_granularity vs min_compress_block_size). Granularity of decode stays at blockRows;
// granularity of compression is the frame.
//
// Object layout:
//
//	[uvarint nGranules][uvarint blockRows][uvarint nFrames]
//	  per frame:   [uvarint granulesInFrame][uvarint compressedLen]
//	  per granule: [uvarint streamLen]                 (length within the decompressed frame)
//	[frame0][frame1]…
//
// blockRows is the nominal rows per granule (the last granule may hold fewer); row r lives in
// granule r/blockRows. Each frame is comp.Compress(concatenated streams of its granules). The
// granule boundaries align with the part's marks granules (same size), so the marks index already
// carries each granule's [minTime,maxTime] for time-pruning — the directory need not repeat it.
//
// The prior layout — one compressed block per granule, directory
// [uvarint nBlocks][uvarint blockRows][nBlocks × uvarint blockLen] — is still read (a column written
// under it has [flagBlocked] set but not [flagFramed]), so existing parts read unchanged; the writer
// only emits the framed layout.

// defaultCompressBlockBytes is the minimum uncompressed bytes packed into one compression frame of a
// block-framed column (ClickHouse's min_compress_block_size default). At the 1024-row decode granule
// a timestamp stream is ~1.6 KB, so a frame gathers tens of granules' worth of context.
const defaultCompressBlockBytes = 64 << 10

// encodeBlocked serializes c as a block-framed column: each blockRows-row granule is codec-encoded
// (codec, with the decimal precision budget for CodecDecimal), consecutive granules are packed into
// compression frames of at least compressBytes and compressed per frame, all preceded by the
// directory. blockRows must be > 0; compressBytes ≤ 0 selects [defaultCompressBlockBytes].
func encodeBlocked(
	c Column, codec chunk.Codec, budget uint8, comp *compress.Compressor, blockRows, compressBytes int,
) ([]byte, error) {
	if blockRows <= 0 {
		return nil, errors.Errorf("block: blockRows must be > 0, got %d", blockRows)
	}

	if compressBytes <= 0 {
		compressBytes = defaultCompressBlockBytes
	}

	n := c.rows()

	var (
		frames    [][]byte
		frameLens []int // granules per frame
		gLens     []int // per-granule stream length within its frame
		pending   []byte
		inFrame   int
	)

	seal := func() {
		if inFrame == 0 {
			return
		}

		frames = append(frames, comp.Compress(nil, pending))
		frameLens = append(frameLens, inFrame)
		pending, inFrame = pending[:0], 0
	}

	for lo := 0; lo < n; lo += blockRows {
		hi := min(lo+blockRows, n)

		before := len(pending)

		var err error
		if pending, err = appendBlockStream(pending, c, codec, budget, lo, hi); err != nil {
			return nil, err
		}

		gLens = append(gLens, len(pending)-before)
		inFrame++

		if len(pending) >= compressBytes {
			seal()
		}
	}

	seal()

	dst := binary.AppendUvarint(nil, uint64(len(gLens)))
	dst = binary.AppendUvarint(dst, uint64(blockRows))
	dst = binary.AppendUvarint(dst, uint64(len(frames)))

	for i, f := range frames {
		dst = binary.AppendUvarint(dst, uint64(frameLens[i]))
		dst = binary.AppendUvarint(dst, uint64(len(f)))
	}

	for _, l := range gLens {
		dst = binary.AppendUvarint(dst, uint64(l))
	}

	for _, f := range frames {
		dst = append(dst, f...)
	}

	return dst, nil
}

// appendBlockStream codec-encodes c's rows [lo,hi) onto dst as a single chunk stream. Only the
// per-row sequential codecs used by the metric ts/value/sf columns are blockable; other codecs error.
func appendBlockStream(dst []byte, c Column, codec chunk.Codec, budget uint8, lo, hi int) ([]byte, error) {
	switch {
	case c.Kind == KindInt64 && codec == chunk.CodecDoD:
		return chunk.EncodeTimestamps(dst, c.Int64[lo:hi]), nil
	case c.Kind == KindInt64 && codec == chunk.CodecT64:
		return chunk.EncodeIntsT64(dst, c.Int64[lo:hi]), nil
	case c.Kind == KindFloat64 && codec == chunk.CodecGorilla:
		return chunk.EncodeFloats(dst, c.Float64[lo:hi]), nil
	case c.Kind == KindFloat64 && codec == chunk.CodecDecimal:
		return chunk.EncodeFloatsDecimal(dst, c.Float64[lo:hi], decimalPrecision(budget)), nil
	default:
		return nil, errors.Errorf("block: codec %s for kind %s is not blockable", codec, c.Kind)
	}
}

// decimalPrecision maps a lossy precision budget (0 ⇒ lossless) to the bit count the scaled-decimal
// codec takes, mirroring the unblocked path (which uses [decimalPrecisionLossless] for lossless).
func decimalPrecision(budget uint8) uint8 {
	if budget == 0 || budget >= decimalPrecisionLossless {
		return decimalPrecisionLossless
	}

	return budget
}

// blockDir is a parsed block directory: the compression frames' byte spans within the data region
// and, for the framed layout, each granule's span within its (decompressed) frame. In the legacy
// layout every granule is its own frame, so the per-granule spans are absent (gFrame is nil) and a
// granule's stream is the whole decompressed frame.
type blockDir struct {
	blockRows int
	granules  int
	frameOff  []int32 // cumulative byte offsets into data; len == nFrames+1
	gFrame    []int32 // per granule: owning frame; nil in the legacy layout
	gOff      []int32 // per granule: byte offset within the decompressed frame
	gLen      []int32 // per granule: byte length within the decompressed frame
	data      []byte
}

// nBlocks returns the number of decode granules.
func (d blockDir) nBlocks() int { return d.granules }

// frame returns frame f's raw (still compressed) bytes.
func (d blockDir) frame(f int) []byte { return d.data[d.frameOff[f]:d.frameOff[f+1]] }

// frameOf returns the frame holding granule g.
func (d blockDir) frameOf(g int) int {
	if d.gFrame == nil {
		return g
	}

	return int(d.gFrame[g])
}

// granuleStream slices granule g's codec stream out of its already-decompressed frame.
func (d blockDir) granuleStream(g int, frame []byte) ([]byte, error) {
	if d.gFrame == nil {
		return frame, nil
	}

	lo, hi := int(d.gOff[g]), int(d.gOff[g])+int(d.gLen[g])
	if hi > len(frame) {
		return nil, errors.Wrapf(ErrCorrupt, "granule %d span [%d,%d) past frame %d", g, lo, hi, len(frame))
	}

	return frame[lo:hi], nil
}

// parseBlockDir reads the directory from a blocked column object. framed selects the layout: the
// current frame-packed one, or the legacy one-compressed-block-per-granule form. It bounds-checks
// every field against the object length so a corrupt object errors rather than panics.
func parseBlockDir(object []byte, framed bool) (blockDir, error) {
	if framed {
		return parseFramedDir(object)
	}

	return parseLegacyDir(object)
}

// dirCursor reads the uvarint fields of a block directory, tracking the read position and turning a
// malformed field into an [ErrCorrupt] error.
type dirCursor struct {
	object []byte
	pos    int
}

func (c *dirCursor) uvarint(what string) (uint64, error) {
	v, n := binary.Uvarint(c.object[c.pos:])
	if n <= 0 {
		return 0, errors.Wrapf(ErrCorrupt, "block dir %s", what)
	}

	c.pos += n

	return v, nil
}

// parseFramedDir parses the frame-packed directory (see the layout comment above).
func parseFramedDir(object []byte) (blockDir, error) {
	c := dirCursor{object: object}

	nGranules64, err := c.uvarint("nGranules")
	if err != nil {
		return blockDir{}, err
	}

	blockRows64, err := c.uvarint("blockRows")
	if err != nil {
		return blockDir{}, err
	}

	if blockRows64 == 0 {
		return blockDir{}, errors.Wrap(ErrCorrupt, "block dir blockRows is 0")
	}

	nFrames64, err := c.uvarint("nFrames")
	if err != nil {
		return blockDir{}, err
	}

	// Every granule and frame costs at least one directory byte, so neither count can exceed the
	// object length — the guard that keeps a corrupt count from driving a huge allocation.
	if nGranules64 > uint64(len(object)) || nFrames64 > uint64(len(object)) {
		return blockDir{}, errors.Wrapf(ErrCorrupt, "block dir counts %d/%d exceed object", nGranules64, nFrames64)
	}

	nGranules, nFrames := int(nGranules64), int(nFrames64)

	// One allocation for all four index arrays.
	buf := make([]int32, nFrames+1+3*nGranules)
	d := blockDir{
		blockRows: int(blockRows64),
		granules:  nGranules,
		frameOff:  buf[:nFrames+1],
		gFrame:    buf[nFrames+1 : nFrames+1+nGranules],
		gOff:      buf[nFrames+1+nGranules : nFrames+1+2*nGranules],
		gLen:      buf[nFrames+1+2*nGranules:],
	}

	total, err := readFrameTable(&c, &d, nFrames)
	if err != nil {
		return blockDir{}, err
	}

	if err := readGranuleLens(&c, &d); err != nil {
		return blockDir{}, err
	}

	if c.pos+total > len(object) {
		return blockDir{}, errors.Wrap(ErrCorrupt, "block dir data exceeds object")
	}

	d.data = object[c.pos : c.pos+total]

	return d, nil
}

// readFrameTable reads the per-frame (granule count, compressed length) pairs, filling d.frameOff and
// d.gFrame, and returns the frames' total compressed byte length.
func readFrameTable(c *dirCursor, d *blockDir, nFrames int) (int, error) {
	total, g := 0, 0

	for f := range nFrames {
		count, err := c.uvarint("frame granules")
		if err != nil {
			return 0, err
		}

		clen, err := c.uvarint("frame len")
		if err != nil {
			return 0, err
		}

		if count > uint64(d.granules) || g+int(count) > d.granules {
			return 0, errors.Wrapf(ErrCorrupt, "frame %d granule count %d overruns", f, count)
		}

		for range count {
			d.gFrame[g] = int32(f)
			g++
		}

		// Bound each length and the running total against the object so a corrupt uvarint cannot
		// overflow into a negative span (which would panic the data slice in the caller).
		if clen > uint64(len(c.object)) {
			return 0, errors.Wrapf(ErrCorrupt, "frame %d len too large", f)
		}

		total += int(clen)
		if total < 0 || total > len(c.object) {
			return 0, errors.Wrapf(ErrCorrupt, "frame %d data exceeds object", f)
		}

		d.frameOff[f+1] = int32(total)
	}

	if g != d.granules {
		return 0, errors.Wrapf(ErrCorrupt, "block dir frames hold %d granules, want %d", g, d.granules)
	}

	return total, nil
}

// readGranuleLens reads the per-granule stream lengths and turns them into (offset, length) spans
// within each granule's decompressed frame. Requires d.gFrame to be filled.
func readGranuleLens(c *dirCursor, d *blockDir) error {
	off, prevFrame := int32(0), int32(-1)

	for i := range d.granules {
		l, err := c.uvarint("granule len")
		if err != nil {
			return err
		}

		// A granule length is measured in the *decompressed* frame, so it is not bounded by the
		// object; only the running offset is bounded here, and [blockDir.granuleStream] checks the
		// resulting span against the frame it actually decompressed.
		if d.gFrame[i] != prevFrame {
			off, prevFrame = 0, d.gFrame[i]
		}

		// The running offset is an int32 like the spans it fills; a corrupt directory whose lengths
		// sum past that would wrap negative and panic the slice in [blockDir.granuleStream].
		if int64(off)+int64(l) > math.MaxInt32 {
			return errors.Wrapf(ErrCorrupt, "granule %d offset overflows", i)
		}

		d.gOff[i], d.gLen[i] = off, int32(l)
		off += int32(l)
	}

	return nil
}

// parseLegacyDir parses the pre-framing directory, where each granule is its own compressed block:
// [uvarint nBlocks][uvarint blockRows][nBlocks × uvarint blockLen] followed by the blocks.
func parseLegacyDir(object []byte) (blockDir, error) {
	nBlocks64, n := binary.Uvarint(object)
	if n <= 0 {
		return blockDir{}, errors.Wrap(ErrCorrupt, "block dir nBlocks")
	}

	pos := n

	blockRows64, n := binary.Uvarint(object[pos:])
	if n <= 0 {
		return blockDir{}, errors.Wrap(ErrCorrupt, "block dir blockRows")
	}

	if blockRows64 == 0 {
		return blockDir{}, errors.Wrap(ErrCorrupt, "block dir blockRows is 0")
	}

	pos += n

	nBlocks := int(nBlocks64)
	// Each block length is ≥1 byte of directory, so the count cannot exceed the object length.
	if nBlocks64 > uint64(len(object)) {
		return blockDir{}, errors.Wrapf(ErrCorrupt, "block dir nBlocks %d exceeds object", nBlocks64)
	}

	offsets := make([]int32, nBlocks+1)

	total := 0

	for i := range nBlocks {
		l64, n := binary.Uvarint(object[pos:])
		if n <= 0 {
			return blockDir{}, errors.Wrapf(ErrCorrupt, "block dir len %d", i)
		}

		pos += n

		// Bound each length and the running total against the object so a corrupt uvarint cannot
		// overflow int(l64) into a negative span (which would panic the data slice below).
		if l64 > uint64(len(object)) {
			return blockDir{}, errors.Wrapf(ErrCorrupt, "block dir len %d too large", i)
		}

		total += int(l64)
		if total < 0 || total > len(object) {
			return blockDir{}, errors.Wrapf(ErrCorrupt, "block dir data %d exceeds object", i)
		}

		offsets[i+1] = int32(total)
	}

	if pos+total > len(object) {
		return blockDir{}, errors.Wrap(ErrCorrupt, "block dir data exceeds object")
	}

	return blockDir{
		blockRows: int(blockRows64),
		granules:  nBlocks,
		frameOff:  offsets,
		data:      object[pos : pos+total],
	}, nil
}

// blockStreams decompresses granules of a blocked column, caching the last decompressed frame. Since
// a frame holds many consecutive granules, a caller walking granules in order (every read path does:
// whole-column decode, a row range, a sorted block set, a cursor) decompresses each frame once.
// Not safe for concurrent use; one instance serves one decode walk.
type blockStreams struct {
	dir   blockDir
	comp  *compress.Compressor
	frame int    // index of the frame in buf, -1 when empty
	buf   []byte // the decompressed frame
}

func newBlockStreams(dir blockDir, comp *compress.Compressor) blockStreams {
	return blockStreams{dir: dir, comp: comp, frame: -1}
}

// granule returns granule g's codec stream. The result aliases the cached frame buffer and stays
// valid until the next call that crosses a frame boundary.
func (s *blockStreams) granule(g int) ([]byte, error) {
	f := s.dir.frameOf(g)
	if f != s.frame {
		buf, err := s.comp.Decompress(s.buf[:0], s.dir.frame(f))
		if err != nil {
			return nil, errors.Wrapf(err, "decompress frame %d", f)
		}

		s.buf, s.frame = buf, f
	}

	return s.dir.granuleStream(g, s.buf)
}

// decodeBlockedColumn decodes every block of a blocked column into dst (sized to rows) in place: each
// block decodes directly into its row span of dst, so the whole-column path adds no per-row copy over
// the single-stream path. dec is the per-block typed decoder (DecodeTimestamps, DecodeFloats, …).
func decodeBlockedColumn[T any](
	dir blockDir, comp *compress.Compressor, rows int, dst []T, dec func([]T, []byte) ([]T, int, error),
) ([]T, error) {
	out := dst[:0]
	if cap(out) < rows {
		out = make([]T, 0, rows)
	}

	out = out[:rows]

	base := 0
	streams := newBlockStreams(dir, comp)

	for i := range dir.nBlocks() {
		stream, err := streams.granule(i)
		if err != nil {
			return nil, err
		}

		// cap(out[base:]) == rows-base ≥ this block's row count (blocks partition [0,rows)), so the
		// decoder fills out[base:base+blkRows] in place without reallocating.
		sub, _, err := dec(out[base:base], stream)
		if err != nil {
			return nil, errors.Wrapf(err, "decode block %d", i)
		}

		base += len(sub)
		if base > rows {
			return nil, errors.Wrapf(ErrCorrupt, "block %d overran row count %d", i, rows)
		}
	}

	return out[:base], nil
}

// decodeBlocksInto decodes the given block indices into their row spans of out (sized to rows),
// leaving rows outside those blocks untouched. It is the engine's series-skip primitive: decode only
// the blocks a query's matched series touch.
func decodeBlocksInto[T any](
	dir blockDir, comp *compress.Compressor, rows int, out []T, blocks []int, dec func([]T, []byte) ([]T, int, error),
) error {
	if dec == nil {
		return errors.New("block: nil decoder")
	}

	streams := newBlockStreams(dir, comp)

	for _, b := range blocks {
		if b < 0 || b >= dir.nBlocks() {
			return errors.Errorf("block: block %d out of range [0,%d)", b, dir.nBlocks())
		}

		lo := b * dir.blockRows
		// A corrupt/mismatched directory can place a block past the destination's row count; guard
		// before slicing out[lo:] so a bad object errors rather than panicking.
		if lo >= rows {
			return errors.Wrapf(ErrCorrupt, "block %d start %d past rows %d", b, lo, rows)
		}

		hi := min(lo+dir.blockRows, rows)

		stream, err := streams.granule(b)
		if err != nil {
			return err
		}

		// cap(out[lo:]) == rows-lo ≥ this block's row count, so the decoder fills out[lo:hi] in place.
		sub, _, err := dec(out[lo:lo], stream)
		if err != nil {
			return errors.Wrapf(err, "decode block %d", b)
		}

		if len(sub) != hi-lo {
			return errors.Wrapf(ErrCorrupt, "block %d decoded %d rows, want %d", b, len(sub), hi-lo)
		}
	}

	return nil
}

// decodeBlockedRange decodes only the blocks spanning rows [lo,hi) and returns that row range. It is
// the seek primitive: a query touching a fraction of a column decodes a fraction of its blocks. The
// result is a view into a buffer reusing dst's capacity; lo/hi must satisfy 0 ≤ lo < hi ≤ rows.
func decodeBlockedRange[T any](
	dir blockDir, comp *compress.Compressor, lo, hi int, dst []T, dec func([]T, []byte) ([]T, int, error),
) ([]T, error) {
	if lo < 0 || hi <= lo {
		return nil, errors.Errorf("block: bad range [%d,%d)", lo, hi)
	}

	first := lo / dir.blockRows
	last := (hi - 1) / dir.blockRows

	if last >= dir.nBlocks() {
		return nil, errors.Wrapf(ErrCorrupt, "range [%d,%d) past blocks", lo, hi)
	}

	out := dst[:0]
	streams := newBlockStreams(dir, comp)

	var scratch []T

	for i := first; i <= last; i++ {
		stream, err := streams.granule(i)
		if err != nil {
			return nil, err
		}

		scratch, _, err = dec(scratch, stream)
		if err != nil {
			return nil, errors.Wrapf(err, "decode block %d", i)
		}

		out = append(out, scratch...)
	}

	// out now holds rows [first*blockRows, …); slice out the requested window within it.
	relLo := lo - first*dir.blockRows

	relHi := hi - first*dir.blockRows
	if relHi > len(out) {
		return nil, errors.Wrapf(ErrCorrupt, "range [%d,%d) past decoded rows", lo, hi)
	}

	return out[relLo:relHi], nil
}

// Decoder decodes individual blocks of a blocked column, parsing the directory once so a caller
// that decodes several blocks (e.g. a per-block cache filling its misses) does not re-parse it per
// block. Obtain one via [ColumnReader.Decoder]; it holds the column's already-read object.
type Decoder struct {
	rows int
	i64  func([]int64, []byte) ([]int64, int, error)
	f64  func([]float64, []byte) ([]float64, int, error)

	// streams holds the decompressed compression frame, reused across this decoder's blocks. A
	// decoder decodes its column's blocks serially (never concurrently), and each block's decoded
	// output goes to a separate destination, so the frame buffer is live only during one block's
	// decode — turning a per-block decompress into one per frame.
	streams blockStreams
}

// NumBlocks returns the column's block count.
func (d *Decoder) NumBlocks() int { return d.streams.dir.nBlocks() }

// BlockRows returns the column's nominal block size in rows.
func (d *Decoder) BlockRows() int { return d.streams.dir.blockRows }

// BlockSpan returns block blk's half-open row range [lo, hi) in the column.
func (d *Decoder) BlockSpan(blk int) (lo, hi int) {
	lo = blk * d.streams.dir.blockRows

	return lo, min(lo+d.streams.dir.blockRows, d.rows)
}

// DecodeInt64 decodes block blk into a fresh slice (for an int64 column).
func (d *Decoder) DecodeInt64(blk int) ([]int64, error) {
	return decodeOneBlockInto(&d.streams, blk, nil, d.i64)
}

// DecodeInt64Into decodes block blk into dst (for an int64 column), reusing dst's backing array when
// it has room for the block's rows — so a caller drawing dst from a pool decodes without allocating
// the output. Passing a nil dst is equivalent to [Decoder.DecodeInt64].
func (d *Decoder) DecodeInt64Into(blk int, dst []int64) ([]int64, error) {
	return decodeOneBlockInto(&d.streams, blk, dst, d.i64)
}

// DecodeFloat64 decodes block blk into a fresh slice (for a float64 column).
func (d *Decoder) DecodeFloat64(blk int) ([]float64, error) {
	return decodeOneBlockInto(&d.streams, blk, nil, d.f64)
}

// DecodeFloat64Into is the float64 analog of [Decoder.DecodeInt64Into].
func (d *Decoder) DecodeFloat64Into(blk int, dst []float64) ([]float64, error) {
	return decodeOneBlockInto(&d.streams, blk, dst, d.f64)
}

// decodeOneBlockInto decompresses and decodes a single block into dst (reusing dst's backing array
// when its capacity allows), taking its stream from s (whose frame buffer is retained across calls).
// A nil dst decodes into a fresh slice.
func decodeOneBlockInto[T any](
	s *blockStreams, blk int, dst []T, dec func([]T, []byte) ([]T, int, error),
) ([]T, error) {
	if dec == nil {
		return nil, errors.New("block: nil decoder")
	}

	if blk < 0 || blk >= s.dir.nBlocks() {
		return nil, errors.Errorf("block: block %d out of range [0,%d)", blk, s.dir.nBlocks())
	}

	stream, err := s.granule(blk)
	if err != nil {
		return nil, err
	}

	out, _, err := dec(dst[:0], stream)

	return out, err
}

// blockedTsCursor is a forward [chunk.TsCursor] over a blocked int64 column: it decodes one block at
// a time, opening the next block when the current is exhausted, so it spans block boundaries
// transparently. Each block is an independent codec stream (its row 0 is absolute), so crossing a
// boundary just starts a fresh per-block decoder — no cross-block state.
type blockedTsCursor struct {
	streams blockStreams
	rows    int
	pos     int
	blk     int            // index of the open block; -1 before the first
	cur     chunk.TsCursor // decoder for block blk; advanced past its end opens the next
}

func newBlockedTsCursor(dir blockDir, comp *compress.Compressor, rows int) *blockedTsCursor {
	return &blockedTsCursor{streams: newBlockStreams(dir, comp), rows: rows, blk: -1}
}

func (c *blockedTsCursor) Len() int { return c.rows }
func (c *blockedTsCursor) Pos() int { return c.pos }

func (c *blockedTsCursor) Next() (int64, error) {
	for c.cur == nil || c.cur.Pos() >= c.cur.Len() {
		c.blk++
		if c.blk >= c.streams.dir.nBlocks() {
			return 0, errCursorEOF
		}

		stream, err := c.streams.granule(c.blk)
		if err != nil {
			return 0, err
		}

		c.cur, err = chunk.NewTsDecoder(stream)
		if err != nil {
			return 0, errors.Wrapf(err, "block %d", c.blk)
		}
	}

	v, err := c.cur.Next()
	if err != nil {
		return 0, err
	}

	c.pos++

	return v, nil
}

// blockedFloatCursor is the float64 analog of [blockedTsCursor], over a blocked Gorilla/decimal
// column.
type blockedFloatCursor struct {
	streams blockStreams
	codec   chunk.Codec
	rows    int
	pos     int
	blk     int
	cur     chunk.FloatDecoder
}

func newBlockedFloatCursor(dir blockDir, comp *compress.Compressor, codec chunk.Codec, rows int) *blockedFloatCursor {
	return &blockedFloatCursor{streams: newBlockStreams(dir, comp), codec: codec, rows: rows, blk: -1}
}

func (c *blockedFloatCursor) Len() int { return c.rows }
func (c *blockedFloatCursor) Pos() int { return c.pos }

func (c *blockedFloatCursor) Next() (float64, error) {
	for c.cur == nil || c.cur.Pos() >= c.cur.Len() {
		c.blk++
		if c.blk >= c.streams.dir.nBlocks() {
			return 0, errCursorEOF
		}

		stream, err := c.streams.granule(c.blk)
		if err != nil {
			return 0, err
		}

		c.cur, err = chunk.NewFloatDecoder(c.codec, stream)
		if err != nil {
			return 0, errors.Wrapf(err, "block %d", c.blk)
		}
	}

	v, err := c.cur.Next()
	if err != nil {
		return 0, err
	}

	c.pos++

	return v, nil
}
