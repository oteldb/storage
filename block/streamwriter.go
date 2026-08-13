package block

import (
	"context"
	"encoding/binary"
	"math"

	"github.com/go-faster/errors"

	"github.com/oteldb/storage/backend"
	"github.com/oteldb/storage/encoding/chunk"
	"github.com/oteldb/storage/encoding/compress"
)

// StreamWriter builds a part incrementally: the schema is declared up front and each column encodes
// a granule as soon as one fills, so the working set is the encoded part rather than its
// uncompressed rows. See ARCH.md ("Two writers") for what that buys and what it costs.
//
// Only encodings that restart per granule can stream: blocked [KindInt64]/[KindFloat64], and
// [KindInt128], whose RLE codec is fed runs. Output matches [PartWriter]'s byte for byte except for
// an [Column.AutoCodec] column.
type StreamWriter struct {
	partConfig

	comps map[compress.Algorithm]*compress.Compressor
	cols  []*streamColumn

	// omitConstAt is -1 when OmitConstColumn was not called.
	omitConstAt  int
	omitConstVal float64
}

// NewStreamWriter returns a [StreamWriter] with the given options applied. It takes the same
// options as [NewPartWriter] and lays a part out identically.
func NewStreamWriter(opts ...PartOption) *StreamWriter {
	return &StreamWriter{
		partConfig:  newPartConfig(opts),
		comps:       make(map[compress.Algorithm]*compress.Compressor),
		omitConstAt: -1,
	}
}

// OmitConstColumn drops the last declared column from the part entirely if every row appended to it
// turned out to be v.
//
// It covers a column the format leaves *absent* rather than constant — the sampling weight, which a
// reader defaults to 1. A present-but-constant column is not block-framed and would drop readers
// onto the whole-part decode path. Only the last column may be omitted; dropping an earlier one
// would renumber the object keys after it.
func (w *StreamWriter) OmitConstColumn(v float64) error {
	if len(w.cols) == 0 {
		return errors.New("block: OmitConstColumn before any column was declared")
	}

	if c := w.cols[len(w.cols)-1]; c.kind != KindFloat64 {
		return errors.Errorf("block: OmitConstColumn on column %q, which is %s, not %s", c.name, c.kind, KindFloat64)
	}

	w.omitConstAt, w.omitConstVal = len(w.cols)-1, v

	return nil
}

// AddColumn declares a column; only c's schema fields are read, since rows arrive through the
// Append methods. A column's ordinal — the index those methods take — is its object key.
//
// An AutoCodec column streams under both candidate codecs and keeps the denser, so the choice is
// still made over the whole column rather than a prefix. It compares block-framed sizes where
// [PartWriter] compares whole-column ones, so the two can pick differently in a marginal case; both
// are lossless.
func (w *StreamWriter) AddColumn(c Column) error {
	if !c.Kind.valid() {
		return errors.Errorf("block: column %q has invalid kind %d", c.Name, c.Kind)
	}

	codec := c.Codec
	if codec == chunk.CodecNone {
		codec = defaultCodec(c.Kind)
	}

	alg := c.Compress
	if alg == compress.AlgorithmNone {
		alg = w.defaultComp
	}

	sc := &streamColumn{
		name:    c.Name,
		kind:    c.Kind,
		codec:   codec,
		comp:    w.compressorFor(alg),
		blocked: c.Block,
		allSame: true,
	}

	switch c.Kind {
	case KindInt128:
		if c.Block {
			return errors.Errorf("block: column %q: int128 columns are not block-framed", c.Name)
		}

		if codec != chunk.CodecID128 {
			return errors.Errorf("block: column %q: codec %s invalid for kind %s", c.Name, codec, c.Kind)
		}
	case KindInt64, KindFloat64:
		if !c.Block {
			return errors.Errorf(
				"block: column %q: an unblocked %s column cannot be streamed (its codec stream "+
					"cannot resume across appends); use PartWriter", c.Name, c.Kind)
		}

		if c.AutoCodec && c.Kind == KindFloat64 && c.Codec == chunk.CodecNone {
			sc.autoCodec = true
			sc.budget = c.FloatPrecisionBits

			if sc.budget >= decimalPrecisionLossless {
				sc.budget = 0 // ≥64 bits is lossless; normalize to the lossless sentinel
			}

			sc.codec = chunk.CodecGorilla
			sc.alt = newBlockAccum(sc.comp, w.compressBytes)
			sc.altOK = true
		}

		sc.blk = newBlockAccum(sc.comp, w.compressBytes)
	default:
		return errors.Errorf("block: column %q: kind %s is not streamable; use PartWriter", c.Name, c.Kind)
	}

	w.cols = append(w.cols, sc)

	return nil
}

// AppendInt64 appends vals to the i-th column, encoding every granule they complete.
func (w *StreamWriter) AppendInt64(i int, vals []int64) error {
	c, err := w.column(i, KindInt64)
	if err != nil {
		return err
	}

	return c.appendInt64(vals, w.granuleSize)
}

// AppendFloat64 appends vals to the i-th column, encoding every granule they complete.
func (w *StreamWriter) AppendFloat64(i int, vals []float64) error {
	c, err := w.column(i, KindFloat64)
	if err != nil {
		return err
	}

	return c.appendFloat64(vals, w.granuleSize)
}

// AppendU128Run appends count copies of v to the i-th column as one run, which is how an id column
// streams without materializing its rows. A count ≤ 0 is a no-op.
func (w *StreamWriter) AppendU128Run(i int, v chunk.U128, count int) error {
	c, err := w.column(i, KindInt128)
	if err != nil {
		return err
	}

	if count <= 0 {
		return nil
	}

	c.runs = append(c.runs, chunk.U128Run{Value: v, Count: count})
	c.rows += count
	c.raw += int64(count) * 16

	return nil
}

// Rows returns the row count appended to the first declared column, or 0 if none is declared.
func (w *StreamWriter) Rows() int {
	if len(w.cols) == 0 {
		return 0
	}

	return w.cols[0].rows
}

// EncodedBytes returns the compressed bytes accumulated so far, what a caller seals a part on when
// the target is a size on disk.
//
// It is a lower bound: low by at most one unsealed frame per column plus the id column's RLE stream,
// tens of KiB against a cap in the hundreds of MiB.
func (w *StreamWriter) EncodedBytes() int64 {
	var total int64

	for _, c := range w.cols {
		if c.blk == nil {
			continue // an id column has no accumulator; its stream is built at the end
		}

		n := c.blk.bytes
		if c.autoCodec && c.altOK && c.alt.bytes < n {
			n = c.alt.bytes
		}

		total += int64(n)
	}

	return total
}

// build serializes the part. Every column must have received the same number of rows, and the
// writer must not be appended to afterwards.
func (w *StreamWriter) build() (builtPart, error) {
	if len(w.cols) == 0 {
		return builtPart{}, errors.New("block: part has no columns")
	}

	rows := w.cols[0].rows
	for _, c := range w.cols[1:] {
		if c.rows != rows {
			return builtPart{}, errors.Errorf("block: column %q has %d rows, want %d", c.name, c.rows, rows)
		}
	}

	descs := make([]ColumnDesc, len(w.cols))
	objects := make([][]byte, len(w.cols))

	for i, c := range w.cols {
		desc, obj, err := c.finish(w.granuleSize)
		if err != nil {
			return builtPart{}, errors.Wrapf(err, "column %q", c.name)
		}

		descs[i], objects[i] = desc, obj
	}

	// Dropped whole, so the part looks as if the column had never been declared; always the last
	// one, so the rest keep their object keys.
	if i := w.omitConstAt; i == len(descs)-1 && descs[i].Const && descs[i].ConstFloat64 == w.omitConstVal {
		descs, objects = descs[:i], objects[:i]
	}

	if len(descs) == 0 {
		return builtPart{}, errors.New("block: part has no columns")
	}

	m := Manifest{
		Version:     manifestVersion,
		RowCount:    rows,
		GranuleSize: w.granuleSize,
		Columns:     descs,
	}

	marks := Marks{GranuleSize: w.granuleSize}
	if idx := w.sortKeyIndex(); idx >= 0 {
		marks.Granules = w.cols[idx].granules
		m.MinTime, m.MaxTime = descs[idx].MinInt64, descs[idx].MaxInt64
	}

	encodedMarks := marks.Encode(nil)
	m.DiskBytes = objectBytes(objects, encodedMarks)

	// Only the columns the part keeps: an omitted const column contributes no decoded bytes to a
	// reader of this part.
	for _, c := range w.cols[:len(descs)] {
		m.RawBytes += c.raw
	}

	return builtPart{objects: objects, marks: encodedMarks, manifest: m.Encode(nil)}, nil
}

func (w *StreamWriter) compressorFor(alg compress.Algorithm) *compress.Compressor {
	c, ok := w.comps[alg]
	if !ok {
		c = compress.NewCompressor(alg, w.level)
		w.comps[alg] = c
	}

	return c
}

func (w *StreamWriter) column(i int, want Kind) (*streamColumn, error) {
	if i < 0 || i >= len(w.cols) {
		return nil, errors.Errorf("block: column %d out of range [0,%d)", i, len(w.cols))
	}

	c := w.cols[i]
	if c.kind != want {
		return nil, errors.Errorf("block: column %q is %s, not %s", c.name, c.kind, want)
	}

	return c, nil
}

func (w *StreamWriter) sortKeyIndex() int {
	for i, c := range w.cols {
		if w.sortKey != "" {
			if c.name == w.sortKey && c.kind == KindInt64 {
				return i
			}

			continue
		}

		if c.kind == KindInt64 {
			return i
		}
	}

	return -1
}

// WriteStreamPart serializes w and writes the part under prefix on b, in the same order and under
// the same keys as [WritePart] — manifest last, so the part becomes readable only once committed.
func WriteStreamPart(ctx context.Context, b backend.Backend, prefix string, w *StreamWriter) error {
	built, err := w.build()
	if err != nil {
		return err
	}

	return built.write(ctx, b, prefix)
}

// streamColumn accumulates one column: the granule currently being filled, the granules already
// encoded, and the running descriptor stats.
type streamColumn struct {
	name    string
	kind    Kind
	codec   chunk.Codec
	comp    *compress.Compressor
	blocked bool

	rows int
	// raw is the decoded footprint of the rows appended so far, recorded in the manifest because a
	// merge's working set is made of decoded bytes, not encoded ones.
	raw int64
	// encoded is the rows already packed into granules: the FirstRow of the next one.
	encoded int

	// alt holds the granules under the rival scaled-decimal codec for an AutoCodec column, so the
	// denser of the two can be kept without a second pass over the rows. altOK is cleared once the
	// candidate is inadmissible — a granule that fails the lossless round-trip, or a non-finite
	// value under a lossy budget.
	blk       *blockAccum
	alt       *blockAccum
	altOK     bool
	autoCodec bool
	budget    uint8

	stageI64 []int64
	stageF64 []float64

	runs     []chunk.U128Run // KindInt128 only
	granules []Granule       // the marks index, built as granules are encoded

	// Running descriptor stats: the incremental form of fillInt64Stats/fillFloat64Stats.
	minI64, maxI64 int64
	minF64, maxF64 float64
	firstBits      uint64
	allSame        bool
	sawNonFinite   bool
	haveStats      bool

	// Scratch reused across granules, so the round-trip check does not allocate per granule.
	encBuf []byte
	decBuf []float64
}

func (c *streamColumn) appendInt64(vals []int64, granuleSize int) error {
	for _, v := range vals {
		if !c.haveStats {
			c.minI64, c.maxI64, c.haveStats = v, v, true

			continue
		}

		c.minI64, c.maxI64 = min(c.minI64, v), max(c.maxI64, v)
	}

	c.stageI64 = append(c.stageI64, vals...)
	c.rows += len(vals)
	c.raw += int64(len(vals)) * 8

	// Drain by offset and compact once: a batch spanning many granules must not re-copy the staging
	// buffer per granule.
	off := 0
	for len(c.stageI64)-off >= granuleSize {
		if err := c.flushGranuleInt64(c.stageI64[off : off+granuleSize]); err != nil {
			return err
		}

		off += granuleSize
	}

	c.stageI64 = append(c.stageI64[:0], c.stageI64[off:]...)

	return nil
}

func (c *streamColumn) flushGranuleInt64(vals []int64) error {
	// Marks granules share the blocked columns' granule size, so the index entry falls out here
	// rather than needing a second pass over the column.
	lo, hi := vals[0], vals[0]
	for _, v := range vals[1:] {
		lo, hi = min(lo, v), max(hi, v)
	}

	c.granules = append(c.granules, Granule{FirstRow: c.encoded, MinKey: lo, MaxKey: hi})
	c.encoded += len(vals)

	return c.blk.addGranule(func(dst []byte) ([]byte, error) {
		return appendInt64Granule(dst, c.codec, vals)
	})
}

func (c *streamColumn) appendFloat64(vals []float64, granuleSize int) error {
	for _, v := range vals {
		c.trackFloat(v)
	}

	c.stageF64 = append(c.stageF64, vals...)
	c.rows += len(vals)
	c.raw += int64(len(vals)) * 8

	off := 0
	for len(c.stageF64)-off >= granuleSize {
		if err := c.flushGranuleFloat64(c.stageF64[off : off+granuleSize]); err != nil {
			return err
		}

		off += granuleSize
	}

	c.stageF64 = append(c.stageF64[:0], c.stageF64[off:]...)

	return nil
}

// trackFloat folds one value into the running descriptor stats.
func (c *streamColumn) trackFloat(v float64) {
	if math.IsNaN(v) || math.IsInf(v, 0) {
		c.sawNonFinite = true
	}

	bits := math.Float64bits(v)

	if !c.haveStats {
		c.minF64, c.maxF64, c.firstBits, c.haveStats = v, v, bits, true

		return
	}

	if bits != c.firstBits {
		c.allSame = false
	}

	if math.IsNaN(v) {
		return // min/max ignore NaN, matching fillFloat64Stats
	}

	if math.IsNaN(c.minF64) || v < c.minF64 {
		c.minF64 = v
	}

	if math.IsNaN(c.maxF64) || v > c.maxF64 {
		c.maxF64 = v
	}
}

func (c *streamColumn) flushGranuleFloat64(vals []float64) error {
	if err := c.blk.addGranule(func(dst []byte) ([]byte, error) {
		return appendFloat64Granule(dst, c.codec, c.budget, vals)
	}); err != nil {
		return err
	}

	if c.alt == nil || !c.altOK {
		return nil
	}

	if err := c.alt.addGranule(func(dst []byte) ([]byte, error) {
		return appendFloat64Granule(dst, chunk.CodecDecimal, c.budget, vals)
	}); err != nil {
		return err
	}

	// One granule the decimal codec cannot reproduce disqualifies it for the whole column, matching
	// the whole-column round-trip guard.
	if c.budget == 0 && !c.decimalGranuleRoundTrips(vals) {
		c.altOK = false
	}

	return nil
}

// decimalGranuleRoundTrips re-encodes into scratch rather than reading back the granule just
// packed, which may already have been compressed into a sealed frame.
func (c *streamColumn) decimalGranuleRoundTrips(vals []float64) bool {
	var err error
	if c.encBuf, err = appendFloat64Granule(c.encBuf[:0], chunk.CodecDecimal, 0, vals); err != nil {
		return false
	}

	got, _, err := chunk.DecodeFloatsDecimal(c.decBuf[:0], c.encBuf)
	if err != nil || len(got) != len(vals) {
		return false
	}

	c.decBuf = got

	for i := range vals {
		if got[i] != vals[i] {
			return false
		}
	}

	return true
}

// finish encodes the trailing partial granule and returns the column's descriptor and object.
func (c *streamColumn) finish(granuleSize int) (ColumnDesc, []byte, error) {
	desc := ColumnDesc{Name: c.name, Kind: c.kind, Codec: c.codec, Compress: c.comp.Algorithm()}
	if desc.Compress != compress.AlgorithmNone {
		desc.Level = c.comp.Level()
	}

	if c.kind == KindInt128 {
		// No stats and never constant-collapsed: the RLE codec already shrinks a single-id column
		// to a handful of bytes.
		return desc, c.comp.Compress(nil, chunk.EncodeU128Runs(nil, c.runs)), nil
	}

	if len(c.stageI64) > 0 {
		if err := c.flushGranuleInt64(c.stageI64); err != nil {
			return ColumnDesc{}, nil, err
		}

		c.stageI64 = c.stageI64[:0]
	}

	if len(c.stageF64) > 0 {
		if err := c.flushGranuleFloat64(c.stageF64); err != nil {
			return ColumnDesc{}, nil, err
		}

		c.stageF64 = c.stageF64[:0]
	}

	switch c.kind {
	case KindInt64:
		if c.haveStats {
			desc.MinInt64, desc.MaxInt64 = c.minI64, c.maxI64
			if c.minI64 == c.maxI64 {
				desc.Const, desc.ConstInt64 = true, c.minI64
			}
		}
	case KindFloat64:
		c.fillFloatDesc(&desc)
	case KindBytes, KindInt128:
		// Unreachable: AddColumn rejects bytes columns and returns int128 above.
	}

	if desc.Const {
		return desc, nil, nil
	}

	desc.Blocked, desc.Framed = true, true

	acc := c.blk
	if c.autoCodec && c.altOK && c.alt.bytes < c.blk.bytes {
		desc.Codec, acc = chunk.CodecDecimal, c.alt
	}

	if c.autoCodec {
		desc.FloatPrecisionBits = c.budget
	}

	return desc, acc.finish(granuleSize), nil
}

// fillFloatDesc is the incremental form of [fillFloat64Stats], down to its NaN handling: an
// all-same column is its own min and max and collapses to a constant; an all-NaN one falls back to
// the first value, having no meaningful range.
func (c *streamColumn) fillFloatDesc(desc *ColumnDesc) {
	if !c.haveStats {
		return
	}

	first := math.Float64frombits(c.firstBits)

	if c.allSame {
		desc.MinFloat64, desc.MaxFloat64 = first, first
		desc.Const, desc.ConstFloat64 = true, first

		return
	}

	if math.IsNaN(c.minF64) || math.IsNaN(c.maxF64) {
		c.minF64, c.maxF64 = first, first
	}

	desc.MinFloat64, desc.MaxFloat64 = c.minF64, c.maxF64

	// The lossy scaled-decimal codec cannot represent NaN/±Inf.
	if c.budget != 0 && c.sawNonFinite {
		c.altOK = false
	}
}

func appendInt64Granule(dst []byte, codec chunk.Codec, vals []int64) ([]byte, error) {
	switch codec {
	case chunk.CodecDoD:
		return chunk.EncodeTimestamps(dst, vals), nil
	case chunk.CodecT64:
		return chunk.EncodeIntsT64(dst, vals), nil
	default:
		return nil, errors.Errorf("block: codec %s for kind %s is not blockable", codec, KindInt64)
	}
}

func appendFloat64Granule(dst []byte, codec chunk.Codec, budget uint8, vals []float64) ([]byte, error) {
	switch codec {
	case chunk.CodecGorilla:
		return chunk.EncodeFloats(dst, vals), nil
	case chunk.CodecDecimal:
		return chunk.EncodeFloatsDecimal(dst, vals, decimalPrecision(budget)), nil
	default:
		return nil, errors.Errorf("block: codec %s for kind %s is not blockable", codec, KindFloat64)
	}
}

// blockAccum is the streaming form of [encodeBlocked], emitting the identical layout: granules are
// packed into compression frames of at least compressBytes and each frame is compressed as it
// seals, so only the frame being filled is held uncompressed.
type blockAccum struct {
	comp          *compress.Compressor
	compressBytes int

	frames    [][]byte
	frameLens []int // granules per frame
	gLens     []int // per-granule stream length within its frame
	pending   []byte
	inFrame   int
	bytes     int // compressed bytes sealed so far, the size the codec choice compares on
}

func newBlockAccum(comp *compress.Compressor, compressBytes int) *blockAccum {
	if compressBytes <= 0 {
		compressBytes = defaultCompressBlockBytes
	}

	return &blockAccum{comp: comp, compressBytes: compressBytes}
}

// addGranule appends one granule's codec stream, produced by enc onto the pending frame buffer, and
// seals the frame once it holds enough bytes.
func (a *blockAccum) addGranule(enc func([]byte) ([]byte, error)) error {
	before := len(a.pending)

	out, err := enc(a.pending)
	if err != nil {
		return err
	}

	a.pending = out
	a.gLens = append(a.gLens, len(a.pending)-before)
	a.inFrame++

	if len(a.pending) >= a.compressBytes {
		a.seal()
	}

	return nil
}

func (a *blockAccum) seal() {
	if a.inFrame == 0 {
		return
	}

	f := a.comp.Compress(nil, a.pending)

	a.frames = append(a.frames, f)
	a.frameLens = append(a.frameLens, a.inFrame)
	a.bytes += len(f)
	a.pending, a.inFrame = a.pending[:0], 0
}

// finish seals the trailing frame and serializes the directory + data, per the layout documented in
// blockcolumn.go.
func (a *blockAccum) finish(blockRows int) []byte {
	a.seal()

	dir := binary.AppendUvarint(nil, uint64(len(a.gLens)))
	dir = binary.AppendUvarint(dir, uint64(blockRows))
	dir = binary.AppendUvarint(dir, uint64(len(a.frames)))

	for i, f := range a.frames {
		dir = binary.AppendUvarint(dir, uint64(a.frameLens[i]))
		dir = binary.AppendUvarint(dir, uint64(len(f)))
	}

	for _, l := range a.gLens {
		dir = binary.AppendUvarint(dir, uint64(l))
	}

	// Allocated to the exact final size and drained frame by frame. On a merged part's column this
	// buffer is hundreds of MiB: appending into a growing one would transiently hold two copies of
	// it, and keeping the frames alive past their copy a third.
	dst := make([]byte, 0, len(dir)+a.bytes)
	dst = append(dst, dir...)

	for i, f := range a.frames {
		dst = append(dst, f...)
		a.frames[i] = nil
	}

	a.pending = nil

	return dst
}
