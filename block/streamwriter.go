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

// StreamWriter builds a part incrementally. The caller declares the schema up front and then
// appends rows in batches; each column codec-encodes a granule as soon as one fills, so at most
// one granule of raw rows per column is ever resident. A [PartWriter], by contrast, holds every
// row of every column until the part is serialized.
//
// That is what lets a merge write parts much larger than its memory budget: the writer's working
// set is the *encoded* part (compressed frames), not the uncompressed rows. For metric columns
// that is several times smaller on high-entropy gauges and orders of magnitude smaller on
// counter-shaped data, so part granularity stops being pinned to a raw-byte budget.
//
// Only the encodings that can be restarted per granule are streamable, since a granule must be
// decodable without touching an earlier row:
//
//   - [KindInt64] and [KindFloat64] with Block set (the per-row sequential codecs, which reset
//     their running state at row 0 of every Encode call)
//   - [KindInt128], whose RLE codec is fed runs directly and never materializes the rows
//
// An unblocked int64/float64 column is rejected: its single codec stream cannot be resumed across
// append calls, so it inherently needs the whole column. Use [PartWriter] for those.
//
// The part it produces is byte-identical to the one [PartWriter] produces from the same rows,
// except for an [Column.AutoCodec] column — see [StreamWriter.AddColumn].
type StreamWriter struct {
	partConfig

	comps map[compress.Algorithm]*compress.Compressor
	cols  []*streamColumn

	// omitConstAt/omitConstVal implement OmitConstColumn; omitConstAt is -1 when unset.
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

// OmitConstColumn marks the last declared column to be dropped from the part entirely — descriptor
// and object both — if every row appended to it turned out to be v.
//
// It exists for a column the part format leaves *absent* rather than constant, the sampling-weight
// column being the case: a reader defaults a missing weight to 1, and a present-but-constant
// column is not block-framed, which would drop every reader of the part onto the whole-part decode
// path instead of the block-sliced one. A streamed column's schema is fixed before its first row,
// so whether it is all-unit can only be settled when the part is written.
//
// Only the last column may be omitted: dropping any earlier one would renumber the object keys of
// the columns after it.
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

// AddColumn declares a column. Only the schema fields of c are read (Name, Kind, Codec, Compress,
// Block, AutoCodec, FloatPrecisionBits); its typed slices are ignored, since rows arrive through
// the Append methods. Columns are declared in order and their ordinal — the index passed to the
// Append methods — is their object key, matching [PartWriter].
//
// An AutoCodec float column is encoded under both candidate codecs as it streams and the denser
// one is kept at [WriteStreamPart], which costs a second set of compressed frames but keeps
// the choice made over the whole column rather than a prefix. The winner is picked on the
// block-framed sizes actually written, whereas [PartWriter] picks on whole-column single-stream
// sizes, so in a marginal case the two can choose differently. Both choices are lossless (the
// same round-trip guard applies), so the part still decodes to the same values.
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

// AppendU128Run appends count copies of v to the i-th column as a single run. The RLE codec stores
// the run as-is, so a series' whole row range costs one entry regardless of its sample count — the
// reason an id column can stream without ever materializing its rows. A count ≤ 0 is a no-op.
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

	return nil
}

// Rows returns the row count appended to the first declared column, or 0 if no column has been
// declared. It is what a caller polls to decide when a part is large enough to finish.
func (w *StreamWriter) Rows() int {
	if len(w.cols) == 0 {
		return 0
	}

	return w.cols[0].rows
}

// build serializes the part: it encodes each column's trailing partial granule, seals the
// compression frames, and builds the marks index and manifest. Every column must have received
// the same number of rows. The writer must not be appended to afterwards.
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

	// A column the caller asked to omit when constant is dropped whole, so the part looks exactly
	// as if it had never been declared (see OmitConstColumn). It is always the last one, so the
	// remaining columns keep their object keys.
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

	return builtPart{objects: objects, marks: marks.Encode(nil), manifest: m.Encode(nil)}, nil
}

func (w *StreamWriter) compressorFor(alg compress.Algorithm) *compress.Compressor {
	c, ok := w.comps[alg]
	if !ok {
		c = compress.NewCompressor(alg, w.level)
		w.comps[alg] = c
	}

	return c
}

// column returns the i-th declared column, or an error if i is out of range or its kind does not
// match want.
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

// sortKeyIndex mirrors [PartWriter.sortKeyIndex] over the declared stream columns.
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

// WriteStreamPart serializes w and writes the part's objects under prefix on b, in the same order
// and under the same keys as [WritePart] — the manifest last, so the part only becomes readable
// once fully committed.
func WriteStreamPart(ctx context.Context, b backend.Backend, prefix string, w *StreamWriter) error {
	built, err := w.build()
	if err != nil {
		return err
	}

	return built.write(ctx, b, prefix)
}

// streamColumn accumulates one column of a streamed part: the raw rows of the granule currently
// being filled, the encoded granules, and the running descriptor stats.
type streamColumn struct {
	name    string
	kind    Kind
	codec   chunk.Codec
	comp    *compress.Compressor
	blocked bool

	rows int
	// encoded is the rows already packed into granules; it is the FirstRow of the next one.
	encoded int

	// blk holds the encoded granules under codec; alt holds them under the rival scaled-decimal
	// codec for an AutoCodec column (nil otherwise), so the denser of the two can be kept at
	// Finish without a second pass over the rows.
	blk *blockAccum
	alt *blockAccum
	// altOK tracks whether the decimal candidate is still admissible: it is cleared by a granule
	// that fails the lossless round-trip (budget 0) or by a non-finite value (lossy budget), which
	// the scaled-decimal codec cannot represent.
	altOK     bool
	autoCodec bool
	budget    uint8

	stageI64 []int64
	stageF64 []float64

	// runs is the id column's run-length form (KindInt128 only).
	runs []chunk.U128Run

	// granules is the marks index, built as the sort-key column's granules are encoded.
	granules []Granule

	// Running descriptor stats, the incremental form of fillInt64Stats/fillFloat64Stats.
	minI64, maxI64 int64
	minF64, maxF64 float64
	firstBits      uint64
	allSame        bool
	sawNonFinite   bool
	haveStats      bool

	// scratch buffers reused across granules so encoding a granule does not allocate per call.
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

	// Drain by offset and compact once, so a batch spanning many granules does not re-copy the
	// staging buffer per granule.
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
	// The marks granules share the blocked columns' granule size, so the sort-key column's index
	// entry is computed here rather than by a second pass over the column.
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

// trackFloat folds one value into the running descriptor stats: the all-same test that drives
// constant collapse, the NaN-ignoring min/max, and whether the column can take the lossy
// scaled-decimal codec at all.
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

	// Lossless regime: a granule the decimal codec cannot reproduce exactly disqualifies the
	// candidate for the whole column, exactly as the whole-column round-trip guard does.
	if c.budget == 0 && !c.decimalGranuleRoundTrips(vals) {
		c.altOK = false
	}

	return nil
}

// decimalGranuleRoundTrips reports whether vals survive a scaled-decimal encode/decode unchanged.
// It re-encodes into a scratch buffer rather than reading back the granule the accumulator just
// packed, since that one may already have been compressed into a sealed frame.
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
		// Id columns carry no stats and are never constant-collapsed: the RLE codec already
		// shrinks a single-id column to a handful of bytes.
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

// fillFloatDesc is the incremental form of [fillFloat64Stats]: an all-same column is its own min
// and max (NaN included) and collapses to a constant; otherwise the NaN-ignoring running min/max
// stand, falling back to the first value when every value was NaN.
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

	// The lossy scaled-decimal codec cannot represent NaN/±Inf, so a column carrying one is not a
	// candidate — the whole-column precondition, tracked as the rows streamed past.
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

// blockAccum incrementally builds a block-framed column object: it packs codec-encoded granules
// into compression frames of at least compressBytes and compresses each frame as it seals, so only
// the frame being filled is held uncompressed. It is the streaming form of [encodeBlocked] and
// emits the identical layout.
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

// addGranule appends one granule's codec stream, produced by enc onto the pending frame buffer,
// and seals the frame once it holds enough bytes.
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

// finish seals the trailing frame and serializes the directory + data, matching the layout
// documented in blockcolumn.go.
func (a *blockAccum) finish(blockRows int) []byte {
	a.seal()

	dst := binary.AppendUvarint(nil, uint64(len(a.gLens)))
	dst = binary.AppendUvarint(dst, uint64(blockRows))
	dst = binary.AppendUvarint(dst, uint64(len(a.frames)))

	for i, f := range a.frames {
		dst = binary.AppendUvarint(dst, uint64(a.frameLens[i]))
		dst = binary.AppendUvarint(dst, uint64(len(f)))
	}

	for _, l := range a.gLens {
		dst = binary.AppendUvarint(dst, uint64(l))
	}

	for _, f := range a.frames {
		dst = append(dst, f...)
	}

	return dst
}
