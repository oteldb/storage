package block

import (
	"context"
	"slices"
	"strconv"
	"sync"

	"github.com/go-faster/errors"

	"github.com/oteldb/storage/backend"
	"github.com/oteldb/storage/encoding/chunk"
	"github.com/oteldb/storage/encoding/compress"
)

// defaultGranuleSize is the sparse-index granularity in rows (ClickHouse default;
// _ref/docs/storage-engine.md §2). Overridable via [WithGranuleSize].
const defaultGranuleSize = 8192

// A part is stored as a set of backend objects under one key prefix (DESIGN.md §14 M1):
//
//	{prefix}/manifest   the schema + stats, written LAST (the commit point)
//	{prefix}/marks      the sparse granule index
//	{prefix}/c/{i}      column i's stream (absent for a constant-collapsed column)
func manifestKey(prefix string) string { return prefix + "/manifest" }
func marksKey(prefix string) string    { return prefix + "/marks" }
func columnKey(prefix string, i int) string {
	return prefix + "/c/" + strconv.Itoa(i)
}

// partConfig is the layout configuration a [PartOption] sets. It is shared by the batch
// [PartWriter] and the incremental [StreamWriter] so both accept the same options and lay a part
// out identically.
type partConfig struct {
	sortKey       string
	granuleSize   int
	compressBytes int
	defaultComp   compress.Algorithm
	level         compress.Level
	dictCap       int64
	sizing        bool
}

func newPartConfig(opts []PartOption) partConfig {
	c := partConfig{
		granuleSize:   defaultGranuleSize,
		compressBytes: defaultCompressBlockBytes,
		level:         compress.LevelDefault,
		dictCap:       defaultSharedDictBytes,
	}
	for _, opt := range opts {
		opt(&c)
	}

	return c
}

// PartWriter accumulates columns and serializes them into a part's objects. Columns are
// added in order; their ordinal is their object key. The sort-key column (timestamp for
// metrics) drives the marks index and the manifest time range.
type PartWriter struct {
	partConfig

	columns  []Column
	rows     int
	haveRows bool
	comps    map[compress.Algorithm]*compress.Compressor
}

// PartOption configures a [PartWriter] or a [StreamWriter].
type PartOption func(*partConfig)

// WithGranuleSize sets the sparse-index granularity in rows (default 8192).
func WithGranuleSize(n int) PartOption { return func(c *partConfig) { c.granuleSize = n } }

// WithCompressBlockBytes sets the minimum uncompressed bytes packed into one compression frame of a
// block-framed column (default [defaultCompressBlockBytes]). It decouples the compression unit from
// the decode granule ([WithGranuleSize]): a granule stays the smallest decodable slice, while a frame
// gathers enough consecutive granules to give the compressor real context. Decode-compatible either
// way — the directory records the packing.
func WithCompressBlockBytes(n int) PartOption {
	return func(c *partConfig) { c.compressBytes = n }
}

// WithSortKey names the int64 column that the marks index and time range are built over.
// If unset, the first int64 column is used.
func WithSortKey(name string) PartOption { return func(c *partConfig) { c.sortKey = name } }

// WithCompression sets the default block-compression algorithm for columns that do not
// set [Column.Compress] (default none — the chunk codecs already compress well).
func WithCompression(alg compress.Algorithm) PartOption {
	return func(c *partConfig) { c.defaultComp = alg }
}

// WithCompressionLevel sets the compression level used by the block compressors (default
// [compress.LevelDefault]). It is decode-irrelevant — the reader reconstructs the decompressor
// from the per-column algorithm recorded in the manifest, regardless of the level data was written
// at — so a merge can rewrite cold parts at a higher ratio with no format change.
func WithCompressionLevel(level compress.Level) PartOption {
	return func(c *partConfig) { c.level = level }
}

// WithSharedDictBytes caps the resident size of a bytes column's shared dictionary: its entries'
// bytes plus a fixed per-entry overhead (default 32 MiB). A granule whose new values would pass the
// cap self-encodes instead of joining. n is clamped to [0, 64 MiB], the format's ceiling.
func WithSharedDictBytes(n int64) PartOption {
	return func(c *partConfig) { c.dictCap = min(max(n, 0), maxSharedDictRaw) }
}

// WithSizingStats records [ColumnSizing] for every column with an object, so a merge can bound its
// memory from the manifest alone. A part carrying it needs a manifest version-3 reader.
func WithSizingStats() PartOption { return func(c *partConfig) { c.sizing = true } }

func (c *partConfig) layout() columnLayout {
	return columnLayout{blockRows: c.granuleSize, compressBytes: c.compressBytes, dictCap: c.dictCap, sizing: c.sizing}
}

// NewPartWriter returns a [PartWriter] with the given options applied.
func NewPartWriter(opts ...PartOption) *PartWriter {
	return &PartWriter{
		partConfig: newPartConfig(opts),
		comps:      make(map[compress.Algorithm]*compress.Compressor),
	}
}

// AddColumn appends a column. All columns in a part must have the same row count.
func (w *PartWriter) AddColumn(c Column) error {
	if !c.Kind.valid() {
		return errors.Errorf("block: column %q has invalid kind %d", c.Name, c.Kind)
	}

	codec := c.Codec
	if codec == chunk.CodecNone {
		codec = defaultCodec(c.Kind)
	}

	if err := c.checkObserver(codec); err != nil {
		return err
	}

	n := c.rows()
	if w.haveRows && n != w.rows {
		return errors.Errorf("block: column %q has %d rows, want %d", c.Name, n, w.rows)
	}

	w.rows, w.haveRows = n, true
	w.columns = append(w.columns, c)

	return nil
}

func (w *PartWriter) compressorFor(alg compress.Algorithm) *compress.Compressor {
	c, ok := w.comps[alg]
	if !ok {
		c = compress.NewCompressor(alg, w.level)
		w.comps[alg] = c
	}

	return c
}

// builtPart is the in-memory serialized form of a part: one object per column (nil for
// constant columns), the marks object, and the manifest object.
type builtPart struct {
	objects  [][]byte
	marks    []byte
	manifest []byte
}

func (w *PartWriter) build() (builtPart, error) {
	if len(w.columns) == 0 {
		return builtPart{}, errors.New("block: part has no columns")
	}

	descs := make([]ColumnDesc, len(w.columns))
	objects := make([][]byte, len(w.columns))

	for i := range w.columns {
		c := &w.columns[i]

		alg := c.Compress
		if alg == compress.AlgorithmNone {
			alg = w.defaultComp
		}

		desc, obj, err := buildColumnWith(*c, w.compressorFor(alg), w.layout())
		if err != nil {
			return builtPart{}, errors.Wrapf(err, "column %q", c.Name)
		}

		desc.Bytes = int64(len(obj))
		descs[i] = desc
		objects[i] = obj
	}

	m := Manifest{
		Version:     writerVersion(descs),
		RowCount:    w.rows,
		GranuleSize: w.granuleSize,
		Columns:     descs,
	}

	marks := Marks{GranuleSize: w.granuleSize}
	if idx := w.sortKeyIndex(); idx >= 0 {
		marks = BuildMarks(w.columns[idx].Int64, w.granuleSize)
		m.MinTime, m.MaxTime = descs[idx].MinInt64, descs[idx].MaxInt64
	}

	encodedMarks := marks.Encode(nil)
	m.DiskBytes = objectBytes(objects, encodedMarks)
	m.RawBytes = rawBytes(w.columns)

	return builtPart{objects: objects, marks: encodedMarks, manifest: m.Encode(nil)}, nil
}

// sortKeyIndex returns the index of the sort-key column: the one named by [WithSortKey],
// or the first int64 column if unnamed, or -1 if there is no int64 column.
func (w *PartWriter) sortKeyIndex() int {
	for i := range w.columns {
		c := &w.columns[i]
		if w.sortKey != "" {
			if c.Name == w.sortKey && c.Kind == KindInt64 {
				return i
			}

			continue
		}

		if c.Kind == KindInt64 {
			return i
		}
	}

	return -1
}

// WritePart serializes the writer's columns and writes the part's objects under prefix
// on b. Column and marks objects are written first; the manifest is written LAST so the
// part only becomes readable once fully committed. The objects are written deferred: the caller
// runs [backend.SyncPrefix] on prefix before anything durable names the part.
func WritePart(ctx context.Context, b backend.Backend, prefix string, w *PartWriter) error {
	built, err := w.build()
	if err != nil {
		return err
	}

	return built.write(ctx, b, prefix)
}

// objectBytes totals the part's column and marks objects; a constant-collapsed column has none.
func objectBytes(objects [][]byte, marks []byte) int64 {
	total := int64(len(marks))
	for _, obj := range objects {
		total += int64(len(obj))
	}

	return total
}

// write stores the part's objects under prefix on b, manifest LAST so the part only becomes
// readable once fully committed.
func (p builtPart) write(ctx context.Context, b backend.Backend, prefix string) error {
	for i, obj := range p.objects {
		if obj == nil {
			continue // constant column: value lives in the manifest
		}

		if err := backend.WriteDeferred(ctx, b, columnKey(prefix, i), obj); err != nil {
			return errors.Wrapf(err, "write column %d", i)
		}
	}

	if err := backend.WriteDeferred(ctx, b, marksKey(prefix), p.marks); err != nil {
		return errors.Wrap(err, "write marks")
	}

	if err := backend.WriteDeferred(ctx, b, manifestKey(prefix), p.manifest); err != nil {
		return errors.Wrap(err, "write manifest")
	}

	return nil
}

// DeletePart removes every object under the part at prefix. The manifest goes first and durably,
// which retires the part in one directory sync: the rest are deleted deferred, and a power cut
// that brings any of them back leaves an orphan with no manifest, which no reader takes for a part
// and the open-time sweep removes.
func DeletePart(ctx context.Context, b backend.Backend, prefix string) error {
	keys, err := b.List(ctx, prefix+"/")
	if err != nil {
		return err
	}

	manifest := manifestKey(prefix)
	if slices.Contains(keys, manifest) {
		if err := b.Delete(ctx, manifest); err != nil {
			return err
		}
	}

	for _, k := range keys {
		if k == manifest {
			continue
		}

		if err := backend.DeleteDeferred(ctx, b, k); err != nil {
			return err
		}
	}

	return nil
}

// PartReader reads a part written by [WritePart]. It loads only the manifest up front;
// columns and marks are read lazily, so a query touches only the objects it references
// (DESIGN.md §7).
type PartReader struct {
	b        backend.Backend
	prefix   string
	manifest Manifest
	byName   map[string]int
	// compsMu guards comps: Column is called concurrently (overlapping fetches prefetching the
	// same part), and the per-algorithm compressor is created lazily on first use.
	compsMu sync.Mutex
	comps   map[compress.Algorithm]*compress.Compressor
	level   compress.Level
}

// PartPresent reports whether the part at prefix still exists, by probing its manifest — the object
// [OpenPart] reads first and the commit point of a part write, so its absence is the part's absence.
// It is the liveness check for an already-open part, which needs no reopening but must still be
// noticed when its objects go away.
func PartPresent(ctx context.Context, b backend.Backend, prefix string) (bool, error) {
	if _, err := backend.SizeOf(ctx, b, manifestKey(prefix)); err != nil {
		if errors.Is(err, backend.ErrNotExist) {
			return false, nil
		}

		return false, errors.Wrap(err, "probe manifest")
	}

	return true, nil
}

// OpenPart reads a part's manifest from b under prefix and returns a reader. It returns
// an error (wrapping [ErrCorrupt] or [backend.ErrNotExist]) if the manifest is absent or
// malformed — an incompletely written part (no manifest) is therefore not readable.
func OpenPart(ctx context.Context, b backend.Backend, prefix string) (*PartReader, error) {
	// Column/marks/manifest objects are decoded, never mutated, so the no-copy read view is safe.
	raw, err := backend.ReadView(ctx, b, manifestKey(prefix))
	if err != nil {
		return nil, errors.Wrap(err, "read manifest")
	}

	m, err := DecodeManifest(raw)
	if err != nil {
		return nil, err
	}

	byName := make(map[string]int, len(m.Columns))
	for i := range m.Columns {
		byName[m.Columns[i].Name] = i
	}

	return &PartReader{
		b:        b,
		prefix:   prefix,
		manifest: m,
		byName:   byName,
		comps:    make(map[compress.Algorithm]*compress.Compressor),
		level:    compress.LevelDefault,
	}, nil
}

// Manifest returns the part's decoded manifest.
func (r *PartReader) Manifest() Manifest { return r.manifest }

// RowCount returns the number of rows in the part.
func (r *PartReader) RowCount() int { return r.manifest.RowCount }

// ColumnNames returns the column names in part order.
func (r *PartReader) ColumnNames() []string {
	names := make([]string, len(r.manifest.Columns))
	for i := range r.manifest.Columns {
		names[i] = r.manifest.Columns[i].Name
	}

	return names
}

// Column returns a lazy reader for the named column. A constant column is synthesized
// from the manifest with no I/O; otherwise its object is read from the backend.
func (r *PartReader) Column(ctx context.Context, name string) (*ColumnReader, error) {
	i, ok := r.byName[name]
	if !ok {
		return nil, errors.Errorf("block: no column %q", name)
	}

	desc := r.manifest.Columns[i]
	comp := r.compressorFor(desc.Compress)

	if desc.Const {
		return r.columnReader(desc, nil, comp), nil
	}

	obj, err := backend.ReadView(ctx, r.b, columnKey(r.prefix, i))
	if err != nil {
		return nil, errors.Wrapf(err, "read column %q", name)
	}

	// Both writers record the exact size, so a mismatch is an object altered or cut short.
	if desc.Bytes > 0 && int64(len(obj)) != desc.Bytes {
		return nil, errors.Wrapf(ErrCorrupt, "column %q: object of %d bytes, the manifest says %d", name, len(obj), desc.Bytes)
	}

	return r.columnReader(desc, obj, comp), nil
}

// ColumnDescByName returns the named column's descriptor from the already-loaded manifest, without
// reading the column object — so a caller can check Const/Blocked/Codec before deciding whether to
// read and decode the column. ok is false for an unknown column.
func (r *PartReader) ColumnDescByName(name string) (ColumnDesc, bool) {
	i, ok := r.byName[name]
	if !ok {
		return ColumnDesc{}, false
	}

	return r.manifest.Columns[i], true
}

// Marks reads and decodes the part's sparse granule index.
func (r *PartReader) Marks(ctx context.Context) (Marks, error) {
	raw, err := backend.ReadView(ctx, r.b, marksKey(r.prefix))
	if err != nil {
		return Marks{}, errors.Wrap(err, "read marks")
	}

	return DecodeMarks(raw)
}

func (r *PartReader) columnReader(desc ColumnDesc, obj []byte, comp *compress.Compressor) *ColumnReader {
	cr := newColumnReader(desc, obj, comp, r.manifest.RowCount)
	cr.limits = columnLimits(desc, r.manifest.RawBytes, r.manifest.RowCount)

	return cr
}

func (r *PartReader) compressorFor(alg compress.Algorithm) *compress.Compressor {
	r.compsMu.Lock()
	defer r.compsMu.Unlock()

	c, ok := r.comps[alg]
	if !ok {
		c = compress.NewCompressor(alg, r.level)
		r.comps[alg] = c
	}

	return c
}
