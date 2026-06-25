package block

import (
	"context"
	"strconv"

	"github.com/go-faster/errors"

	"github.com/oteldb/storage/backend"
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

// PartWriter accumulates columns and serializes them into a part's objects. Columns are
// added in order; their ordinal is their object key. The sort-key column (timestamp for
// metrics) drives the marks index and the manifest time range.
type PartWriter struct {
	sortKey     string
	granuleSize int
	defaultComp compress.Algorithm
	level       compress.Level
	columns     []Column
	rows        int
	haveRows    bool
	comps       map[compress.Algorithm]*compress.Compressor
}

// PartOption configures a [PartWriter].
type PartOption func(*PartWriter)

// WithGranuleSize sets the sparse-index granularity in rows (default 8192).
func WithGranuleSize(n int) PartOption { return func(w *PartWriter) { w.granuleSize = n } }

// WithSortKey names the int64 column that the marks index and time range are built over.
// If unset, the first int64 column is used.
func WithSortKey(name string) PartOption { return func(w *PartWriter) { w.sortKey = name } }

// WithCompression sets the default block-compression algorithm for columns that do not
// set [Column.Compress] (default none — the chunk codecs already compress well).
func WithCompression(alg compress.Algorithm) PartOption {
	return func(w *PartWriter) { w.defaultComp = alg }
}

// NewPartWriter returns a [PartWriter] with the given options applied.
func NewPartWriter(opts ...PartOption) *PartWriter {
	w := &PartWriter{
		granuleSize: defaultGranuleSize,
		level:       compress.LevelDefault,
		comps:       make(map[compress.Algorithm]*compress.Compressor),
	}
	for _, opt := range opts {
		opt(w)
	}

	return w
}

// AddColumn appends a column. All columns in a part must have the same row count.
func (w *PartWriter) AddColumn(c Column) error {
	if !c.Kind.valid() {
		return errors.Errorf("block: column %q has invalid kind %d", c.Name, c.Kind)
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

	for i, c := range w.columns {
		alg := c.Compress
		if alg == compress.AlgorithmNone {
			alg = w.defaultComp
		}

		desc, obj, err := buildColumn(c, w.compressorFor(alg))
		if err != nil {
			return builtPart{}, errors.Wrapf(err, "column %q", c.Name)
		}

		descs[i] = desc
		objects[i] = obj
	}

	m := Manifest{
		Version:     manifestVersion,
		RowCount:    w.rows,
		GranuleSize: w.granuleSize,
		Columns:     descs,
	}

	marks := Marks{GranuleSize: w.granuleSize}
	if idx := w.sortKeyIndex(); idx >= 0 {
		marks = BuildMarks(w.columns[idx].Int64, w.granuleSize)
		m.MinTime, m.MaxTime = descs[idx].MinInt64, descs[idx].MaxInt64
	}

	return builtPart{objects: objects, marks: marks.Encode(nil), manifest: m.Encode(nil)}, nil
}

// sortKeyIndex returns the index of the sort-key column: the one named by [WithSortKey],
// or the first int64 column if unnamed, or -1 if there is no int64 column.
func (w *PartWriter) sortKeyIndex() int {
	for i, c := range w.columns {
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
// part only becomes readable once fully committed.
func WritePart(ctx context.Context, b backend.Backend, prefix string, w *PartWriter) error {
	built, err := w.build()
	if err != nil {
		return err
	}

	for i, obj := range built.objects {
		if obj == nil {
			continue // constant column: value lives in the manifest
		}

		if err := b.Write(ctx, columnKey(prefix, i), obj); err != nil {
			return errors.Wrapf(err, "write column %d", i)
		}
	}

	if err := b.Write(ctx, marksKey(prefix), built.marks); err != nil {
		return errors.Wrap(err, "write marks")
	}

	if err := b.Write(ctx, manifestKey(prefix), built.manifest); err != nil {
		return errors.Wrap(err, "write manifest")
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
	comps    map[compress.Algorithm]*compress.Compressor
	level    compress.Level
}

// OpenPart reads a part's manifest from b under prefix and returns a reader. It returns
// an error (wrapping [ErrCorrupt] or [backend.ErrNotExist]) if the manifest is absent or
// malformed — an incompletely written part (no manifest) is therefore not readable.
func OpenPart(ctx context.Context, b backend.Backend, prefix string) (*PartReader, error) {
	raw, err := b.Read(ctx, manifestKey(prefix))
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
		return newColumnReader(desc, nil, comp, r.manifest.RowCount), nil
	}

	obj, err := r.b.Read(ctx, columnKey(r.prefix, i))
	if err != nil {
		return nil, errors.Wrapf(err, "read column %q", name)
	}

	return newColumnReader(desc, obj, comp, r.manifest.RowCount), nil
}

// Marks reads and decodes the part's sparse granule index.
func (r *PartReader) Marks(ctx context.Context) (Marks, error) {
	raw, err := r.b.Read(ctx, marksKey(r.prefix))
	if err != nil {
		return Marks{}, errors.Wrap(err, "read marks")
	}

	return DecodeMarks(raw)
}

func (r *PartReader) compressorFor(alg compress.Algorithm) *compress.Compressor {
	c, ok := r.comps[alg]
	if !ok {
		c = compress.NewCompressor(alg, r.level)
		r.comps[alg] = c
	}

	return c
}
