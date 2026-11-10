package seriesindex

import (
	"bytes"
	"context"
	"encoding/binary"
	"slices"

	"github.com/go-faster/errors"

	"github.com/oteldb/storage/internal/buildbudget"
	"github.com/oteldb/storage/internal/memsize"
	"github.com/oteldb/storage/signal"
)

type referencePair struct{ source, output uint32 }

type mergePostingSource struct {
	index      *Index
	pairs      []referencePair
	dense      []uint32
	denseStart uint32
}

// PostingMerge remaps immutable source postings into one output's canonical reference order.
// Callers append references in source order and include Bytes in output admission.
type PostingMerge struct {
	ctx            context.Context //nolint:containedctx // The merge owns its scoped source I/O lifetime.
	sources        []mergePostingSource
	count          uint64
	postingRecords int64
	names          []string
	nameBytes      int64
	selectedSource int
	outputCount    int
}

// NewPostingMerge retains index handles; the enclosing merge must keep their parts pinned.
func NewPostingMerge(ctx context.Context, indexes []*Index) *PostingMerge {
	m := &PostingMerge{ctx: ctx, sources: make([]mergePostingSource, len(indexes)), selectedSource: -1}
	for i, index := range indexes {
		m.sources[i].index = index
		m.count += uint64(index.count)
		m.postingRecords = buildbudget.Add(m.postingRecords, int64(index.table.count)-2*int64(index.count)-1)
	}
	return m
}

// Append maps a contributing source reference to the output's distinct-series reference.
func (m *PostingMerge) Append(source int, ref, output uint32) error {
	if source < 0 || source >= len(m.sources) || ref >= m.sources[source].index.count {
		return errors.Wrap(ErrCorrupt, "merge reference bounds")
	}
	s := &m.sources[source]
	if n := len(s.pairs); n > 0 && (s.pairs[n-1].source >= ref || s.pairs[n-1].output >= output) {
		return errors.New("seriesindex: merge references are not ordered")
	}
	s.pairs = append(s.pairs, referencePair{source: ref, output: output})
	return nil
}

// Bytes bounds mappings, source pages, copied grouping keys and posting-chunk scratch.
func (m *PostingMerge) Bytes() int64 {
	if m == nil {
		return 0
	}
	n := int64(cap(m.sources))*memsize.Of[mergePostingSource]() + 64<<10 + int64(cap(m.names))*16 + m.nameBytes
	for _, s := range m.sources {
		n = buildbudget.Add(n, int64(cap(s.pairs))*memsize.Of[referencePair]())
		n = buildbudget.Add(n, 2*s.index.MaxPageBytes()+int64(postingChunk)*16)
		n = buildbudget.Add(n, 4*int64(max(cap(s.dense), s.denseLength())))
	}
	return n
}

func (m *PostingMerge) selectCovering(count int) {
	m.outputCount = count
	m.selectedSource = -1
	for i := range m.sources {
		if len(m.sources[i].pairs) != count {
			continue
		}
		if m.selectedSource < 0 || m.sources[i].index.table.count < m.sources[m.selectedSource].index.table.count {
			m.selectedSource = i
		}
	}
}

func (m *PostingMerge) dense() bool {
	if m.selectedSource >= 0 {
		s := &m.sources[m.selectedSource]
		return uint64(len(s.pairs))*4 >= uint64(s.index.count)
	}
	var selected uint64
	for _, s := range m.sources {
		selected += uint64(len(s.pairs))
	}
	// Broad unions benefit from remapping; duplicate-heavy unions can cost more than labels.
	return selected*4 >= m.count && selected <= uint64(m.outputCount)*2
}

func (m *PostingMerge) activeSources() []mergePostingSource {
	if m.selectedSource >= 0 {
		return m.sources[m.selectedSource : m.selectedSource+1]
	}
	return m.sources
}

func (b *Builder) writeMergedPostings(w *TableWriter, prefix byte) error {
	m := b.mergedPostings
	sources := m.activeSources()
	if prefix == 'h' {
		m.names = m.names[:0]
		m.nameBytes = 0
		for i := range sources {
			sources[i].prepareDense()
		}
	}
	cursors := make([]mergePostingCursor, len(sources))
	for i := range cursors {
		cursors[i].source = &sources[i]
		cursors[i].cursor = sources[i].index.table.Cursor(m.ctx)
		if err := cursors[i].start(prefix); err != nil {
			return err
		}
	}
	writer := mergedPostingWriter{sources: cursors, writer: postingsWriter{table: w}, refs: make([]uint32, 0, postingChunk)}
	nameCount := 0
	for first := minimumPostingKey(cursors); first >= 0; first = minimumPostingKey(cursors) {
		writer.group = append(writer.group[:0], cursors[first].key...)
		if prefix == 'n' {
			kept, err := writer.writeName(m)
			if err != nil {
				return err
			}
			if kept {
				nameCount++
			}
			continue
		}
		if prefix == 'h' {
			m.rememberName(writer.group)
		}
		if err := writer.writeReferences(); err != nil {
			return err
		}
	}
	if prefix == 'n' && nameCount != len(m.names) {
		return errors.Wrap(ErrCorrupt, "merge label name missing")
	}
	return nil
}

func (m *PostingMerge) rememberName(group []byte) {
	group[0] = 'n'
	m.names = append(m.names, string(group))
	m.nameBytes += int64(len(group))
	group[0] = 'h'
}

func minimumPostingKey(cursors []mergePostingCursor) int {
	first := -1
	for i := range cursors {
		if cursors[i].valid && (first < 0 || bytes.Compare(cursors[i].key, cursors[first].key) < 0) {
			first = i
		}
	}
	return first
}

type mergedPostingWriter struct {
	sources []mergePostingCursor
	writer  postingsWriter
	refs    []uint32
	group   []byte
}

func (w *mergedPostingWriter) writeName(m *PostingMerge) (bool, error) {
	flag := byte(0)
	for i := range w.sources {
		c := &w.sources[i]
		if c.valid && bytes.Equal(c.key, w.group) {
			flag |= c.flag
			if err := c.advance(); err != nil {
				return false, err
			}
		}
	}
	_, keep := slices.BinarySearch(m.names, string(w.group))
	if !keep {
		return false, nil
	}
	return true, w.writer.table.Append(w.group, []byte{flag})
}

func (w *mergedPostingWriter) nextReference() int {
	next := -1
	for i := range w.sources {
		c := &w.sources[i]
		if c.valid && bytes.Equal(c.key, w.group) && (next < 0 || c.ref < w.sources[next].ref) {
			next = i
		}
	}
	return next
}

func (w *mergedPostingWriter) writeReferences() error {
	w.refs = w.refs[:0]
	var last uint32
	haveLast := false
	for next := w.nextReference(); next >= 0; next = w.nextReference() {
		ref := w.sources[next].ref
		if err := w.sources[next].advance(); err != nil {
			return err
		}
		if haveLast && ref == last {
			continue
		}
		last, haveLast = ref, true
		w.refs = append(w.refs, ref)
		if len(w.refs) == postingChunk {
			if err := w.writer.write(w.group, w.refs); err != nil {
				return err
			}
			w.refs = w.refs[:0]
		}
	}
	return w.writer.write(w.group, w.refs)
}

type mergePostingCursor struct {
	source                 *mergePostingSource
	cursor                 *Cursor
	prefix                 byte
	key, priorKey, encoded []byte
	refs                   [postingChunk]uint32
	at, n                  int
	ref                    uint32
	flag                   byte
	valid                  bool
	lastSource             uint32
	haveSource             bool
}

func (c *mergePostingCursor) start(prefix byte) error {
	c.prefix = prefix
	ok := c.cursor.Seek([]byte{prefix})
	return c.load(ok)
}

func (c *mergePostingCursor) advance() error {
	if c.prefix != 'n' && c.at+1 < c.n {
		c.at++
		c.ref = c.refs[c.at]
		return nil
	}
	return c.load(c.cursor.Next())
}

func (c *mergePostingCursor) load(ok bool) error {
	c.valid = false
	for ok && len(c.cursor.Key()) > 0 && c.cursor.Key()[0] == c.prefix {
		key := c.cursor.Key()
		if c.prefix == 'n' {
			if err := c.checkKey(key, false); err != nil {
				return err
			}
			v := c.cursor.Value()
			if len(v) != 1 || v[0] > 1 {
				return errors.Wrap(ErrCorrupt, "merge label kind flags")
			}
			c.key = append(c.key[:0], key...)
			c.flag, c.valid = v[0], true
			return nil
		}
		if err := c.checkKey(key, true); err != nil {
			return err
		}
		group := key[:len(key)-4]
		if !bytes.Equal(c.priorKey, group) {
			c.haveSource = false
			c.priorKey = append(c.priorKey[:0], group...)
		}
		c.key = append(c.key[:0], group...)
		if err := c.remap(c.cursor.Value(), binary.BigEndian.Uint32(key[len(key)-4:])); err != nil {
			return err
		}
		if c.n > 0 {
			c.at, c.ref, c.valid = 0, c.refs[0], true
			return nil
		}
		ok = c.cursor.Next()
	}
	return c.cursor.Err()
}

func (c *mergePostingCursor) checkKey(key []byte, posting bool) error {
	var n int
	var err error
	c.encoded, n, err = decodeEscaped(c.encoded[:0], key[1:])
	if err != nil {
		return err
	}
	at := 1 + n
	if c.prefix == 'p' {
		c.encoded, n, err = decodeEscaped(c.encoded[:0], key[at:])
		if err != nil {
			return err
		}
		_, used, err := signal.DecodeValue(c.encoded)
		if err != nil || used != len(c.encoded) {
			return errors.Wrap(ErrCorrupt, "merge typed label")
		}
		at += n
	}
	if posting {
		at += 4
	}
	if at != len(key) {
		return errors.Wrap(ErrCorrupt, "merge posting key")
	}
	return nil
}

func (c *mergePostingCursor) remap(data []byte, last uint32) error {
	n, size := binary.Uvarint(data)
	if size <= 0 || n == 0 || n > postingChunk {
		return errors.Wrap(ErrCorrupt, "merge postings count")
	}
	data, c.n = data[size:], 0
	ref := uint64(0)
	for i := range n {
		delta, k := binary.Uvarint(data)
		if k <= 0 || (i > 0 && delta == 0) || delta > uint64(c.source.index.count) || ref+delta >= uint64(c.source.index.count) {
			return errors.Wrap(ErrCorrupt, "merge postings reference")
		}
		ref += delta
		data = data[k:]
		if c.haveSource && ref <= uint64(c.lastSource) {
			return errors.Wrap(ErrCorrupt, "merge posting chunks overlap")
		}
		c.lastSource, c.haveSource = uint32(ref), true
		if output, found := c.source.lookup(uint32(ref)); found {
			c.refs[c.n] = output
			c.n++
		}
	}
	if len(data) != 0 || ref != uint64(last) {
		return errors.Wrap(ErrCorrupt, "merge postings last reference")
	}
	return nil
}

func (s *mergePostingSource) denseLength() int {
	if len(s.pairs) == 0 {
		return 0
	}
	n := uint64(s.pairs[len(s.pairs)-1].source) - uint64(s.pairs[0].source) + 1
	if n > uint64(len(s.pairs))*4 || n > uint64(int(^uint(0)>>1)) {
		return 0
	}
	return int(n)
}

func (s *mergePostingSource) prepareDense() {
	n := s.denseLength()
	if n == 0 {
		s.dense = s.dense[:0]
		return
	}
	if cap(s.dense) < n {
		s.dense = make([]uint32, n)
	} else {
		s.dense = s.dense[:n]
		clear(s.dense)
	}
	s.denseStart = s.pairs[0].source
	for _, p := range s.pairs {
		s.dense[p.source-s.denseStart] = p.output + 1
	}
}

func (s *mergePostingSource) lookup(ref uint32) (uint32, bool) {
	if len(s.dense) > 0 {
		if ref < s.denseStart || uint64(ref-s.denseStart) >= uint64(len(s.dense)) {
			return 0, false
		}
		x := s.dense[ref-s.denseStart]
		return x - 1, x != 0
	}
	pos, found := slices.BinarySearchFunc(s.pairs, ref, func(p referencePair, ref uint32) int {
		if p.source < ref {
			return -1
		}
		if p.source > ref {
			return 1
		}
		return 0
	})
	if !found {
		return 0, false
	}
	return s.pairs[pos].output, true
}
