// Package series indexes canonical series identities using compact local references.
package series

import (
	"encoding/binary"
	"math"

	"github.com/oteldb/storage/index/symbols"
	"github.com/oteldb/storage/internal/memsize"
	"github.com/oteldb/storage/signal"
)

// Index maps canonical series IDs to compact identities. Callers own synchronization.
type Index struct {
	byID   map[signal.SeriesID]uint32
	rows   []row
	sym    *symbols.Table
	shared *Contexts

	// Cached empty-context references are offset by one to reserve zero for unknown.
	emptyResource, emptyScope uint64

	resources []signal.Resource
	scopes    []signal.Scope
	contexts  []identityContext
	res       map[string]uint32
	scope     map[string]uint32
	context   map[contextKey]uint32
	keys      [][]symbols.ID
	keysets   map[string]uint32
	values    []symbols.ID

	buf, kbuf, vbuf []byte
	bytes           int64
	templates       *AttributeTemplates
	templateKeys    map[string]uint32
	templateRows    []*attributeTemplate
	templateLast    uint32
	templateMatch   templateMatchPolicy
	templateBuf     []byte
	templateFields  []templateField
	templatePolicy  templatePolicy
}

type row struct {
	id      signal.SeriesID
	offset  uint64
	context uint32
	keys    uint32
}

type contextKey struct{ resource, scope uint32 }

type identityContext struct {
	resource signal.Resource
	scope    signal.Scope
}

// Entry is a materialized series identity and its canonical ID.
type Entry struct {
	ID     signal.SeriesID
	Series signal.Series
}

// New returns an empty index.
func New() *Index {
	return NewWithContexts(nil)
}

// NewWithContexts shares immutable context payloads; callers budget contexts separately.
func NewWithContexts(contexts *Contexts) *Index {
	return NewWithAttributeTemplates(contexts, nil)
}

// NewWithAttributeTemplates shares fixed point attributes and resource/scope payloads independently.
func NewWithAttributeTemplates(contexts *Contexts, templates *AttributeTemplates) *Index {
	return &Index{
		shared:    contexts,
		templates: templates,
		byID:      make(map[signal.SeriesID]uint32),
		sym:       symbols.New(),
		res:       make(map[string]uint32),
		scope:     make(map[string]uint32),
		context:   make(map[contextKey]uint32),
		keysets:   make(map[string]uint32),
		keys:      [][]symbols.ID{nil},
	}
}

// Add registers s once, retaining its own immutable copy without changing its hash.
func (ix *Index) Add(s signal.Series) signal.SeriesID {
	ix.buf = s.AppendHashInput(ix.buf[:0])
	id := signal.HashBytes(ix.buf)
	return ix.AddEntry(Entry{ID: id, Series: s})
}

// AddEntry registers an identity using its already computed canonical ID.
// Callers must provide entry.ID == entry.Series.Hash(); use [Index.AddChecked] for untrusted identities.
func (ix *Index) AddEntry(entry Entry) signal.SeriesID {
	id, s := entry.ID, entry.Series
	if _, ok := ix.byID[id]; ok {
		return id
	}
	if uint64(len(ix.rows)) > math.MaxUint32 {
		panic("series: local reference exhausted")
	}

	c := contextKey{resource: ix.internResource(s.Resource), scope: ix.internScope(s.Scope)}
	context, ok := ix.context[c]
	if !ok {
		context = uint32(len(ix.contexts))
		ix.context[c] = context
		ix.contexts = append(ix.contexts, identityContext{
			resource: ix.resources[c.resource], scope: ix.scopes[c.scope],
		})
		ix.bytes += memsize.MapEntry[contextKey, uint32]()
	}
	keys, offset := ix.storeAttributes(s.Attributes)
	ix.byID[id] = uint32(len(ix.rows))
	ix.rows = append(ix.rows, row{id: id, offset: offset, context: context, keys: keys})
	ix.bytes += memsize.MapEntry[signal.SeriesID, uint32]()
	return id
}

// Ref returns the engine-local ordinal of id. References are scoped to this index.
func (ix *Index) Ref(id signal.SeriesID) (uint32, bool) {
	ref, ok := ix.byID[id]
	return ref, ok
}

// ID returns the canonical ID of a valid local reference.
func (ix *Index) ID(ref uint32) signal.SeriesID { return ix.rows[ref].id }

// Get materializes id's identity. Its interned payloads remain immutable.
func (ix *Index) Get(id signal.SeriesID) (signal.Series, bool) { return ix.GetInto(id, nil) }

// GetInto materializes id, reusing dst for its point attributes.
func (ix *Index) GetInto(id signal.SeriesID, dst signal.Attributes) (signal.Series, bool) {
	ref, ok := ix.byID[id]
	if !ok {
		return signal.Series{}, false
	}
	return ix.View().At(int(ref), dst), true
}

// Has reports whether id is registered.
func (ix *Index) Has(id signal.SeriesID) bool {
	_, ok := ix.byID[id]
	return ok
}

// Len returns the number of distinct identities.
func (ix *Index) Len() int { return len(ix.rows) }

// Snapshot materializes the registration log. Engine maintenance uses [Index.View].
func (ix *Index) Snapshot() []Entry {
	v := ix.View()
	out := make([]Entry, v.Len())
	for i := range out {
		out[i] = Entry{ID: v.ID(i), Series: v.At(i, nil)}
	}
	return out
}

// ForEach visits identities in registration order. The callback may retain s.
func (ix *Index) ForEach(fn func(id signal.SeriesID, s signal.Series)) {
	v := ix.View()
	for i := range v.Len() {
		fn(v.ID(i), v.At(i, nil))
	}
}

// SizeBytes reports the owned resident structures and interned payloads.
func (ix *Index) SizeBytes() int64 {
	return ix.bytes + ix.sym.SizeBytes() + memsize.Slice(ix.rows) + memsize.Slice(ix.values) +
		memsize.Slice(ix.resources) + memsize.Slice(ix.scopes) + memsize.Slice(ix.contexts) +
		memsize.Slice(ix.keys) + memsize.Slice(ix.buf) + memsize.Slice(ix.kbuf) + memsize.Slice(ix.vbuf) +
		memsize.Slice(ix.templateRows) + memsize.Slice(ix.templateBuf) + memsize.Slice(ix.templateFields)
}

func (ix *Index) internResource(r signal.Resource) uint32 {
	empty := len(r.SchemaURL) == 0 && len(r.Attributes) == 0
	if empty && ix.emptyResource != 0 {
		return uint32(ix.emptyResource - 1)
	}
	ix.kbuf = r.AppendHashInput(ix.kbuf[:0])
	if ref, ok := ix.res[string(ix.kbuf)]; ok {
		return ref
	}
	ref := uint32(len(ix.resources))
	var stored signal.Resource
	var shared bool
	if ix.shared == nil {
		stored = r.Intern(ix.sym.Bytes)
	} else {
		stored, shared = ix.shared.resource(ix.kbuf, r, func() signal.Resource { return r.Intern(ix.sym.Bytes) })
	}
	ix.resources = append(ix.resources, stored)
	ix.res[string(ix.kbuf)] = ref
	ix.bytes += memsize.MapEntry[string, uint32]() + int64(len(ix.kbuf))
	if !shared {
		ix.bytes += memsize.Slice(stored.Attributes)
	}
	if empty {
		ix.emptyResource = uint64(ref) + 1
	}
	return ref
}

func (ix *Index) internScope(s signal.Scope) uint32 {
	empty := len(s.Name) == 0 && len(s.Version) == 0 && len(s.SchemaURL) == 0 && len(s.Attributes) == 0
	if empty && ix.emptyScope != 0 {
		return uint32(ix.emptyScope - 1)
	}
	ix.kbuf = s.AppendHashInput(ix.kbuf[:0])
	if ref, ok := ix.scope[string(ix.kbuf)]; ok {
		return ref
	}
	ref := uint32(len(ix.scopes))
	var stored signal.Scope
	var shared bool
	if ix.shared == nil {
		stored = s.Intern(ix.sym.Bytes)
	} else {
		stored, shared = ix.shared.scope(ix.kbuf, s, func() signal.Scope { return s.Intern(ix.sym.Bytes) })
	}
	ix.scopes = append(ix.scopes, stored)
	ix.scope[string(ix.kbuf)] = ref
	ix.bytes += memsize.MapEntry[string, uint32]() + int64(len(ix.kbuf))
	if !shared {
		ix.bytes += memsize.Slice(stored.Attributes)
	}
	if empty {
		ix.emptyScope = uint64(ref) + 1
	}
	return ref
}

func (ix *Index) internKeys(attrs signal.Attributes) uint32 {
	if attrs == nil {
		return 0
	}
	ix.kbuf = ix.kbuf[:0]
	for i := range attrs {
		ix.kbuf = binary.LittleEndian.AppendUint32(ix.kbuf, uint32(ix.sym.Intern(attrs[i].Key)))
	}
	if ref, ok := ix.keysets[string(ix.kbuf)]; ok {
		return ref
	}
	keys := make([]symbols.ID, len(attrs))
	for i := range keys {
		keys[i] = symbols.ID(binary.LittleEndian.Uint32(ix.kbuf[i*4:]))
	}
	if uint64(len(ix.keys)) >= uint64(templateFlag) {
		panic("series: attribute schema reference exhausted")
	}
	ref := uint32(len(ix.keys))
	ix.keys = append(ix.keys, keys)
	ix.keysets[string(ix.kbuf)] = ref
	ix.bytes += memsize.MapEntry[string, uint32]() + int64(len(ix.kbuf)) + memsize.Slice(keys)
	return ref
}
