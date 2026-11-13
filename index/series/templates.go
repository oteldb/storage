package series

import (
	"bytes"
	"encoding/binary"
	"math"
	"sync"
	"sync/atomic"

	"github.com/oteldb/storage/internal/buildbudget"
	"github.com/oteldb/storage/internal/memsize"
	"github.com/oteldb/storage/signal"
)

const templateFlag uint32 = 1 << 31

// AttributeTemplates shares immutable fixed attributes; variable keys retain per-series values.
// Full pools fall back to flat attributes. Callers budget the pool separately from indexes.
type AttributeTemplates struct {
	shards   [64]templateShard
	variable [][]byte
	limit    int64
	bytes    atomic.Int64
	hash     func([]byte) signal.SeriesID
}

type templateShard struct {
	mu        sync.Mutex
	entries   map[string]*attributeTemplate
	probation [128]templateEvidence
	bytes     int64
}

type attributeTemplate struct {
	key       string
	fields    []templateField
	dynamic   uint32
	canonical []byte
	segments  []templateSegment
}

type templateSegment struct{ offset, length uint32 }

type templateField struct {
	scalar                   uint64
	keyOffset, keyLength     uint32
	valueOffset, valueLength uint32
	slot                     uint32
	kind                     signal.ValueKind
}

// NewAttributeTemplates returns a bounded pool; insufficient budgets disable sharing.
func NewAttributeTemplates(maxBytes int64, variableKeys ...[]byte) *AttributeTemplates {
	base := buildbudget.Add(memsize.Of[AttributeTemplates](), int64(len(variableKeys))*24)
	for _, key := range variableKeys {
		base = buildbudget.Add(base, int64(len(key)))
	}
	if maxBytes <= base {
		return nil
	}
	p := &AttributeTemplates{limit: (maxBytes - base) / 64}
	p.variable = make([][]byte, len(variableKeys))
	for i, key := range variableKeys {
		p.variable[i] = bytes.Clone(key)
	}
	p.bytes.Store(base)
	return p
}

// SizeBytes reports conservatively charged pool memory, including lookup-table slack.
func (p *AttributeTemplates) SizeBytes() int64 {
	if p == nil {
		return 0
	}
	return p.bytes.Load()
}

func (p *AttributeTemplates) isVariable(name []byte) bool {
	for _, key := range p.variable {
		if bytes.Equal(key, name) {
			return true
		}
	}
	return false
}

func (p *AttributeTemplates) intern(key []byte, fields []templateField, dynamic uint32) (*attributeTemplate, bool) {
	hash := signal.HashBytes(key)
	if p.hash != nil {
		hash = p.hash(key)
	}
	s := &p.shards[hash.Lo%64]
	s.mu.Lock()
	defer s.mu.Unlock()
	if t := s.entries[string(key)]; t != nil {
		return t, true
	}
	cost := buildbudget.Add(memsize.Of[attributeTemplate](), int64(len(key))*4)
	cost = buildbudget.Add(cost, 2*int64(len(fields))*memsize.Of[templateField]())
	cost = buildbudget.Add(cost, 2*int64(dynamic+1)*memsize.Of[templateSegment]())
	cost = buildbudget.Add(cost, 8*memsize.MapEntry[string, *attributeTemplate]())
	if s.entries == nil {
		cost = buildbudget.Add(cost, memsize.MapBase)
	}
	if cost > p.limit-s.bytes {
		return nil, false
	}
	uses := s.observe(hash)
	if uses < 4 {
		return nil, uses >= 3
	}
	t := &attributeTemplate{
		key: string(key), fields: append([]templateField(nil), fields...), dynamic: dynamic,
		canonical: make([]byte, 0, len(key)), segments: make([]templateSegment, 0, dynamic+1),
	}
	t.buildCanonical(key)
	for i, field := range t.fields {
		if field.slot != math.MaxUint32 {
			continue
		}
		encoded := t.canonical[field.valueOffset : field.valueOffset+field.valueLength]
		t.fields[i].kind = signal.ValueKind(encoded[0])
		switch t.fields[i].kind {
		case signal.KindEmpty, signal.KindMap, signal.KindSlice:
			continue
		case signal.KindBool, signal.KindInt, signal.KindDouble:
			t.fields[i].scalar = binary.BigEndian.Uint64(encoded[1:])
		case signal.KindStr, signal.KindBytes:
			value, _, err := signal.DecodeValue(encoded)
			if err != nil {
				return nil, false
			}
			data := value.Str()
			if value.Kind() == signal.KindBytes {
				data = value.Bytes()
			}
			offset := field.valueOffset + uint32(len(encoded)-len(data))
			t.fields[i].scalar = uint64(offset)<<32 | uint64(uint32(len(data)))
		}
	}
	if s.entries == nil {
		s.entries = make(map[string]*attributeTemplate)
	}
	s.entries[t.key] = t
	s.bytes += cost
	p.bytes.Add(cost)
	return t, true
}

func (t *attributeTemplate) buildCanonical(key []byte) {
	t.canonical = binary.AppendUvarint(t.canonical[:0], uint64(len(t.fields)))
	start := uint32(0)
	for i, f := range t.fields {
		name := key[f.keyOffset : f.keyOffset+f.keyLength]
		t.canonical = binary.AppendUvarint(t.canonical, uint64(len(name)))
		t.fields[i].keyOffset = uint32(len(t.canonical))
		t.canonical = append(t.canonical, name...)
		if f.slot == math.MaxUint32 {
			t.fields[i].valueOffset = uint32(len(t.canonical))
			t.canonical = append(t.canonical, key[f.valueOffset:f.valueOffset+f.valueLength]...)
		} else {
			end := uint32(len(t.canonical))
			t.segments = append(t.segments, templateSegment{offset: start, length: end - start})
			start = end
		}
	}
	t.segments = append(t.segments, templateSegment{offset: start, length: uint32(len(t.canonical)) - start})
}

func (v View) appendTemplateCanonical(dst []byte, r row) []byte {
	t := v.templates[r.keys&^templateFlag]
	for i := range t.dynamic {
		segment := t.segments[i]
		dst = append(dst, t.canonical[segment.offset:segment.offset+segment.length]...)
		dst = append(dst, v.symbols[v.values[int(r.offset)+int(i)]]...)
	}
	last := t.segments[t.dynamic]
	return append(dst, t.canonical[last.offset:last.offset+last.length]...)
}

func (ix *Index) beginTemplate(n int) {
	ix.templateBuf = binary.AppendUvarint(ix.templateBuf[:0], uint64(n))
	ix.templateFields = ix.templateFields[:0]
}

func (ix *Index) templateField(name []byte, variable bool, slot uint32) *templateField {
	ix.templateBuf = binary.AppendUvarint(ix.templateBuf, uint64(len(name)))
	ix.templateFields = append(ix.templateFields, templateField{
		keyOffset: uint32(len(ix.templateBuf)), keyLength: uint32(len(name)), slot: math.MaxUint32,
	})
	f := &ix.templateFields[len(ix.templateFields)-1]
	ix.templateBuf = append(ix.templateBuf, name...)
	if variable {
		f.slot = slot
		ix.templateBuf = append(ix.templateBuf, 1)
	} else {
		ix.templateBuf = append(ix.templateBuf, 0)
		f.valueOffset = uint32(len(ix.templateBuf))
	}
	return f
}

func (ix *Index) finishTemplate(dynamic uint32) (uint32, bool) {
	if uint64(len(ix.templateBuf)) > math.MaxUint32 || len(ix.templateFields)-int(dynamic) < 2 {
		ix.templatePolicy.observe(false)
		return 0, false
	}
	if ref, ok := ix.templateKeys[string(ix.templateBuf)]; ok {
		ix.templatePolicy.observe(true)
		return ref, true
	}
	if uint64(len(ix.templateRows)) >= uint64(templateFlag) {
		return 0, false
	}
	t, recurrent := ix.templates.intern(ix.templateBuf, ix.templateFields, dynamic)
	ix.templatePolicy.observe(recurrent)
	if t == nil {
		return 0, false
	}
	ref := uint32(len(ix.templateRows))
	if ix.templateKeys == nil {
		ix.templateKeys = make(map[string]uint32)
	}
	ix.templateKeys[t.key] = ref
	ix.templateRows = append(ix.templateRows, t)
	ix.bytes += memsize.MapEntry[string, uint32]()
	return ref, true
}

func (ix *Index) storeAttributes(attrs signal.Attributes) (uint32, uint64) {
	if ix.templates != nil {
		if keys, offset, ok := ix.storeTemplateAttributes(attrs); ok {
			return keys, offset
		}
	}
	keys := ix.internKeys(attrs)
	offset := uint64(len(ix.values))
	for _, attr := range attrs {
		ix.vbuf = signal.AppendValue(ix.vbuf[:0], attr.Value)
		ix.values = append(ix.values, ix.sym.Intern(ix.vbuf))
	}
	return keys, offset
}

func (ix *Index) storeTemplateAttributes(attrs signal.Attributes) (uint32, uint64, bool) {
	if !ix.templatePolicy.probe() {
		return 0, 0, false
	}
	ix.beginTemplate(len(attrs))
	var dynamic uint32
	for _, attr := range attrs {
		variable := ix.templates.isVariable(attr.Key)
		f := ix.templateField(attr.Key, variable, dynamic)
		if variable {
			dynamic++
		} else {
			ix.templateBuf = signal.AppendValue(ix.templateBuf, attr.Value)
			f.valueLength = uint32(len(ix.templateBuf)) - f.valueOffset
		}
	}
	ref, ok := ix.finishTemplate(dynamic)
	if !ok {
		return 0, 0, false
	}
	offset := uint64(len(ix.values))
	for i, field := range ix.templateRows[ref].fields {
		if field.slot != math.MaxUint32 {
			ix.vbuf = signal.AppendValue(ix.vbuf[:0], attrs[i].Value)
			ix.values = append(ix.values, ix.sym.Intern(ix.vbuf))
		}
	}
	return ref | templateFlag, offset, true
}

func (v View) encodedField(r row, f templateField, t *attributeTemplate) ([]byte, []byte) {
	name := t.canonical[f.keyOffset : f.keyOffset+f.keyLength]
	if f.slot == math.MaxUint32 {
		return name, t.canonical[f.valueOffset : f.valueOffset+f.valueLength]
	}
	return name, v.symbols[v.values[int(r.offset)+int(f.slot)]]
}

func (ix *Index) copyAttributes(v View, i int) (uint32, uint64) {
	if ix.templates != nil {
		if keys, offset, ok := ix.copyTemplateAttributes(v, i); ok {
			return keys, offset
		}
	}
	ix.kbuf = ix.kbuf[:0]
	v.EncodedAttributes(i, func(name, _ []byte) {
		ix.kbuf = binary.LittleEndian.AppendUint32(ix.kbuf, uint32(ix.sym.Intern(name)))
	})
	keys := uint32(0)
	if v.AttributeCount(i) > 0 {
		keys = ix.internKeyBuffer(v.AttributeCount(i))
	}
	offset := uint64(len(ix.values))
	v.EncodedAttributes(i, func(_, encoded []byte) {
		ix.values = append(ix.values, ix.sym.Intern(encoded))
	})
	return keys, offset
}

func (t *attributeTemplate) encodingWorkspace() int64 {
	n := int64(256)
	for _, f := range t.fields {
		n = buildbudget.Add(n, buildbudget.Add(192, 4*int64(f.keyLength)))
		if f.slot == math.MaxUint32 {
			n = buildbudget.Add(n, buildbudget.Add(192, 4*int64(f.valueLength)))
		}
	}
	return n
}

func (ix *Index) copyTemplateAttributes(v View, i int) (uint32, uint64, bool) {
	if !ix.templatePolicy.probe() {
		return 0, 0, false
	}
	ix.beginTemplate(v.AttributeCount(i))
	var dynamic uint32
	v.EncodedAttributes(i, func(name, encoded []byte) {
		variable := ix.templates.isVariable(name)
		f := ix.templateField(name, variable, dynamic)
		if variable {
			dynamic++
		} else {
			ix.templateBuf = append(ix.templateBuf, encoded...)
			f.valueLength = uint32(len(ix.templateBuf)) - f.valueOffset
		}
	})
	if ref, ok := ix.finishTemplate(dynamic); ok {
		offset := uint64(len(ix.values))
		j := 0
		v.EncodedAttributes(i, func(_, encoded []byte) {
			if ix.templateRows[ref].fields[j].slot != math.MaxUint32 {
				ix.values = append(ix.values, ix.sym.Intern(encoded))
			}
			j++
		})
		return ref | templateFlag, offset, true
	}
	return 0, 0, false
}
