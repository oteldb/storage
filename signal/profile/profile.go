// Package profile holds the profiles signal's ingest model: the []byte-based, OTLP-shaped batch
// accepted at the storage boundary (in place of OTel-Go pprofile.Profiles), and its projection into
// the columnar sample model the record engine ingests plus the content-addressed symbol store.
//
// A profile is a pprof-style graph: samples reference stacks, stacks reference locations, locations
// reference functions and mappings, and everything bottoms out in an interned string table — all
// index-based, with the tables shared across a whole batch in a [Dictionary] (OTLP's
// ProfilesDictionary). The stream identity of a sample is its producing Resource+Scope; each sample
// flattens to a record row (value, sample-type id, stack id, profile id, trace/span ids, attributes)
// the record engine filters by condition, while the symbol tables ride along as a deduplicated,
// content-addressed side store. See [Project] and the symbol store in symbols.go.
package profile

import (
	"sync"

	"github.com/oteldb/storage/signal"
)

// Profiles is the internal profiles ingest batch — the OTLP Resource→Scope→Profile hierarchy plus
// the shared symbol [Dictionary] the samples index into. All identity and string data is []byte.
// Resettable and pool-friendly (see [GetProfiles]/[PutProfiles]); build it with the Add* helpers.
type Profiles struct {
	Resources  []ResourceProfiles
	Dictionary Dictionary
}

// ResourceProfiles groups the profiles emitted under one [signal.Resource].
type ResourceProfiles struct {
	Resource signal.Resource
	Scopes   []ScopeProfiles
}

// ScopeProfiles groups the profiles emitted under one [signal.Scope]. A (Resource, Scope) pair is
// one profile **stream**.
type ScopeProfiles struct {
	Scope    signal.Scope
	Profiles []Profile
}

// Profile is a single profile: a set of samples sharing one value type (SampleType), collected at
// TimeNanos over DurationNanos. ProfileID is 16 bytes (or nil). AttributeIndices reference the
// dictionary's attribute table. Values/timestamps live per [Sample].
type Profile struct {
	Samples          []Sample
	AttributeIndices []int32
	ProfileID        []byte
	SampleType       ValueType
	PeriodType       ValueType
	TimeNanos        int64
	DurationNanos    int64
	Period           int64
	Dropped          uint32
}

// Sample is one stack occurrence. StackIndex references the dictionary stack table. A sample carries
// either one aggregated Value (Values[0], no timestamps) or paired Values/TimestampsUnixNano arrays
// (one observation each). AttributeIndices/LinkIndex reference the dictionary.
type Sample struct {
	Values             []int64
	TimestampsUnixNano []uint64
	AttributeIndices   []int32
	StackIndex         int32
	LinkIndex          int32
}

// Dictionary is the batch-shared symbol set (OTLP ProfilesDictionary): index-based tables that
// samples/stacks/locations reference. Strings[0] is the "" sentinel. Build it with the Intern/Add
// helpers, which dedup strings and append structured entries.
type Dictionary struct {
	Strings    [][]byte
	Stacks     []Stack
	Locations  []Location
	Functions  []Function
	Mappings   []Mapping
	Attributes []KeyValueAndUnit
	Links      []Link

	strIndex map[string]int32 // interning map for Strings (lazily built)
}

// Stack is a call stack: location indices, leaf first.
type Stack struct {
	LocationIndices []int32
}

// Location is a program location: a mapping, an address, and the (possibly inlined) source lines.
type Location struct {
	Lines            []Line
	AttributeIndices []int32
	Address          uint64
	MappingIndex     int32
}

// Line is one source line at a location: a function plus line/column (for inlined frames there are
// several lines, caller last).
type Line struct {
	FunctionIndex int32
	Line          int64
	Column        int64
}

// Function is a function symbol: name/system-name/filename are string-table indices.
type Function struct {
	NameStrindex       int32
	SystemNameStrindex int32
	FilenameStrindex   int32
	StartLine          int64
}

// Mapping is a binary/library loaded in memory.
type Mapping struct {
	AttributeIndices []int32
	MemoryStart      uint64
	MemoryLimit      uint64
	FileOffset       uint64
	FilenameStrindex int32
}

// ValueType is a (type, unit) pair, both string-table indices (e.g. "cpu"/"nanoseconds").
type ValueType struct {
	TypeStrindex int32
	UnitStrindex int32
}

// KeyValueAndUnit is a dictionary attribute: an interned key, a typed value, and an optional unit.
type KeyValueAndUnit struct {
	Value        signal.Value
	KeyStrindex  int32
	UnitStrindex int32
}

// Link is a profile sample's link to a trace span (16-byte trace id, 8-byte span id).
type Link struct {
	TraceID []byte
	SpanID  []byte
}

// Reset clears the batch for reuse, retaining backing arrays.
func (p *Profiles) Reset() {
	p.Resources = p.Resources[:0]
	p.Dictionary.Reset()
}

// Reset clears the dictionary for reuse, retaining backing arrays and the interning map.
func (d *Dictionary) Reset() {
	d.Strings = d.Strings[:0]
	d.Stacks = d.Stacks[:0]
	d.Locations = d.Locations[:0]
	d.Functions = d.Functions[:0]
	d.Mappings = d.Mappings[:0]
	d.Attributes = d.Attributes[:0]
	d.Links = d.Links[:0]
	clear(d.strIndex)
}

// InternString returns the index of s in the string table, appending it (and the "" sentinel at 0
// on first use) if new. The caller may reuse s after the call.
func (d *Dictionary) InternString(s []byte) int32 {
	if d.strIndex == nil {
		d.strIndex = make(map[string]int32)
	}

	if len(d.Strings) == 0 {
		d.Strings = append(d.Strings, []byte{}) // [0] == "" sentinel
		d.strIndex[""] = 0
	}

	if id, ok := d.strIndex[string(s)]; ok {
		return id
	}

	id := int32(len(d.Strings))
	d.Strings = append(d.Strings, append([]byte(nil), s...))
	d.strIndex[string(s)] = id

	return id
}

// AddFunction appends f and returns its index.
func (d *Dictionary) AddFunction(f Function) int32 {
	d.Functions = append(d.Functions, f)

	return int32(len(d.Functions) - 1)
}

// AddLocation appends l and returns its index.
func (d *Dictionary) AddLocation(l Location) int32 {
	d.Locations = append(d.Locations, l)

	return int32(len(d.Locations) - 1)
}

// AddMapping appends m and returns its index.
func (d *Dictionary) AddMapping(m Mapping) int32 {
	d.Mappings = append(d.Mappings, m)

	return int32(len(d.Mappings) - 1)
}

// AddStack appends a stack of the given leaf-first location indices and returns its index.
func (d *Dictionary) AddStack(locationIndices ...int32) int32 {
	d.Stacks = append(d.Stacks, Stack{LocationIndices: locationIndices})

	return int32(len(d.Stacks) - 1)
}

// AddAttribute appends a and returns its index.
func (d *Dictionary) AddAttribute(a KeyValueAndUnit) int32 {
	d.Attributes = append(d.Attributes, a)

	return int32(len(d.Attributes) - 1)
}

// AddLink appends a link and returns its index. Index 0 is a valid link here; a sample with no link
// leaves LinkIndex 0 and the projection treats a zero-id link with empty ids as absent.
func (d *Dictionary) AddLink(l Link) int32 {
	d.Links = append(d.Links, l)

	return int32(len(d.Links) - 1)
}

// AddResource appends a fresh [ResourceProfiles] and returns a pointer to it.
func (p *Profiles) AddResource() *ResourceProfiles {
	p.Resources = grow(p.Resources)
	rp := &p.Resources[len(p.Resources)-1]
	rp.Resource = signal.Resource{}
	rp.Scopes = rp.Scopes[:0]

	return rp
}

// AddScope appends a fresh [ScopeProfiles] under the resource.
func (rp *ResourceProfiles) AddScope() *ScopeProfiles {
	rp.Scopes = grow(rp.Scopes)
	sp := &rp.Scopes[len(rp.Scopes)-1]
	sp.Scope = signal.Scope{}
	sp.Profiles = sp.Profiles[:0]

	return sp
}

// AddProfile appends a fresh, fully-zeroed [Profile] under the scope.
func (sp *ScopeProfiles) AddProfile() *Profile {
	sp.Profiles = grow(sp.Profiles)
	pr := &sp.Profiles[len(sp.Profiles)-1]
	*pr = Profile{}

	return pr
}

// AddSample appends a fully-zeroed [Sample] to the profile.
func (pr *Profile) AddSample() *Sample {
	pr.Samples = grow(pr.Samples)
	s := &pr.Samples[len(pr.Samples)-1]
	*s = Sample{}

	return s
}

// grow extends s by one element, reusing the retained backing array when len < cap.
func grow[T any](s []T) []T {
	if len(s) < cap(s) {
		return s[:len(s)+1]
	}

	var zero T

	return append(s, zero)
}

var profilesPool = sync.Pool{New: func() any { return &Profiles{} }}

// GetProfiles returns a reset [Profiles] from a shared pool; pair with [PutProfiles].
func GetProfiles() *Profiles {
	p, _ := profilesPool.Get().(*Profiles)

	return p
}

// PutProfiles resets p and returns it to the pool. Do not use p afterward.
func PutProfiles(p *Profiles) {
	p.Reset()
	profilesPool.Put(p)
}
