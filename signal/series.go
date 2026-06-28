package signal

import "bytes"

// Resource is an OTel Resource: the entity (service instance, host, …) that produced the
// telemetry. Its attributes are identifying — any differing key/value is a different
// resource — and schema_url is part of its identity.
type Resource struct {
	SchemaURL  []byte
	Attributes Attributes
}

// Scope is an OTel InstrumentationScope: the logical instrumentation unit (name, version,
// schema_url, attributes) that produced the telemetry. A distinct tuple is a distinct
// grouping, so all four fields are identifying.
type Scope struct {
	Name       []byte
	Version    []byte
	SchemaURL  []byte
	Attributes Attributes
}

// Series is the signal-neutral identity of a time series: the OTel three-level identity
// backbone — its [Resource], the [Scope] that produced it, and its data-point
// [Attributes]. Equal Series have equal [Series.Hash].
//
// Signal-specific packages extend this with their own identifying fields: a metric
// series adds name, unit, temporality and monotonicity to the hash pre-image (via
// [Series.AppendHashInput] then their own fields) before computing the final id.
type Series struct {
	Resource   Resource
	Scope      Scope
	Attributes Attributes
}

// AppendHashInput appends the canonical, unambiguous hash pre-image of the whole identity
// (resource ‖ scope ‖ attributes) to dst. Each component is length-delimited, so no two
// distinct identities share a pre-image. It is the reversible wire form decoded by
// [DecodeSeries].
func (s Series) AppendHashInput(dst []byte) []byte {
	dst = s.Resource.AppendHashInput(dst)
	dst = s.Scope.AppendHashInput(dst)

	return s.Attributes.AppendHashInput(dst)
}

// Hash returns the content-addressed [SeriesID] of the full identity.
func (s Series) Hash() SeriesID { return HashBytes(s.AppendHashInput(nil)) }

// Equal reports whether two identities are deeply equal.
func (s Series) Equal(o Series) bool {
	return s.Resource.Equal(o.Resource) && s.Scope.Equal(o.Scope) && s.Attributes.Equal(o.Attributes)
}

// Clone returns a deep copy of the identity.
func (s Series) Clone() Series {
	return Series{Resource: s.Resource.Clone(), Scope: s.Scope.Clone(), Attributes: s.Attributes.Clone()}
}

// Intern returns a copy of the identity with every string/byte payload replaced by fn(payload), so
// all byte storage is drawn from one shared pool (one owned copy per distinct string). It is the
// long-lived-storage form of Clone: a series index that interns its identities holds references to
// a single symbol table instead of a private clone per series. A nil fn yields a plain deep copy.
func (s Series) Intern(fn func([]byte) []byte) Series {
	return Series{Resource: s.Resource.Intern(fn), Scope: s.Scope.Intern(fn), Attributes: s.Attributes.Intern(fn)}
}

// Equal reports whether two resources are deeply equal.
func (r Resource) Equal(o Resource) bool {
	return bytes.Equal(r.SchemaURL, o.SchemaURL) && r.Attributes.Equal(o.Attributes)
}

// Clone returns a deep copy of the resource.
func (r Resource) Clone() Resource {
	return Resource{SchemaURL: bytes.Clone(r.SchemaURL), Attributes: r.Attributes.Clone()}
}

// Intern returns a copy with every string/byte payload replaced by fn(payload). See [Series.Intern].
func (r Resource) Intern(fn func([]byte) []byte) Resource {
	url := r.SchemaURL
	if fn != nil {
		url = fn(url)
	} else {
		url = bytes.Clone(url)
	}

	return Resource{SchemaURL: url, Attributes: r.Attributes.Intern(fn)}
}

// AppendHashInput appends the canonical, length-delimited hash pre-image of the resource
// (schema_url ‖ attributes) to dst. It is the resource segment of [Series.AppendHashInput],
// exposed so ingest paths can build a series hash from hoisted, per-group prefixes without
// rebuilding the resource segment per point.
func (r Resource) AppendHashInput(dst []byte) []byte {
	dst = appendLenBytes(dst, r.SchemaURL)

	return r.Attributes.AppendHashInput(dst)
}

// Equal reports whether two scopes are deeply equal.
func (s Scope) Equal(o Scope) bool {
	return bytes.Equal(s.Name, o.Name) &&
		bytes.Equal(s.Version, o.Version) &&
		bytes.Equal(s.SchemaURL, o.SchemaURL) &&
		s.Attributes.Equal(o.Attributes)
}

// Clone returns a deep copy of the scope.
func (s Scope) Clone() Scope {
	return Scope{
		Name:       bytes.Clone(s.Name),
		Version:    bytes.Clone(s.Version),
		SchemaURL:  bytes.Clone(s.SchemaURL),
		Attributes: s.Attributes.Clone(),
	}
}

// Intern returns a copy with every string/byte payload replaced by fn(payload). See [Series.Intern].
func (s Scope) Intern(fn func([]byte) []byte) Scope {
	clone := fn == nil
	name, version, url := s.Name, s.Version, s.SchemaURL
	if clone {
		name = bytes.Clone(name)
		version = bytes.Clone(version)
		url = bytes.Clone(url)
	} else {
		name = fn(name)
		version = fn(version)
		url = fn(url)
	}

	return Scope{Name: name, Version: version, SchemaURL: url, Attributes: s.Attributes.Intern(fn)}
}

// AppendHashInput appends the canonical, length-delimited hash pre-image of the scope
// (name ‖ version ‖ schema_url ‖ attributes) to dst. It is the scope segment of
// [Series.AppendHashInput], exposed so ingest paths can hoist it per scope group.
func (s Scope) AppendHashInput(dst []byte) []byte {
	dst = appendLenBytes(dst, s.Name)
	dst = appendLenBytes(dst, s.Version)
	dst = appendLenBytes(dst, s.SchemaURL)

	return s.Attributes.AppendHashInput(dst)
}
