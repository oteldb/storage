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
	dst = s.Resource.appendHashInput(dst)
	dst = s.Scope.appendHashInput(dst)

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

// Equal reports whether two resources are deeply equal.
func (r Resource) Equal(o Resource) bool {
	return bytes.Equal(r.SchemaURL, o.SchemaURL) && r.Attributes.Equal(o.Attributes)
}

// Clone returns a deep copy of the resource.
func (r Resource) Clone() Resource {
	return Resource{SchemaURL: bytes.Clone(r.SchemaURL), Attributes: r.Attributes.Clone()}
}

func (r Resource) appendHashInput(dst []byte) []byte {
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

func (s Scope) appendHashInput(dst []byte) []byte {
	dst = appendLenBytes(dst, s.Name)
	dst = appendLenBytes(dst, s.Version)
	dst = appendLenBytes(dst, s.SchemaURL)

	return s.Attributes.AppendHashInput(dst)
}
