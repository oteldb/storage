package signal

// Reserved label keys. A stream's instrumentation-scope identity has no attribute of its own, so
// the name and version are indexed as synthetic labels: a query matches them exactly as it matches
// a resource or scope attribute, and [Series.Label] resolves them alongside the real ones.
//
// They are the single source of truth for the engines that index them and for an embedder lowering
// a query to a [github.com/oteldb/storage/query/fetch.Matcher] — a language-level `scope name`
// selector has no other way to spell the key.
const (
	// LabelScopeName is the label a stream's [Scope] Name is indexed under.
	LabelScopeName = "otel.scope.name"
	// LabelScopeVersion is the label a stream's [Scope] Version is indexed under.
	LabelScopeVersion = "otel.scope.version"
)
