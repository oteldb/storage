package backendtest

import "github.com/oteldb/storage/backend"

// WithoutCapabilities wraps b exposing only the [backend.Backend] methods: every optional capability
// ([backend.Viewer], [backend.Sizer], [backend.ReaderAt], [backend.ObjectCreator],
// [backend.DeferredSyncer], [backend.NodeLocal], …) is hidden, so callers take their fallback paths —
// the shape of a minimal embedder backend.
func WithoutCapabilities(b backend.Backend) backend.Backend { return bare{b} }

type bare struct{ backend.Backend }
