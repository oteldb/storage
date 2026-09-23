package backendtest

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/backend"
)

// Case names a backend factory in a test or benchmark table.
type Case struct {
	Name string
	Open func(tb testing.TB) backend.Backend
}

// Memory is the [backend.Memory] case.
func Memory() Case {
	return Case{Name: "memory", Open: func(testing.TB) backend.Backend { return backend.Memory() }}
}

// Dir is a case that opens a backend over a fresh tb.TempDir, failing tb when open does.
func Dir[B backend.Backend](name string, open func(dir string) (B, error)) Case {
	return Case{Name: name, Open: func(tb testing.TB) backend.Backend {
		tb.Helper()

		b, err := open(tb.TempDir())
		require.NoError(tb, err)

		return b
	}}
}

// Cached is c behind a [backend.Cached] read cache of budget bytes, named "cached-" + c.Name.
func Cached(c Case, budget int64) Case {
	return Case{Name: "cached-" + c.Name, Open: func(tb testing.TB) backend.Backend {
		tb.Helper()

		return backend.Cached(c.Open(tb), budget)
	}}
}

// WholeObject is memory behind [WithoutCapabilities]: every read is a whole-object Read.
func WholeObject() Case {
	return Case{Name: "whole-object", Open: func(testing.TB) backend.Backend {
		return WithoutCapabilities(backend.Memory())
	}}
}

// Matrix is every read path a backend offers: memory, file, file behind a 1 MiB read cache, s3, and
// [WholeObject]. This package cannot import the file or s3 backends, so the caller supplies them.
func Matrix(file, s3 Case) []Case {
	return []Case{Memory(), file, Cached(file, 1<<20), s3, WholeObject()}
}
