package backend_test

import (
	"context"
	"testing"

	"github.com/oteldb/storage/backend"
	"github.com/oteldb/storage/backend/backendtest"
)

func TestMemoryConformance(t *testing.T) {
	t.Parallel()
	backendtest.Run(t, func(*testing.T) backend.Backend {
		return backend.Memory()
	})
}

func TestMemoryIsEphemeral(t *testing.T) {
	t.Parallel()
	if !backend.Memory().IsEphemeral() {
		t.Fatal("memory backend must report ephemeral")
	}
}

func TestIsNodeLocal(t *testing.T) {
	t.Parallel()

	if !backend.IsNodeLocal(backend.Memory()) {
		t.Fatal("memory backend must report node-local")
	}

	if !backend.IsNodeLocal(backend.Cached(backend.Memory(), 1<<20)) {
		t.Fatal("the read cache must forward node-locality")
	}

	if backend.IsNodeLocal(withoutCapabilities{backend.Memory()}) {
		t.Fatal("a backend without the capability must not be assumed node-local")
	}
}

// withoutCapabilities hides every optional capability of the backend it delegates to by not
// embedding it.
type withoutCapabilities struct{ inner backend.Backend }

func (b withoutCapabilities) IsEphemeral() bool { return b.inner.IsEphemeral() }

func (b withoutCapabilities) PutIfAbsent(ctx context.Context, key string, data []byte) (bool, error) {
	return b.inner.PutIfAbsent(ctx, key, data)
}

func (b withoutCapabilities) Write(ctx context.Context, key string, data []byte) error {
	return b.inner.Write(ctx, key, data)
}

func (b withoutCapabilities) Read(ctx context.Context, key string) ([]byte, error) {
	return b.inner.Read(ctx, key)
}

func (b withoutCapabilities) List(ctx context.Context, prefix string) ([]string, error) {
	return b.inner.List(ctx, prefix)
}

func (b withoutCapabilities) Delete(ctx context.Context, key string) error {
	return b.inner.Delete(ctx, key)
}
