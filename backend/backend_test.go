package backend_test

import (
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

	if backend.IsNodeLocal(backendtest.WithoutCapabilities(backend.Memory())) {
		t.Fatal("a backend without the capability must not be assumed node-local")
	}
}
