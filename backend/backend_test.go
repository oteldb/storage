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
