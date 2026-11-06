package storage

import "github.com/oteldb/storage/backend"

// WriterMemory forwards the unchanged raw writer's workspace bound.
func (e *ecBackend) WriterMemory(key string) (int64, bool) {
	return backend.WriterMemory(e.inner, key)
}
