package storage

import (
	"github.com/oteldb/storage/backend"
	"github.com/oteldb/storage/internal/buildbudget"
	"github.com/oteldb/storage/internal/memsize"
)

// WriterMemory includes the metering adapter and retained key in the inner workspace bound.
func (b *instrumentedBackend) WriterMemory(key string) (int64, bool) {
	n, known := backend.WriterMemory(b.inner, key)
	if !known {
		return 0, false
	}
	overhead := buildbudget.Add(memsize.Of[instrumentedObjectWriter](), int64(len(key)))
	return buildbudget.Add(n, overhead), true
}
