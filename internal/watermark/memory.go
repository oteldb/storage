package watermark

import "github.com/oteldb/storage/internal/memsize"

const readerWindowBytes = 32 << 10

// MemoryBound prices a reader/cursor pair and sequential validation, including ranged-read copies.
func MemoryBound() (resident, open int64) {
	resident = memsize.Of[Reader]() + memsize.Of[Cursor]() + 2*readerWindowBytes + 256
	open = 3*readerWindowBytes + 256
	return resident, open
}
