package backend

import (
	"context"
	"io"
)

// ObjectReader is a seekable object handle. Read/Seek share a nonconcurrent cursor; ReadAt is independent.
type ObjectReader interface {
	io.Reader
	io.ReaderAt
	io.Seeker
	io.Closer
	Size() int64
}

// ObjectOpener optionally opens reusable object handles without materializing the whole object.
type ObjectOpener interface {
	OpenReader(ctx context.Context, key string) (ObjectReader, bool, error)
	// ReaderMemory bounds retained handle state, including any internal buffers; false declines sizing.
	ReaderMemory(key string) (int64, bool)
}

// OpenReader returns an optional handle; supported=false requests the existing ranged-read path.
func OpenReader(ctx context.Context, b Backend, key string) (reader ObjectReader, supported bool, err error) {
	if opener, ok := b.(ObjectOpener); ok {
		return opener.OpenReader(ctx, key)
	}
	return nil, false, nil
}

// ReaderMemory bounds an optional handle before opening it, excluding caller-owned buffers.
func ReaderMemory(b Backend, key string) (int64, bool) {
	if opener, ok := b.(ObjectOpener); ok {
		n, known := opener.ReaderMemory(key)
		return n, known && n >= 0
	}
	return 0, false
}
