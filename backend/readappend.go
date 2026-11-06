package backend

import "context"

// ReadAppender appends a caller-owned object range to dst.
type ReadAppender interface {
	ReadAtAppend(ctx context.Context, dst []byte, key string, off, n int64) ([]byte, error)
}

// ReadAtAppend appends a clamped range to dst, reusing its capacity. Errors preserve its prefix.
func ReadAtAppend(ctx context.Context, b Backend, dst []byte, key string, off, n int64) ([]byte, error) {
	if err := validateRange(key, off, n); err != nil {
		return dst, err
	}
	if r, ok := b.(ReadAppender); ok {
		return r.ReadAtAppend(ctx, dst, key, off, n)
	}
	data, err := ReadViewAt(ctx, b, key, off, n)
	if err != nil {
		return dst, err
	}
	return append(dst, data...), nil
}
