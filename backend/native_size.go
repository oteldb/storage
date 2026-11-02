package backend

import "context"

// NativeSizer reports object sizes without reading or materializing their contents.
// known=false means this key has no such answer; n is valid only when known and err==nil.
type NativeSizer interface {
	NativeSize(ctx context.Context, key string) (n int64, known bool, err error)
}

// NativeSizeOf asks [NativeSizer], returning unknown without a Read or SizeOf fallback.
func NativeSizeOf(ctx context.Context, b Backend, key string) (int64, bool, error) {
	if err := ctx.Err(); err != nil {
		return 0, false, err
	}
	if s, ok := b.(NativeSizer); ok {
		return s.NativeSize(ctx, key)
	}
	return 0, false, nil
}
