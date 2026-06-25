// Package compress wraps zstd/lz4 block compression and defines the composable
// codec-chain type (preprocessor → general compressor) used to reach 0.4–0.8
// bytes/point (DESIGN.md §3.3, §14 M0).
//
// A [Chain] is a sequence of a column-specific preprocessor (from [chunk]) followed
// by a general compressor (ZSTD or LZ4). Cold-tier recompression is just a different
// chain over the same parts. The wrappers are append-style
// (Compress(dst, src) []byte / Decompress(dst, src) []byte) and use pooled encoders/
// decoders to avoid per-call allocation.
package compress
