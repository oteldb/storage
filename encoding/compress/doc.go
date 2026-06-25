// Package compress wraps zstd/lz4 block compression and the composable codec chains
// (preprocessor → general compressor) used to reach 0.4–0.8 bytes/point (DESIGN.md
// §3.3). Not yet implemented (M0 in progress).
package compress
