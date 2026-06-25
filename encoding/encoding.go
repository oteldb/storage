// Package encoding holds the L2 codec layers (DESIGN.md §3, §14 M0):
//   - [bitstream]: MSB-first bit stream reader/writer (the foundation)
//   - encoding/chunk: value-column codecs (DoD, Gorilla, T64, dictionary, scaled-decimal)
//   - encoding/compress: zstd/lz4 block wrappers and codec chains
package encoding

// Profile is the default codec-chain profile per column kind (DESIGN.md §6). It is
// consumed by [storage.Options] and applied when flushing a part. Zero values mean
// "use the built-in default chain for that column kind".
//
// This is a scaffold stub; the concrete chain type is filled in at M0 as the codecs
// land. It is a placeholder now so the [storage.Options] shape is stable.
type Profile struct {
	// TODO(M0): Timestamp Chain  // delta-of-delta default
	// TODO(M0): FloatGauge Chain // Gorilla XOR or scaled-decimal+nearest-delta
	// TODO(M0): Counter Chain    // delta
	// TODO(M0): LowCard Chain    // dictionary
	// TODO(M0): General Compressor // zstd/lz4 on top
}
