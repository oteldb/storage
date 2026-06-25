// Package bitstream implements a zero-alloc, MSB-first bit stream reader and writer.
//
// It is the foundation of every codec in encoding/chunk (delta-of-delta, Gorilla XOR,
// T64, dictionary, scaled-decimal+nearest-delta): DoubleDelta and Gorilla live or die
// on a correct, fast bit packer (DESIGN.md §10, §14 M0). The reader is modeled on the
// Prometheus/dgryski bstream (an 8-byte refill buffer with a valid-bit count); the
// writer is append-style ([Writer.WriteBits] appends to a caller-owned []byte).
//
// All hot-path methods are leaf functions kept small for inlining. The Writer never
// allocates beyond the append growth of its backing slice; callers may [Reset] it onto
// a pooled buffer. The Reader takes a []byte view and performs no allocation after
// construction.
package bitstream
