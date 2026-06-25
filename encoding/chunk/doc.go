// Package chunk implements the zero-alloc value-column codecs (DESIGN.md §14 M0):
// delta-of-delta timestamps, Gorilla XOR float gauges, T64, dictionary, and
// scaled-decimal+nearest-delta. Each codec is an append-style
// Encode(dst []byte, ...) []byte over [bitstream] and is fuzzed for round-trip
// identity. Not yet implemented (M0 in progress; bitstream is the first foundation).
package chunk
