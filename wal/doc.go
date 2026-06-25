// Package wal implements the L3 write-ahead log: CRC + binary framing over segments
// (DESIGN.md §3, §14 M2). Append-style, group-commit-friendly. Not yet implemented (M2).
package wal
