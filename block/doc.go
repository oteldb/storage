// Package block implements the L2 immutable columnar part format (DESIGN.md §3, §14
// M1): per-column streams with min/max stats and constant-column collapse, a sparse
// granule mark index, and an atomic manifest written last. Not yet implemented (M1).
package block
