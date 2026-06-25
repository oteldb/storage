// Package index implements the L3 indexing layer (DESIGN.md §3, §14 M2):
//   - postings: inverted label→[]SeriesID lists with set-op iterators
//   - symbols: string interning / symbol table
//   - bloom: token bloom filters for full-text and the tokenizer
//   - series: identity → SeriesID (TSID-style key)
//
// Not yet implemented (M2).
package index
