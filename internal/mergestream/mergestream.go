// Package mergestream holds the pieces a streaming merge needs in both the metric and the record
// engine: the k-way key union, the two-unit memory budget, and the forward-cursor contract. Each
// engine keeps its own merge driver; only what they would otherwise each invent lives here.
package mergestream
