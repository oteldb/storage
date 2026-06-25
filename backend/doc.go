// Package backend defines the L1 storage seam (DESIGN.md §3, §5): a common
// [backend.Backend] interface (Read/Write/List/Delete/CAS) with interchangeable
// implementations — memory (ephemeral, the reference and test default), file, and s3.
// The in-memory backend is first-class: the whole engine runs with no disk or object
// store. Not yet implemented (M1 for memory+file, M5 for s3).
package backend
