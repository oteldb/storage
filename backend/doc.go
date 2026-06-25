// Package backend defines the L1 storage seam (DESIGN.md §3, §5): a common
// [Backend] interface (Read/Write/List/Delete over whole-object keys) with
// interchangeable implementations. The in-memory backend ([Memory], in this package)
// is first-class — the reference and test default — so the whole engine runs with no
// disk or object store. The file backend lives in backend/file. The s3 backend and
// compare-and-swap (CAS) are M5.
package backend
