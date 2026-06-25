package backend

// Backend is the L1 storage seam (DESIGN.md §3, §5): a common interface over
// interchangeable implementations — memory (ephemeral), file, and s3. The same
// engine code path runs over all three; the in-memory backend is the reference
// implementation and the default in tests.
//
// Data is addressed by an opaque string key (e.g. a time-bucketed object path or a
// file-relative path). All methods are safe for concurrent use.
//
// This is a scaffold stub: the full interface (Read/Write/List/Delete/CAS, readers
// with range/offset, the CAS result type) is filled in at M1 (memory+file) and M5
// (s3). Only [Backend.IsEphemeral] is exposed now — the engine uses it to default
// [storage.Durability] (DESIGN.md §5).
type Backend interface {
	// IsEphemeral reports whether the backend stores data only in RAM (dropped on
	// [Backend.Close]). [backend.Memory] is ephemeral; file and s3 are not.
	IsEphemeral() bool

	// TODO(M1): Write(ctx, key, data) error
	// TODO(M1): Read(ctx, key) (io.ReadCloser, error)
	// TODO(M1): List(ctx, prefix) (Iterator, error)
	// TODO(M1): Delete(ctx, key) error
	// TODO(M5): PutIfAbsent(ctx, key, data) (ok bool, error)  // CAS
}

// Memory returns an ephemeral in-memory backend (DESIGN.md §5). The whole engine
// runs over it with no disk or object store; parts live in RAM and are dropped on
// [Backend.Close]. It is the reference [Backend] implementation and the default in
// tests.
//
// The full implementation lives in backend/memory (M1); this stub satisfies the
// interface for the facade scaffold.
func Memory() Backend { return memoryBackend{} }

// memoryBackend is the scaffold in-memory backend. Real implementation lands in M1
// (backend/memory package); this stub satisfies the interface for the facade scaffold.
type memoryBackend struct{}

func (memoryBackend) IsEphemeral() bool { return true }
