// Package indexcommit confirms ambiguous bucket-index writes before publication.
package indexcommit

import (
	"bytes"
	"context"
	"time"

	"github.com/go-faster/errors"

	"github.com/oteldb/storage/backend"
	"github.com/oteldb/storage/backend/bucketindex"
)

// Save commits ix, confirming a lost response when the backend holds the exact candidate.
// An unconfirmed error still requires retaining outputs and the WAL; it proves no rollback.
func Save(ctx context.Context, ix *bucketindex.Index, b backend.Backend, key string, expected backend.Version) (backend.Version, error) {
	data := ix.AppendBinary(nil)
	version, ok, err := b.CompareAndSwap(ctx, key, expected, data)
	if err == nil {
		if !ok {
			return backend.VersionAbsent, errors.Wrapf(bucketindex.ErrConflict, "commit index %q", key)
		}
		return version, nil
	}

	// Cancellation can lose a successful response; confirmation must outlive the request but be bounded.
	confirm, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
	defer cancel()
	stored, current, readErr := b.ReadVersioned(confirm, key)
	if readErr == nil && current != backend.VersionAbsent && bytes.Equal(stored, data) {
		return current, nil
	}
	return backend.VersionAbsent, errors.Wrapf(err, "commit index %q (outcome unconfirmed)", key)
}
