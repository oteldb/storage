package recordengine

import (
	"context"

	"github.com/go-faster/errors"

	"github.com/oteldb/storage/backend"
)

// SideStore is an optional per-engine auxiliary store that rides the part lifecycle. A signal whose
// records reference content-addressed side data (e.g. the profiles symbol store: strings, functions,
// locations, stacks) supplies one via [Config.SideStore]; the engine then absorbs each batch's delta
// ([Batch.Side]) into a live accumulator, persists the accumulator as part sidecars on flush, and
// unions the sidecars of compacted parts on merge. The engine treats the data opaquely — only the
// signal package knows the table formats.
//
// Content-addressing is the load-bearing assumption: an entry's id is a hash of its content, so the
// same entry has the same id everywhere and [SideStore.Union] is a plain dedup with no id remap.
//
// All methods are called under the engine's lock, so an implementation need not be safe for
// concurrent use. [SideStore.Union] must be a pure function of its arguments and must not read or
// mutate the live accumulator (it merges already-flushed part data, independent of the head).
type SideStore interface {
	// Absorb merges one batch's encoded side delta ([Batch.Side]) into the live accumulator.
	Absorb(delta []byte) error
	// Encode serializes the accumulated side data into named sidecar payloads (name → bytes),
	// written as {prefix}/sym-{name}.bin at flush.
	Encode() map[string][]byte
	// Reset clears the live accumulator (after a flush drains the head).
	Reset()
	// Names returns the sidecar names to read back for a part on merge (the keys [SideStore.Encode]
	// may produce). A part missing a named sidecar is skipped.
	Names() []string
	// Union merges the loaded sidecars of the compacted parts (one map per part) and returns the
	// merged named payloads to write under the new part. Pure; ignores the live accumulator.
	Union(parts []map[string][]byte) (map[string][]byte, error)
}

// sidecarKey is the backend key of a side-store table sidecar under a part prefix (mirrors the
// per-column bloom sidecars, e.g. {prefix}/sym-stacks.bin).
func sidecarKey(prefix, name string) string { return prefix + "/sym-" + name + ".bin" }

// writeSidecars writes each named side-store payload under prefix.
func writeSidecars(ctx context.Context, b backend.Backend, prefix string, m map[string][]byte) error {
	for name, data := range m {
		if err := b.Write(ctx, sidecarKey(prefix, name), data); err != nil {
			return errors.Wrapf(err, "write sidecar %q", name)
		}
	}

	return nil
}

// loadSidecars reads the named side-store sidecars under prefix, skipping any that are absent.
func loadSidecars(ctx context.Context, b backend.Backend, prefix string, names []string) (map[string][]byte, error) {
	out := make(map[string][]byte, len(names))

	for _, name := range names {
		data, err := b.Read(ctx, sidecarKey(prefix, name))

		switch {
		case errors.Is(err, backend.ErrNotExist):
			continue
		case err != nil:
			return nil, errors.Wrapf(err, "read sidecar %q", name)
		}

		out[name] = data
	}

	return out, nil
}
