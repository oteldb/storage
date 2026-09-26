package storage

import (
	"io/fs"
	"path/filepath"
	"slices"
	"strings"

	"github.com/go-faster/errors"

	"github.com/oteldb/storage/backend"
	"github.com/oteldb/storage/signal"
)

// walTenant is the first path segment no engine prefix may take. An engine owns every key under
// its prefix — the orphan sweep lists it, retention and merges delete under it — so a tenant
// resolving to "wal" or "wal/…" would own the keys of a WAL kept at <backend root>/wal, the
// conventional layout. [Options.validateWALPlacement] refuses that layout for a backend that reports
// its directory; this reservation covers one that cannot.
const walTenant = "wal"

var errReservedTenant = errors.New("tenant id is reserved for the WAL namespace")

// tenantReserved reports whether an engine keyed by tid would have a prefix inside the reserved WAL
// namespace. tid may be a shard key: those only append to the tenant id.
func tenantReserved(tid signal.TenantID) bool {
	first, _, _ := strings.Cut(string(tid), "/")

	return first == walTenant
}

func checkEnginePrefix(prefix string) error {
	if tenantReserved(signal.TenantID(prefix)) {
		return errors.Wrapf(errReservedTenant, "engine prefix %q", prefix)
	}

	return nil
}

// validateWALPlacement refuses a [Options.WALDir] overlapping the backend's local directory, in
// either direction: the backend would list and delete WAL segments as its own keys, or WAL recovery
// would walk every part directory.
func (o *Options) validateWALPlacement() error {
	if o.WALDir == "" || o.Backend == nil {
		return nil
	}

	root, ok := backend.DirOf(o.Backend)
	if !ok {
		return nil
	}

	walDir, err := resolvePath(o.WALDir)
	if err != nil {
		return errors.Wrapf(err, "resolve WALDir %q", o.WALDir)
	}

	if root, err = resolvePath(root); err != nil {
		return errors.Wrapf(err, "resolve backend dir %q", root)
	}

	if pathWithin(walDir, root) || pathWithin(root, walDir) {
		return errOptionInvalid("WALDir " + walDir + " overlaps the backend directory " + root +
			": the backend owns every file under its root, so place the WAL beside it, not inside")
	}

	return nil
}

// resolvePath returns p absolute with every symlink on its longest existing ancestor resolved, so
// two spellings of one directory compare equal before either exists.
func resolvePath(p string) (string, error) {
	abs, err := filepath.Abs(p)
	if err != nil {
		return "", err
	}

	var missing []string

	for dir := abs; ; dir = filepath.Dir(dir) {
		resolved, err := filepath.EvalSymlinks(dir)
		if err == nil {
			slices.Reverse(missing)

			return filepath.Join(append([]string{resolved}, missing...)...), nil
		}

		if !errors.Is(err, fs.ErrNotExist) || filepath.Dir(dir) == dir {
			return "", err
		}

		missing = append(missing, filepath.Base(dir))
	}
}

func pathWithin(child, parent string) bool {
	rel, err := filepath.Rel(parent, child)

	return err == nil && filepath.IsLocal(rel)
}
