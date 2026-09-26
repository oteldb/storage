package storage

import (
	"context"
	"io/fs"
	"os"
	"path/filepath"
	"slices"
	"strings"

	"github.com/go-faster/errors"
	"go.uber.org/zap"

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

// tenantReserved reports whether an engine keyed by tid could have a prefix inside the reserved WAL
// namespace. tid may be a shard key: those only append to the tenant id. The segment is compared
// case-insensitively and a backslash is refused outright, since a case-insensitive filesystem or a
// Windows path maps those spellings onto the same directory.
func tenantReserved(tid signal.TenantID) bool {
	first, _, _ := strings.Cut(string(tid), "/")

	return strings.EqualFold(first, walTenant) || strings.Contains(string(tid), `\`)
}

func checkEnginePrefix(prefix string) error {
	if tenantReserved(signal.TenantID(prefix)) {
		return errors.Wrapf(errReservedTenant, "engine prefix %q", prefix)
	}

	return nil
}

// Sources of a skipped reserved tenant: the "source" attribute of storage.tenant.reserved_skipped.
const (
	reservedSourceRecovery  = "recovery"
	reservedSourceWAL       = "wal_replay"
	reservedSourceBootstrap = "bootstrap"
	reservedSourcePartsync  = "partsync"
)

// skipReserved reports whether tid is reserved, logging and counting the skip. Every path that learns
// a tenant from outside the write path — the backend at recovery, the WAL directory, etcd claims, a
// peer — runs it before touching that tenant's keys. It skips rather than fails, so a tenant
// persisted before the reservation cannot keep the store from opening; its data stays on disk,
// unserved.
func (s *Storage) skipReserved(ctx context.Context, tid signal.TenantID, prefix, source string) bool {
	if !tenantReserved(tid) {
		return false
	}

	s.obs.Logger(ctx).Error("skipping a tenant whose id is reserved for the WAL namespace; its data is left on disk",
		zap.String("tenant", string(tid)), zap.String("prefix", prefix), zap.String("source", source))
	s.obs.Parts.ReservedTenantSkipped(ctx, source)

	return true
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

	if pathsOverlap(walDir, root) {
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

// pathsOverlap reports whether either path lies within the other, lexically or by filesystem
// identity. Identity catches what lexical comparison cannot: two spellings of one directory on a
// case-insensitive filesystem.
func pathsOverlap(a, b string) bool {
	return pathWithin(a, b) || pathWithin(b, a) || sameFileWithin(a, b) || sameFileWithin(b, a)
}

func pathWithin(child, parent string) bool {
	rel, err := filepath.Rel(parent, child)

	return err == nil && filepath.IsLocal(rel)
}

// sameFileWithin reports whether an existing ancestor of child is the same directory as parent's
// nearest existing ancestor, with parent's missing components leading child's path below it.
func sameFileWithin(child, parent string) bool {
	pdir, pmissing := nearestExisting(parent)

	pinfo, err := os.Stat(pdir)
	if err != nil {
		return false
	}

	dir, below := nearestExisting(child)

	for {
		if info, err := os.Stat(dir); err == nil && os.SameFile(info, pinfo) {
			return len(below) >= len(pmissing) && slices.Equal(below[:len(pmissing)], pmissing)
		}

		if filepath.Dir(dir) == dir {
			return false
		}

		below = append([]string{filepath.Base(dir)}, below...)
		dir = filepath.Dir(dir)
	}
}

// nearestExisting splits an absolute path into its longest existing ancestor and the components
// below it.
func nearestExisting(p string) (string, []string) {
	var missing []string

	for ; filepath.Dir(p) != p; p = filepath.Dir(p) {
		if _, err := os.Stat(p); err == nil {
			break
		}

		missing = append(missing, filepath.Base(p))
	}

	slices.Reverse(missing)

	return p, missing
}
