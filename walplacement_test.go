package storage

import (
	"context"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/backend"
	"github.com/oteldb/storage/backend/bucketindex"
	"github.com/oteldb/storage/backend/file"
	"github.com/oteldb/storage/cluster/etcd/etcdtest"
	"github.com/oteldb/storage/internal/obs/obstest"
	"github.com/oteldb/storage/query/fetch"
	"github.com/oteldb/storage/signal"
)

// keyspaceOnly hides every optional capability, [backend.LocalDir] included: a backend whose
// directory storage cannot learn, so only the tenant reservation stands between its keys and a WAL.
type keyspaceOnly struct{ backend.Backend }

func tenantByService(r signal.Resource, _ signal.Scope) signal.TenantID {
	v, _ := r.Attributes.Get([]byte("service.name"))

	return signal.TenantID(v.Str())
}

func TestTenantReserved(t *testing.T) {
	t.Parallel()

	for tid, want := range map[signal.TenantID]bool{
		"wal":        true,
		"wal/logs":   true,
		"wal/_s1":    true,
		"WAL/logs":   true,
		"Wal":        true,
		`wal\logs`:   true,
		`logs\x`:     true,
		"walrus":     false,
		"wal_":       false,
		"x/wal":      false,
		"default":    false,
		"":           false,
		"logs/wal/x": false,
	} {
		assert.Equal(t, want, tenantReserved(tid), "%q", tid)
	}
}

func TestOpenRefusesWALOverlappingFileRoot(t *testing.T) {
	t.Parallel()

	fileAt := func(t *testing.T, dir string) backend.Backend {
		t.Helper()

		be, err := file.New(dir)
		require.NoError(t, err)

		return be
	}

	for _, tt := range []struct {
		name   string
		layout func(t *testing.T) (backend.Backend, string)
		refuse bool
	}{
		{"wal equals root", func(t *testing.T) (backend.Backend, string) {
			t.Helper()
			root := t.TempDir()

			return fileAt(t, root), root
		}, true},
		{"wal inside root", func(t *testing.T) (backend.Backend, string) {
			t.Helper()
			root := t.TempDir()

			return fileAt(t, root), filepath.Join(root, "wal")
		}, true},
		{"wal inside root, unclean spelling", func(t *testing.T) (backend.Backend, string) {
			t.Helper()
			root := t.TempDir()

			return fileAt(t, root), filepath.Join(root, "x", "..", "wal") + string(filepath.Separator)
		}, true},
		{"root inside wal", func(t *testing.T) (backend.Backend, string) {
			t.Helper()
			dir := t.TempDir()

			return fileAt(t, filepath.Join(dir, "parts")), dir
		}, true},
		{"wal inside cached root", func(t *testing.T) (backend.Backend, string) {
			t.Helper()
			root := t.TempDir()

			return backend.Cached(fileAt(t, root), 1<<20), filepath.Join(root, "wal")
		}, true},
		{"sibling directories", func(t *testing.T) (backend.Backend, string) {
			t.Helper()
			dir := t.TempDir()

			return fileAt(t, filepath.Join(dir, "parts")), filepath.Join(dir, "wal")
		}, false},
		{"sibling sharing a name prefix", func(t *testing.T) (backend.Backend, string) {
			t.Helper()
			dir := t.TempDir()

			return fileAt(t, filepath.Join(dir, "data")), filepath.Join(dir, "data-wal")
		}, false},
		{"backend without a directory", func(t *testing.T) (backend.Backend, string) {
			t.Helper()
			root := t.TempDir()

			return keyspaceOnly{fileAt(t, root)}, filepath.Join(root, "wal")
		}, false},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			ctx := context.Background()
			be, walDir := tt.layout(t)

			s, err := Open(ctx, Options{}, WithBackend(be), WithWALDir(walDir), WithFlushInterval(-1))
			if tt.refuse {
				require.ErrorContains(t, err, "overlaps the backend directory")

				return
			}

			require.NoError(t, err)

			_, err = s.WriteMetrics(ctx, gaugeBatch("api", "m", []int64{1}, []float64{1}))
			require.NoError(t, err)
			require.NoError(t, s.Close(ctx))
		})
	}
}

func TestOpenRefusesWALInsideSymlinkedRoot(t *testing.T) {
	t.Parallel()

	if runtime.GOOS == "windows" {
		t.Skip("creating a symlink needs a privilege Windows test runners usually lack")
	}

	dir := t.TempDir()
	root := filepath.Join(dir, "root")
	link := filepath.Join(dir, "link")
	require.NoError(t, os.Mkdir(root, 0o750))
	require.NoError(t, os.Symlink(root, link))

	be, err := file.New(root)
	require.NoError(t, err)

	_, err = Open(context.Background(), Options{}, WithBackend(be), WithWALDir(filepath.Join(link, "wal")))
	require.ErrorContains(t, err, "overlaps the backend directory")
}

func TestReservedTenantRejected(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	mp, m := obstest.Provider(t)

	s, err := InMemory(WithTenant(tenantByService), WithMeterProvider(mp))
	require.NoError(t, err)
	t.Cleanup(func() { _ = s.Close(ctx) })

	for _, tid := range []string{"wal", "wal/logs"} {
		acc, err := s.WriteMetrics(ctx, gaugeBatch(tid, "m", []int64{1, 2}, []float64{1, 2}))
		require.NoError(t, err)
		assert.Equal(t, Accepted{Rejected: 2, RejectedReason: reasonReservedTenant}, acc, tid)

		acc, err = s.WriteLogs(ctx, logBatch(tid, [3]any{1, 9, "x"}))
		require.NoError(t, err)
		assert.Equal(t, Accepted{Rejected: 1, RejectedReason: reasonReservedTenant}, acc, tid)

		_, ok := s.lookupEngine(signal.TenantID(tid))
		assert.False(t, ok, "no metric engine for %q", tid)

		_, ok = s.lookupLogEngine(signal.TenantID(tid))
		assert.False(t, ok, "no log engine for %q", tid)
	}

	acc, err := s.WriteLogs(ctx, logBatch("walrus", [3]any{1, 9, "x"}))
	require.NoError(t, err)
	assert.Equal(t, int64(1), acc.Accepted)

	assert.Equal(t, int64(4), m.Counter("storage.ingest.rejected", "signal", "metric", "reason", reasonReservedTenant))
	assert.Equal(t, int64(2), m.Counter("storage.ingest.rejected", "signal", "log", "reason", reasonReservedTenant))
}

//nolint:paralleltest // owns an embedded etcd; runs serially
func TestClusteredReservedTenantRejected(t *testing.T) {
	endpoint := etcdtest.Start(t)
	ctx := context.Background()

	nodes := map[string]*Storage{
		"node-a": openClusterNodeSharded(t, endpoint, "node-a", 2, WithTenant(tenantByService)),
		"node-b": openClusterNodeSharded(t, endpoint, "node-b", 2, WithTenant(tenantByService)),
	}
	awaitMembership(t, nodes)

	a := nodes["node-a"]

	acc, err := a.WriteMetrics(ctx, gaugeBatch("wal", "m", []int64{1, 2}, []float64{1, 2}))
	require.NoError(t, err)
	assert.Equal(t, Accepted{Rejected: 2, RejectedReason: reasonReservedTenant}, acc)

	acc, err = a.WriteLogs(ctx, logBatch("wal/logs", [3]any{1, 9, "x"}))
	require.NoError(t, err)
	assert.Equal(t, Accepted{Rejected: 1, RejectedReason: reasonReservedTenant}, acc)

	acc, err = a.WriteLogs(ctx, logBatch("api", [3]any{1, 9, "x"}))
	require.NoError(t, err)
	assert.Equal(t, Accepted{Accepted: 1}, acc)
}

func TestEngineCreationRefusesReservedPrefix(t *testing.T) {
	t.Parallel()

	s, err := InMemory()
	require.NoError(t, err)
	t.Cleanup(func() { _ = s.Close(context.Background()) })

	_, err = s.engineFor("wal")
	require.ErrorIs(t, err, errReservedTenant)

	for _, create := range []func(signal.TenantID) error{
		func(tid signal.TenantID) error { _, err := s.logEngineFor(tid); return err },
		func(tid signal.TenantID) error { _, err := s.traceEngineFor(tid); return err },
		func(tid signal.TenantID) error { _, err := s.profileEngineFor(tid); return err },
		func(tid signal.TenantID) error { _, err := s.exemplarEngineFor(tid); return err },
	} {
		require.ErrorIs(t, create("wal"), errReservedTenant)
		require.ErrorIs(t, create("wal/_s3"), errReservedTenant)
		require.NoError(t, create("walrus"))
	}
}

// TestReservedTenantCannotReachWAL keeps a WAL at <root>/wal under a backend that cannot report its
// root, the layout only the reservation guards. A tenant resolving to "wal" would key its logs
// engine at wal/logs/, the directory holding tenant "logs"'s WAL; its ingest must be rejected, and
// flush, the open-time orphan sweep, and close must leave that WAL intact.
func TestReservedTenantCannotReachWAL(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	root := t.TempDir()
	walDir := filepath.Join(root, "wal")
	victim := filepath.Join(walDir, "logs", "logs")

	open := func() *Storage {
		t.Helper()

		be, err := file.New(root)
		require.NoError(t, err)

		s, err := Open(ctx, Options{}, WithBackend(keyspaceOnly{be}), WithWALDir(walDir),
			WithFlushInterval(-1), WithOrphanGrace(1), WithTenant(tenantByService))
		require.NoError(t, err)

		return s
	}

	segments := func() []string {
		t.Helper()

		m, err := filepath.Glob(filepath.Join(victim, "*.wal"))
		require.NoError(t, err)

		return m
	}

	s1 := open()
	for i, svc := range []string{"logs", "wal", "wal/logs"} {
		acc, err := s1.WriteLogs(ctx, logBatch(svc, [3]any{10 + i, 9, "flushed"}))
		require.NoError(t, err)
		assert.Equal(t, svc == "logs", acc.Rejected == 0, svc)
	}

	s1.maintain(ctx)

	acc, err := s1.WriteLogs(ctx, logBatch("logs", [3]any{20, 9, "unflushed"}))
	require.NoError(t, err)
	require.Equal(t, int64(1), acc.Accepted)
	require.NotEmpty(t, segments(), "the unflushed record is in a WAL segment")

	lister, err := file.New(root)
	require.NoError(t, err)

	keys, err := lister.List(ctx, walTenant+"/")
	require.NoError(t, err)

	for _, k := range keys {
		assert.True(t, strings.HasSuffix(k, ".wal"), "only WAL segments live under wal/: %q", k)
	}

	crash(t, s1)

	s2 := open()
	require.NotEmpty(t, segments(), "the open-time orphan sweep left the WAL alone")

	got := logBodies(t, s2.LogFetcher("logs"), fetch.Request{Start: 0, End: 1000})
	assert.ElementsMatch(t, []string{"flushed", "unflushed"}, got)

	s2.maintain(ctx)
	require.NoError(t, s2.Close(ctx))

	info, err := os.Stat(victim)
	require.NoError(t, err, "the WAL directory survives close")
	assert.True(t, info.IsDir())
}

func TestPathsOverlapByIdentity(t *testing.T) {
	t.Parallel()

	if runtime.GOOS == "windows" {
		t.Skip("creating a symlink needs a privilege Windows test runners usually lack")
	}

	dir := t.TempDir()
	root := filepath.Join(dir, "root")
	link := filepath.Join(dir, "link")
	require.NoError(t, os.Mkdir(root, 0o750))
	require.NoError(t, os.Symlink(root, link))

	assert.True(t, pathsOverlap(filepath.Join(link, "wal", "x"), root), "a missing WAL below an alias of the root")
	assert.True(t, pathsOverlap(root, link), "the root under another name")
	assert.False(t, pathsOverlap(filepath.Join(dir, "wal"), root), "a sibling")
	assert.False(t, pathsOverlap(filepath.Join(dir, "rootwal"), root), "a sibling sharing a name prefix")
}

func TestOpenRefusesWALInsideCaseAliasOfRoot(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	root := filepath.Join(dir, "Parts")
	require.NoError(t, os.Mkdir(root, 0o750))

	if _, err := os.Stat(filepath.Join(dir, "parts")); err != nil {
		t.Skip("the temp filesystem is case-sensitive, so no case alias of the root exists")
	}

	be, err := file.New(root)
	require.NoError(t, err)

	_, err = Open(context.Background(), Options{}, WithBackend(be), WithWALDir(filepath.Join(dir, "parts", "wal")))
	require.ErrorContains(t, err, "overlaps the backend directory")
}

// TestRecoverySkipsReservedTenant persists a tenant under the reserved id, as a release predating the
// reservation could, both as flushed parts and as unflushed WAL segments. Open must still succeed and
// serve every other tenant, leaving the reserved tenant's data where it is.
func TestRecoverySkipsReservedTenant(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	root, walDir := t.TempDir(), t.TempDir()

	open := func(opts ...Option) *Storage {
		t.Helper()

		be, err := file.New(root)
		require.NoError(t, err)

		s, err := Open(ctx, Options{}, append([]Option{
			WithBackend(be), WithWALDir(walDir), WithFlushInterval(-1), WithTenant(tenantByService),
		}, opts...)...)
		require.NoError(t, err)

		return s
	}

	s1 := open()
	for _, svc := range []string{"legacy", "api"} {
		_, err := s1.WriteMetrics(ctx, gaugeBatch(svc, "m", []int64{1}, []float64{1}))
		require.NoError(t, err)
		_, err = s1.WriteLogs(ctx, logBatch(svc, [3]any{1, 9, "flushed"}))
		require.NoError(t, err)
	}

	s1.maintain(ctx)

	for _, svc := range []string{"legacy", "api"} {
		_, err := s1.WriteMetrics(ctx, gaugeBatch(svc, "m", []int64{2}, []float64{2}))
		require.NoError(t, err)
	}

	crash(t, s1)

	require.NoError(t, os.Rename(filepath.Join(root, "legacy"), filepath.Join(root, walTenant)))
	require.NoError(t, os.Rename(filepath.Join(walDir, "legacy"), filepath.Join(walDir, walTenant)))

	mp, m := obstest.Provider(t)
	s2 := open(WithMeterProvider(mp))
	t.Cleanup(func() { _ = s2.Close(ctx) })

	batches := mustDrain(t, s2.Fetcher("api"), fetch.Request{Start: 0, End: 1 << 60, Matchers: []fetch.Matcher{nameMatcher("m")}})
	require.Len(t, batches, 1)
	assert.Equal(t, []int64{1, 2}, batches[0].Timestamps, "the other tenant serves its parts and its replayed WAL")

	_, ok := s2.lookupEngine(walTenant)
	assert.False(t, ok)
	_, ok = s2.lookupLogEngine(walTenant)
	assert.False(t, ok)

	assert.Equal(t, int64(2), m.Counter("storage.tenant.reserved_skipped", "source", reservedSourceRecovery),
		"one bucket index per signal")
	assert.Equal(t, int64(1), m.Counter("storage.tenant.reserved_skipped", "source", reservedSourceWAL))

	_, err := os.Stat(filepath.Join(root, walTenant, "metrics", bucketindex.Object))
	require.NoError(t, err, "the reserved tenant's parts stay on disk")

	segments, err := filepath.Glob(filepath.Join(walDir, walTenant, "metrics", "*.wal"))
	require.NoError(t, err)
	assert.NotEmpty(t, segments, "the reserved tenant's WAL stays on disk")
}

// TestPartSyncSkipsReservedTenant has a peer hold a tenant under the reserved id, as a node running a
// release predating the reservation could. Mirroring it must do no I/O on this node's backend.
//
//nolint:paralleltest // owns an embedded etcd; runs serially
func TestPartSyncSkipsReservedTenant(t *testing.T) {
	endpoint := etcdtest.Start(t)
	ctx := context.Background()

	beA, beB := backend.Memory(), backend.Memory()
	mp, m := obstest.Provider(t)

	nodes := map[string]*Storage{
		"node-a": openClusterNodeWith(t, endpoint, "node-a", beA, WithMeterProvider(mp)),
		"node-b": openClusterNodeWith(t, endpoint, "node-b", beB),
	}
	awaitMembership(t, nodes)

	standIn := walTenant + "/metrics/00000000000000000001-00000000000000000001.wal"

	require.NoError(t, beA.Write(ctx, standIn, []byte("segment")))
	require.NoError(t, beB.Write(ctx, walTenant+"/metrics/"+bucketindex.Object, []byte("index")))
	require.NoError(t, beB.Write(ctx, walTenant+"/metrics/01M3EYF8X9KT5H1BRB7H82SGMB/manifest", []byte("part")))

	for _, strict := range []bool{false, true} {
		synced, st, err := nodes["node-a"].syncPartsResult(ctx, walTenant, metricsPrefix, strict)
		require.NoError(t, err)
		assert.False(t, synced)
		assert.Zero(t, st)
	}

	keys, err := beA.List(ctx, walTenant+"/")
	require.NoError(t, err)
	assert.Equal(t, []string{standIn}, keys, "nothing copied, nothing pruned")
	assert.Equal(t, int64(2), m.Counter("storage.tenant.reserved_skipped", "source", reservedSourcePartsync))
}
