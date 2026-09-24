package storage

import (
	"context"
	"slices"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/backend"
	"github.com/oteldb/storage/backend/backendtest"
	"github.com/oteldb/storage/internal/obs"
)

const probeKey = "p/obj"

var probePayload = []byte("0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ-_")

type wrapFunc func(backend.Backend) backend.Backend

// capabilityProbes observe each optional capability through a wrapper by behavior, not by type
// assertion: a wrapper implements every method unconditionally and falls back through the package
// helpers, so what forwarding changes is which path the inner backend sees.
var capabilityProbes = []struct {
	name  string
	probe func(t *testing.T, wrap wrapFunc) bool
}{
	{"Viewer", func(t *testing.T, wrap wrapFunc) bool {
		t.Helper()

		w := wrap(seeded(t, backend.Memory()))

		a := readOK(t, backend.ReadView, w)
		b := readOK(t, backend.ReadView, w)

		return &a[0] == &b[0]
	}},
	{"ViewerAt", func(t *testing.T, wrap wrapFunc) bool {
		t.Helper()

		mem := seeded(t, backend.Memory())
		stored, err := backend.ReadView(context.Background(), mem, probeKey)
		require.NoError(t, err)

		counted := backendtest.NewByteCounter(seeded(t, backend.Memory()))

		aliased := readRangeOK(t, backend.ReadViewAt, wrap(mem))
		readRangeOK(t, backend.ReadViewAt, wrap(counted))

		return &aliased[0] == &stored[8] && counted.Bytes() == 4
	}},
	{"ReaderAt", func(t *testing.T, wrap wrapFunc) bool {
		t.Helper()

		counted := backendtest.NewByteCounter(seeded(t, backend.Memory()))
		readRangeOK(t, backend.ReadAt, wrap(counted))

		return counted.Bytes() == 4
	}},
	{"Sizer", func(t *testing.T, wrap wrapFunc) bool {
		t.Helper()

		counted := backendtest.NewSizedByteCounter(seeded(t, backend.Memory()))

		n, err := backend.SizeOf(context.Background(), wrap(counted), probeKey)
		require.NoError(t, err)
		require.Equal(t, int64(len(probePayload)), n)

		return counted.Reads() == 0
	}},
	{"ObjectCreator", func(t *testing.T, wrap wrapFunc) bool {
		t.Helper()

		inner := backendtest.NewStreamingMemory()
		w := wrap(inner)
		createOK(t, w, backend.CreateObject)

		return backend.StreamsWrites(w) && inner.Creates() == 1
	}},
	{"DeferredSyncer", func(t *testing.T, wrap wrapFunc) bool {
		t.Helper()

		inner := backendtest.WithDeferred(backend.Memory())
		w := wrap(inner)
		ctx := context.Background()

		require.NoError(t, backend.WriteDeferred(ctx, w, "p/a", probePayload))
		createOK(t, w, backend.CreateObjectDeferred)

		pending := inner.Pending("p")
		require.NoError(t, backend.SyncPrefix(ctx, w, "p"))

		return slices.Equal(pending, []string{"p/a", "p/new"}) && len(inner.Pending("p")) == 0
	}},
	{"DeleteDeferred", func(t *testing.T, wrap wrapFunc) bool {
		t.Helper()

		inner := backendtest.WithDeferred(seeded(t, backend.Memory()))
		require.NoError(t, backend.DeleteDeferred(context.Background(), wrap(inner), probeKey))

		return slices.Contains(inner.Deletes(), "~"+probeKey)
	}},
	{"NodeLocal", func(_ *testing.T, wrap wrapFunc) bool {
		return backend.IsNodeLocal(wrap(backend.Memory()))
	}},
	{"SpaceReporter", func(_ *testing.T, wrap wrapFunc) bool {
		n, err := backend.FreeSpace(context.Background(), wrap(backendtest.WithCapacity(backend.Memory(), 123, backendtest.Unknown)))

		return err == nil && n == 123
	}},
	{"InodeReporter", func(_ *testing.T, wrap wrapFunc) bool {
		n, err := backend.FreeInodes(context.Background(), wrap(backendtest.WithCapacity(backend.Memory(), backendtest.Unknown, 45)))

		return err == nil && n == 45
	}},
}

// TestWrappersForwardExactlyTheirInnerCapabilities holds every backend wrapper to one rule: it has a
// capability exactly when the backend beneath it does. A dropped one costs silently — a ranged read
// fetching whole objects, a streamed write back in RAM, a disk the merge cap cannot see — and an
// invented one lies to the merge engine, which sizes its output part against [backend.StreamsWrites]
// and the capacity reports.
//
// Each wrapper is probed twice: over a backend with the capability, where the probe must observe it
// forwarded unless the row names it as deliberately not, and over one with none, where every value
// claim must be absent and every method must still answer through its fallback.
func TestWrappersForwardExactlyTheirInnerCapabilities(t *testing.T) {
	t.Parallel()

	all := make([]string, 0, len(capabilityProbes))
	for _, c := range capabilityProbes {
		all = append(all, c.name)
	}

	without := func(drop ...string) []string {
		return slices.DeleteFunc(slices.Clone(all), func(c string) bool { return slices.Contains(drop, c) })
	}

	cached := func(b backend.Backend) backend.Backend { return backend.Cached(b, 1<<20) }
	instrumented := func(b backend.Backend) backend.Backend { return instrumentBackend(b, obs.NewNop().Backend) }
	ecWrap := func(b backend.Backend) backend.Backend { return &ecBackend{inner: b} }

	for _, tt := range []struct {
		name     string
		wrap     wrapFunc
		forwards []string
	}{
		{"cached", cached, all},
		{"cached/cached", func(b backend.Backend) backend.Backend { return cached(cached(b)) }, all},
		{"instrumented", instrumented, all},
		// A converted part's commit marker is its EC sidecar, so a deferred delete a power cut
		// undoes could resurrect a part; the EC wrapper deletes synchronously on purpose.
		{"ec", ecWrap, without("DeleteDeferred")},
		{"ec/cached/instrumented", func(b backend.Backend) backend.Backend {
			return ecWrap(cached(instrumented(b)))
		}, without("DeleteDeferred")},
		// Takes the streaming write path over any backend while answering StreamsWrites for it.
		{"backendtest.Deferred", func(b backend.Backend) backend.Backend {
			return backendtest.WithDeferred(b)
		}, []string{"ObjectCreator"}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			for _, c := range capabilityProbes {
				assert.Equal(t, slices.Contains(tt.forwards, c.name), c.probe(t, tt.wrap), c.name)
			}

			assertFallbacks(t, tt.wrap(seeded(t, backendtest.WithoutCapabilities(backend.Memory()))))
		})
	}
}

// assertFallbacks checks w, over an inner backend with no optional capability, claims none and still
// serves every capability method through its fallback.
func assertFallbacks(t *testing.T, w backend.Backend) {
	t.Helper()

	ctx := context.Background()

	assert.False(t, backend.StreamsWrites(w), "StreamsWrites")
	assert.False(t, backend.IsNodeLocal(w), "IsNodeLocal")

	_, err := backend.FreeSpace(ctx, w)
	require.ErrorIs(t, err, backend.ErrSpaceUnknown)

	_, err = backend.FreeInodes(ctx, w)
	require.ErrorIs(t, err, backend.ErrSpaceUnknown)

	assert.Equal(t, probePayload, readOK(t, backend.ReadView, w))
	assert.Equal(t, probePayload[8:12], readRangeOK(t, backend.ReadAt, w))
	assert.Equal(t, probePayload[8:12], readRangeOK(t, backend.ReadViewAt, w))

	n, err := backend.SizeOf(ctx, w, probeKey)
	require.NoError(t, err)
	assert.Equal(t, int64(len(probePayload)), n)

	createOK(t, w, backend.CreateObject)
	require.NoError(t, backend.WriteDeferred(ctx, w, "p/a", probePayload))
	require.NoError(t, backend.SyncPrefix(ctx, w, "p"))
	require.NoError(t, backend.DeleteDeferred(ctx, w, "p/a"))

	keys, err := w.List(ctx, "p/")
	require.NoError(t, err)
	assert.NotContains(t, keys, "p/a")
}

func seeded(t *testing.T, b backend.Backend) backend.Backend {
	t.Helper()
	require.NoError(t, b.Write(context.Background(), probeKey, probePayload))

	return b
}

func readOK(
	t *testing.T, read func(context.Context, backend.Backend, string) ([]byte, error), b backend.Backend,
) []byte {
	t.Helper()

	got, err := read(context.Background(), b, probeKey)
	require.NoError(t, err)
	require.Equal(t, probePayload, got)

	return got
}

func readRangeOK(
	t *testing.T, read func(context.Context, backend.Backend, string, int64, int64) ([]byte, error), b backend.Backend,
) []byte {
	t.Helper()

	got, err := read(context.Background(), b, probeKey, 8, 4)
	require.NoError(t, err)
	require.Equal(t, probePayload[8:12], got)

	return got
}

// createOK streams "p/new" through create and checks it landed.
func createOK(
	t *testing.T, b backend.Backend, create func(context.Context, backend.Backend, string) (backend.ObjectWriter, error),
) {
	t.Helper()

	ctx := context.Background()

	w, err := create(ctx, b, "p/new")
	require.NoError(t, err)

	_, err = w.Write(probePayload)
	require.NoError(t, err)
	require.NoError(t, w.Commit(ctx))

	got, err := b.Read(ctx, "p/new")
	require.NoError(t, err)
	require.Equal(t, probePayload, got)
}
