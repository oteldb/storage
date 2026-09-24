package enginetest

import (
	"context"
	"slices"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/backend"
	"github.com/oteldb/storage/backend/faultbackend"
	"github.com/oteldb/storage/internal/reproduce"
)

// staleLoad is the shape #685 needs: e holds only part A, while the committed index names A, B and C
// because another writer committed B and C after e last loaded. B's manifest is the one a load of e
// fails on.
type staleLoad struct {
	be   *faultbackend.Backend
	e    Engine
	b, c string
}

// newStaleLoad builds a [staleLoad]. breakB runs once B is committed and before C is, so it can make
// B unopenable for e the way the test needs (corrupt it, or fail its next read).
func (k Kind) newStaleLoad(t *testing.T, cfg Config, breakB func(be *faultbackend.Backend, manifest string)) staleLoad {
	t.Helper()

	ctx := context.Background()
	be := faultbackend.Wrap(backend.Memory())
	cfg.Backend = be

	w := k.open(t, be)
	w.Append(t, api(100, 1))
	require.NoError(t, w.Flush(ctx))

	e := k.Open(t, cfg)
	require.NoError(t, e.LoadParts(ctx))

	b := k.flushNew(t, w, be, api(200, 2))

	keys := k.partKeys(ctx, t, be, b)
	i := slices.IndexFunc(keys, func(key string) bool { return strings.HasSuffix(key, "/manifest") })
	require.GreaterOrEqual(t, i, 0)
	breakB(be, keys[i])

	c := k.flushNew(t, w, be, web(300, 3))

	return staleLoad{be: be, e: e, b: k.Prefix + "/" + b, c: k.Prefix + "/" + c}
}

// flushNew flushes rows through w and returns the id of the one part the flush added.
func (k Kind) flushNew(t *testing.T, w Engine, be backend.Backend, rows ...Row) string {
	t.Helper()

	ctx := context.Background()
	before := k.partDirs(ctx, t, be)

	w.Append(t, rows...)
	require.NoError(t, w.Flush(ctx))

	var added []string

	for _, id := range k.partDirs(ctx, t, be) {
		if !slices.Contains(before, id) {
			added = append(added, id)
		}
	}

	require.Len(t, added, 1)

	return added[0]
}

func corruptObject(t *testing.T) func(be *faultbackend.Backend, key string) {
	t.Helper()

	return func(be *faultbackend.Backend, key string) {
		require.NoError(t, be.Write(context.Background(), key, []byte("not a manifest")))
	}
}

// failedLoadKeepsUnopenedParts is #685: a load that fails on a part it cannot open must not leave the
// engine conditioned on the new index while it still holds the old part set, or its next commit
// passes the compare-and-swap and drops every entry it never opened.
func failedLoadKeepsUnopenedParts(t *testing.T, k Kind) {
	t.Helper()

	reproduce.Unfixed(t, 685, "the next flush commits an index without the unopenable part")

	ctx := context.Background()
	s := k.newStaleLoad(t, Config{}, corruptObject(t))

	require.Error(t, s.e.LoadParts(ctx), "B's manifest is corrupt")

	s.e.Append(t, api(400, 4))
	_ = s.e.Flush(ctx)

	names := prefixes(k.loadIndex(t, s.be).Entries)
	require.Contains(t, names, s.b, "the part the load could not open is still committed")
	require.Contains(t, names, s.c, "and so is the one another writer committed")
}
