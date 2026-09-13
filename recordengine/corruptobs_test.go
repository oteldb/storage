package recordengine_test

import (
	"context"
	"strings"
	"testing"

	"github.com/go-faster/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/backend"
	"github.com/oteldb/storage/backend/bucketindex"
	"github.com/oteldb/storage/backend/faultbackend"
	"github.com/oteldb/storage/internal/obs/obstest"
	"github.com/oteldb/storage/recordengine"
)

const corruptionDetected = "storage.corruption.detected"

func newObservedEngine(t *testing.T, be backend.Backend) (*recordengine.Engine, *obstest.Metrics) {
	t.Helper()

	o, m := obstest.New(t)

	return recordengine.New(recordengine.Config{Schema: testSchema, Backend: be, Prefix: "t/recs", Obs: o}), m
}

func flushedStore(ctx context.Context, t *testing.T) backend.Backend {
	t.Helper()

	mem := backend.Memory()
	e := newEngine(t, mem)
	ingest(t, e, mkBatch("api", rrec{ts: 100, body: "alpha", attr: [2]string{"tier", "gold"}}))
	require.NoError(t, e.Flush(ctx))

	return mem
}

func flipLastByteOf(be backend.Backend, suffix string) *faultbackend.Backend {
	return faultbackend.Wrap(be).Add(faultbackend.Rule{
		Kind:  faultbackend.Read,
		Match: func(op faultbackend.Op) bool { return strings.HasSuffix(op.Key, suffix) },
		Replace: func(_ faultbackend.Op, b []byte) []byte {
			out := append([]byte(nil), b...)
			out[len(out)-1] ^= 0xff

			return out
		},
	})
}

func TestCorruptionIsCounted(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	t.Run("BloomTolerated", func(t *testing.T) {
		t.Parallel()

		e, m := newObservedEngine(t, corruptBlooms(flushedStore(ctx, t)))
		require.NoError(t, e.LoadParts(ctx))

		assert.Positive(t, m.Counter(corruptionDetected, "component", "bloom", "disposition", "tolerated"))
		assert.Zero(t, m.Counter(corruptionDetected, "disposition", "fatal"))
	})

	t.Run("ManifestFatal", func(t *testing.T) {
		t.Parallel()

		e, m := newObservedEngine(t, flipLastByteOf(flushedStore(ctx, t), "/manifest"))
		require.Error(t, e.LoadParts(ctx))

		assert.Equal(t, int64(1), m.Counter(corruptionDetected, "component", "part", "disposition", "fatal"))
	})

	t.Run("BucketIndexFatal", func(t *testing.T) {
		t.Parallel()

		be := flushedStore(ctx, t)
		require.NoError(t, be.Write(ctx, "t/recs/"+bucketindex.Object, []byte("not an index")))
		e, m := newObservedEngine(t, be)

		err := e.LoadParts(ctx)
		require.ErrorIs(t, err, bucketindex.ErrCorrupt)
		assert.Equal(t, int64(1), m.Counter(corruptionDetected, "component", "bucket_index", "disposition", "fatal"))
	})

	t.Run("ReadFailureIsNotCorruption", func(t *testing.T) {
		t.Parallel()

		be := faultbackend.Wrap(flushedStore(ctx, t)).Add(faultbackend.Rule{
			Kind:  faultbackend.Read,
			Match: func(op faultbackend.Op) bool { return strings.HasSuffix(op.Key, "/manifest") },
			Err:   errors.New("injected timeout"),
		})
		e, m := newObservedEngine(t, be)
		require.Error(t, e.LoadParts(ctx))

		assert.Zero(t, m.Counter(corruptionDetected))
	})
}
