package engine_test

import (
	"context"
	"strings"
	"testing"

	"github.com/go-faster/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/backend"
	"github.com/oteldb/storage/backend/faultbackend"
	"github.com/oteldb/storage/engine"
	"github.com/oteldb/storage/internal/obs/obstest"
)

const corruptionDetected = "storage.corruption.detected"

func flushedMetricStore(ctx context.Context, t *testing.T) backend.Backend {
	t.Helper()

	mem := backend.Memory()
	e := engine.New(engine.Config{Backend: mem, Prefix: "default/metrics"})
	_, err := e.Append(mkSeries("__name__", "up"), 100, 1)
	require.NoError(t, err)
	require.NoError(t, e.Flush(ctx))

	return mem
}

func newObservedMetricEngine(t *testing.T, be backend.Backend) (*engine.Engine, *obstest.Metrics) {
	t.Helper()

	o, m := obstest.New(t)

	return engine.New(engine.Config{Backend: be, Prefix: "default/metrics", Obs: o}), m
}

func onRead(be backend.Backend, suffix string, rule faultbackend.Rule) *faultbackend.Backend {
	rule.Kind = faultbackend.Read
	rule.Match = func(op faultbackend.Op) bool { return strings.HasSuffix(op.Key, suffix) }

	return faultbackend.Wrap(be).Add(rule)
}

func flipLastByte(_ faultbackend.Op, b []byte) []byte {
	out := append([]byte(nil), b...)
	out[len(out)-1] ^= 0xff

	return out
}

func TestCorruptionIsCounted(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	t.Run("ManifestFatal", func(t *testing.T) {
		t.Parallel()

		e, m := newObservedMetricEngine(t, onRead(flushedMetricStore(ctx, t), "/manifest", faultbackend.Rule{Replace: flipLastByte}))
		require.Error(t, e.LoadParts(ctx))

		assert.Equal(t, int64(1), m.Counter(corruptionDetected, "component", "part", "disposition", "fatal"))
	})

	t.Run("PartIdentityTolerated", func(t *testing.T) {
		t.Parallel()

		e, m := newObservedMetricEngine(t, onRead(flushedMetricStore(ctx, t), "/identity", faultbackend.Rule{Replace: flipLastByte}))
		require.NoError(t, e.LoadParts(ctx))

		assert.Equal(t, int64(1), m.Counter(corruptionDetected, "component", "part_identity", "disposition", "tolerated"))
	})

	t.Run("ReadFailureIsNotCorruption", func(t *testing.T) {
		t.Parallel()

		e, m := newObservedMetricEngine(t, onRead(flushedMetricStore(ctx, t), "/manifest",
			faultbackend.Rule{Err: errors.New("injected timeout")}))
		require.Error(t, e.LoadParts(ctx))

		assert.Zero(t, m.Counter(corruptionDetected))
	})
}
