package indexcommit

import (
	"context"
	"testing"

	"github.com/go-faster/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/backend"
	"github.com/oteldb/storage/backend/bucketindex"
)

type lostResponse struct {
	backend.Backend

	cancel context.CancelFunc
	err    error
	before bool
}

func (b lostResponse) CompareAndSwap(ctx context.Context, key string, expected backend.Version, data []byte) (backend.Version, bool, error) {
	if b.before {
		return backend.VersionAbsent, false, b.err
	}
	v, ok, err := b.Backend.CompareAndSwap(ctx, key, expected, data)
	if err != nil || !ok {
		return v, ok, err
	}
	if b.cancel != nil {
		b.cancel()
	}
	return backend.VersionAbsent, false, b.err
}

func TestSaveConfirmsLostResponse(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name   string
		before bool
		cancel bool
	}{
		{name: "lost-response"},
		{name: "canceled-response", cancel: true},
		{name: "before-write", before: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			injected := errors.New("lost response")
			be := lostResponse{Backend: backend.Memory(), err: injected, before: tc.before}
			if tc.cancel {
				be.cancel = cancel
			}
			ix := &bucketindex.Index{Generation: bucketindex.Generation{}}
			version, err := Save(ctx, ix, be, "index", backend.VersionAbsent)
			if tc.before {
				require.ErrorIs(t, err, injected)
				return
			}
			require.NoError(t, err)
			assert.NotEqual(t, backend.VersionAbsent, version)
			_, err = Save(t.Context(), ix, be, "index", backend.VersionAbsent)
			require.ErrorIs(t, err, bucketindex.ErrConflict)
		})
	}
}
