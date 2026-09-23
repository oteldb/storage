package s3test_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/backend"
	"github.com/oteldb/storage/backend/backendtest"
	"github.com/oteldb/storage/backend/s3"
	"github.com/oteldb/storage/backend/s3/s3test"
	"github.com/oteldb/storage/reliability"
)

func TestBackendConformance(t *testing.T) {
	t.Parallel()

	backendtest.Run(t, func(t *testing.T) backend.Backend {
		t.Helper()

		return s3test.Backend(t, "conformance", s3.WithRetry(reliability.Default()))
	})
}

func TestSharedSeesOneBucket(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	open := s3test.Shared(t, "shared")

	require.NoError(t, open().Write(ctx, "k", []byte("v")))

	got, err := open().Read(ctx, "k")
	require.NoError(t, err)
	assert.Equal(t, []byte("v"), got)
}
