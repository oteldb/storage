package s3_test

import (
	"context"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/backend"
	"github.com/oteldb/storage/backend/backendtest"
	"github.com/oteldb/storage/backend/s3"
	"github.com/oteldb/storage/backend/s3/s3test"
)

// TestS3IntegrationEmbedded runs the full backend conformance suite over the aws-sdk-go-v2
// adapter against a real S3 protocol implementation (the embeddable go-faster/fs server). It
// exercises the adapter's GET/PUT/HEAD/DELETE/paginated-LIST — and the If-None-Match
// PutIfAbsent CAS (go-faster/fs ≥ v0.2.0) — against actual HTTP + XML, which the in-memory
// fakes cannot.
func TestS3IntegrationEmbedded(t *testing.T) {
	t.Parallel()

	const bucket = "oteldb-test"
	store := s3.NewAWS(s3test.Client(t, bucket), bucket)

	// Each subtest gets an isolated key prefix in the shared bucket.
	backendtest.Run(t, func(t *testing.T) backend.Backend {
		t.Helper()

		return s3.New(store, strings.ReplaceAll(t.Name(), "/", "_")+"/")
	})
}

// TestS3IntegrationStreamedObject drives the streamed write over real HTTP and XML: create, several
// UploadParts, complete. The in-memory fake reproduces the adapter's calls but not the wire shape
// of the part list, which is where a multipart upload is most easily got wrong.
func TestS3IntegrationStreamedObject(t *testing.T) {
	t.Parallel()

	const bucket = "oteldb-stream"

	ctx := context.Background()
	b := s3.New(s3.NewAWS(s3test.Client(t, bucket), bucket), "oteldb/")
	require.True(t, backend.StreamsWrites(b), "the AWS adapter uploads in parts")

	want := payload(streamedBytes)

	w, err := backend.CreateObject(ctx, b, "part/col")
	require.NoError(t, err)
	defer w.Abort()

	writeStreamed(t, w, want)
	require.NoError(t, w.Commit(ctx))

	got, err := b.Read(ctx, "part/col")
	require.NoError(t, err)
	require.Equal(t, want, got)

	// A ranged read of the assembled object is what the merge's read side will do to it, and a
	// multipart object's ETag is not its digest — so this also pins that nothing downstream reads
	// the ETag as one.
	head, err := backend.ReadAt(ctx, b, "part/col", 1<<20, 4096)
	require.NoError(t, err)
	assert.Equal(t, want[1<<20:(1<<20)+4096], head)
}

// TestS3IntegrationStreamedAbort pins the other half over the wire: an aborted upload leaves no
// object and no upload the bucket would keep billing.
func TestS3IntegrationStreamedAbort(t *testing.T) {
	t.Parallel()

	const bucket = "oteldb-stream-abort"

	ctx := context.Background()
	b := s3.New(s3.NewAWS(s3test.Client(t, bucket), bucket), "oteldb/")

	w, err := backend.CreateObject(ctx, b, "part/col")
	require.NoError(t, err)

	writeStreamed(t, w, payload(streamedBytes))
	w.Abort()

	_, err = b.Read(ctx, "part/col")
	require.ErrorIs(t, err, backend.ErrNotExist)

	keys, err := b.List(ctx, "")
	require.NoError(t, err)
	assert.Empty(t, keys, "an aborted stream leaves no debris behind")
}

// TestS3IntegrationAbortIsIdempotent pins the adapter's one error translation against a real
// server's codes rather than against a fake that returns them by construction: an upload the store
// no longer knows leaves nothing to abort, so the cleanup path must read that as success.
func TestS3IntegrationAbortIsIdempotent(t *testing.T) {
	t.Parallel()

	const bucket = "oteldb-abort"

	ctx := context.Background()

	store, ok := s3.NewAWS(s3test.Client(t, bucket), bucket).(s3.MultipartObjectStore)
	require.True(t, ok)

	require.NoError(t, store.AbortMultipartUpload(ctx, "k", "never-existed"))

	id, err := store.CreateMultipartUpload(ctx, "k")
	require.NoError(t, err)
	require.NoError(t, store.AbortMultipartUpload(ctx, "k", id))
	require.NoError(t, store.AbortMultipartUpload(ctx, "k", id))
}
