package s3_test

import (
	"context"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	awss3 "github.com/aws/aws-sdk-go-v2/service/s3"
	fsserver "github.com/go-faster/fs/server"
	"github.com/go-faster/fs/storagemem"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/backend"
	"github.com/oteldb/storage/backend/backendtest"
	"github.com/oteldb/storage/backend/s3"
)

// embeddedS3 starts an in-process, S3-compatible server (go-faster/fs over in-memory storage)
// and returns an aws-sdk-go-v2 client pointed at it. No Docker or MinIO is required, so the
// integration test runs in normal `go test`.
func embeddedS3(t *testing.T, bucket string) *awss3.Client {
	t.Helper()

	store := storagemem.New()
	require.NoError(t, store.CreateBucket(context.Background(), bucket))

	srv := httptest.NewServer(backendtest.AtomicConditionalPut(fsserver.NewHandler(store)))
	t.Cleanup(srv.Close)

	return awss3.New(awss3.Options{
		Region:       "us-east-1",
		BaseEndpoint: aws.String(srv.URL),
		UsePathStyle: true, // address as endpoint/bucket/key
		Credentials:  credentials.NewStaticCredentialsProvider("test", "test", ""),
	})
}

// TestS3IntegrationEmbedded runs the full backend conformance suite over the aws-sdk-go-v2
// adapter against a real S3 protocol implementation (the embeddable go-faster/fs server). It
// exercises the adapter's GET/PUT/HEAD/DELETE/paginated-LIST — and the If-None-Match
// PutIfAbsent CAS (go-faster/fs ≥ v0.2.0) — against actual HTTP + XML, which the in-memory
// fakes cannot.
func TestS3IntegrationEmbedded(t *testing.T) {
	t.Parallel()

	const bucket = "oteldb-test"
	store := s3.NewAWS(embeddedS3(t, bucket), bucket)

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
	b := s3.New(s3.NewAWS(embeddedS3(t, bucket), bucket), "oteldb/")
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
	b := s3.New(s3.NewAWS(embeddedS3(t, bucket), bucket), "oteldb/")

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
