// Package s3test runs an in-process S3-compatible server (go-faster/fs over memory) for tests, so the
// s3 backend is exercised over real HTTP and XML without Docker or MinIO.
package s3test

import (
	"context"
	"net/http/httptest"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	awss3 "github.com/aws/aws-sdk-go-v2/service/s3"
	fsserver "github.com/go-faster/fs/server"
	"github.com/go-faster/fs/storagemem"
	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/backend"
	"github.com/oteldb/storage/backend/backendtest"
	"github.com/oteldb/storage/backend/s3"
)

// Client starts a server holding an empty bucket and returns a client for it. The server serializes
// conditional PUTs ([backendtest.AtomicConditionalPut]), and cleanup closes the client's idle
// connections before the server.
func Client(tb testing.TB, bucket string) *awss3.Client {
	tb.Helper()

	store := storagemem.New()
	require.NoError(tb, store.CreateBucket(context.Background(), bucket))

	srv := httptest.NewServer(backendtest.AtomicConditionalPut(fsserver.NewHandler(store)))
	httpClient := srv.Client()

	tb.Cleanup(func() {
		httpClient.CloseIdleConnections()
		srv.Close()
	})

	return awss3.New(awss3.Options{
		Region:       "us-east-1",
		BaseEndpoint: aws.String(srv.URL),
		UsePathStyle: true,
		HTTPClient:   httpClient,
		Credentials:  credentials.NewStaticCredentialsProvider("test", "test", ""),
	})
}

// Shared starts a server and returns a factory of backends over its bucket, at the bucket root: several
// nodes or processes against one object store.
func Shared(tb testing.TB, bucket string, opts ...s3.Option) func() backend.Backend {
	tb.Helper()

	client := Client(tb, bucket)

	return func() backend.Backend { return s3.New(s3.NewAWS(client, bucket), "", opts...) }
}

// Backend starts a server and returns a backend over its bucket, at the bucket root.
func Backend(tb testing.TB, bucket string, opts ...s3.Option) backend.Backend {
	tb.Helper()

	return Shared(tb, bucket, opts...)()
}
