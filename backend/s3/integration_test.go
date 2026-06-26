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

	srv := httptest.NewServer(fsserver.NewHandler(store))
	t.Cleanup(srv.Close)

	return awss3.New(awss3.Options{
		Region:       "us-east-1",
		BaseEndpoint: aws.String(srv.URL),
		UsePathStyle: true, // address as endpoint/bucket/key
		Credentials:  credentials.NewStaticCredentialsProvider("test", "test", ""),
	})
}

// TestS3IntegrationEmbedded runs the core backend conformance suite over the aws-sdk-go-v2
// adapter against a real S3 protocol implementation (the embeddable go-faster/fs server). It
// exercises the adapter's GET/PUT/HEAD/DELETE/paginated-LIST against actual HTTP + XML, which
// the in-memory fakes cannot.
//
// It uses [backendtest.RunCore], not Run: go-faster/fs v0.1.0 does not implement the
// If-None-Match conditional write, so the PutIfAbsent CAS is covered separately by the
// in-memory fakeAWS conformance (TestAWSAdapterConformance), which honors it. Verifying the
// CAS over a real wire needs a conditional-write-capable store (AWS S3 / MinIO).
func TestS3IntegrationEmbedded(t *testing.T) {
	t.Parallel()

	const bucket = "oteldb-test"
	store := s3.NewAWS(embeddedS3(t, bucket), bucket)

	// Each subtest gets an isolated key prefix in the shared bucket.
	backendtest.RunCore(t, func(t *testing.T) backend.Backend {
		t.Helper()

		return s3.New(store, strings.ReplaceAll(t.Name(), "/", "_")+"/")
	})
}
