package s3_test

import (
	"context"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	awss3 "github.com/aws/aws-sdk-go-v2/service/s3"
	smithy "github.com/aws/smithy-go"
	"github.com/go-faster/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/backend"
	"github.com/oteldb/storage/backend/backendtest"
	"github.com/oteldb/storage/backend/s3"
)

// TestS3Integration runs the backend conformance suite against a real S3-compatible endpoint
// (MinIO by default) through the aws-sdk-go-v2 adapter. It is skipped unless OTELDB_S3_ENDPOINT
// is set, so it is a no-op in normal `go test` yet always compiles (and so cannot rot).
//
// To run against a local MinIO:
//
//	docker run -p 9000:9000 -e MINIO_ROOT_USER=minioadmin -e MINIO_ROOT_PASSWORD=minioadmin \
//	  minio/minio server /data
//	OTELDB_S3_ENDPOINT=http://localhost:9000 go test ./backend/s3/ -run TestS3Integration -v
//
// Optional overrides: OTELDB_S3_ACCESS_KEY, OTELDB_S3_SECRET_KEY, OTELDB_S3_BUCKET,
// OTELDB_S3_REGION (defaults: minioadmin / minioadmin / oteldb-test / us-east-1).
//
//nolint:paralleltest // a serial integration test against a shared external endpoint
func TestS3Integration(t *testing.T) {
	endpoint := os.Getenv("OTELDB_S3_ENDPOINT")
	if endpoint == "" {
		t.Skip("set OTELDB_S3_ENDPOINT (e.g. http://localhost:9000) to run the MinIO/S3 integration test")
	}

	bucket := envOr("OTELDB_S3_BUCKET", "oteldb-test")
	client := awss3.New(awss3.Options{
		Region:       envOr("OTELDB_S3_REGION", "us-east-1"),
		BaseEndpoint: aws.String(endpoint),
		UsePathStyle: true, // MinIO requires path-style addressing
		Credentials: credentials.NewStaticCredentialsProvider(
			envOr("OTELDB_S3_ACCESS_KEY", "minioadmin"),
			envOr("OTELDB_S3_SECRET_KEY", "minioadmin"),
			"",
		),
	})

	ctx := context.Background()
	ensureBucket(ctx, t, client, bucket)

	store := s3.NewAWS(client, bucket)

	// Each run uses a unique root so re-runs and parallel subtests never collide; the run's
	// objects are best-effort deleted at the end.
	run := "it/" + strconv.FormatInt(time.Now().UnixNano(), 10) + "/"
	t.Cleanup(func() { deleteAllUnder(ctx, client, bucket, run) })

	// The conformance suite over the real endpoint: each subtest gets an isolated key prefix.
	backendtest.Run(t, func(t *testing.T) backend.Backend {
		t.Helper()

		return s3.New(store, run+strings.ReplaceAll(t.Name(), "/", "_")+"/")
	})

	// A direct smoke check that a part-shaped commit survives a round trip.
	b := s3.New(store, run+"smoke/")
	require.NoError(t, b.Write(ctx, "default/metrics/0/manifest", []byte("m")))
	ok, err := b.PutIfAbsent(ctx, "default/metrics/0/manifest", []byte("dup"))
	require.NoError(t, err)
	assert.False(t, ok, "PutIfAbsent must lose to the existing object (If-None-Match)")
	got, err := b.Read(ctx, "default/metrics/0/manifest")
	require.NoError(t, err)
	assert.Equal(t, []byte("m"), got)
}

func envOr(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}

	return def
}

// ensureBucket creates the bucket, tolerating "already exists / owned by you".
func ensureBucket(ctx context.Context, t *testing.T, client *awss3.Client, bucket string) {
	t.Helper()

	_, err := client.CreateBucket(ctx, &awss3.CreateBucketInput{Bucket: aws.String(bucket)})
	if err == nil {
		return
	}

	var ae smithy.APIError
	if errors.As(err, &ae) {
		switch ae.ErrorCode() {
		case "BucketAlreadyOwnedByYou", "BucketAlreadyExists":
			return
		}
	}

	require.NoError(t, err, "create bucket %q", bucket)
}

// deleteAllUnder best-effort removes every object under prefix (test teardown).
func deleteAllUnder(ctx context.Context, client *awss3.Client, bucket, prefix string) {
	p := awss3.NewListObjectsV2Paginator(client, &awss3.ListObjectsV2Input{
		Bucket: aws.String(bucket), Prefix: aws.String(prefix),
	})
	for p.HasMorePages() {
		page, err := p.NextPage(ctx)
		if err != nil {
			return
		}

		for i := range page.Contents {
			_, _ = client.DeleteObject(ctx, &awss3.DeleteObjectInput{Bucket: aws.String(bucket), Key: page.Contents[i].Key})
		}
	}
}
