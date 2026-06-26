package storage_test

// The M5 exit check: a full vertical on S3. The storage facade runs over the s3 backend
// (an in-process go-faster/fs server, no Docker); one process ingests and flushes to the
// object store, then a fresh process — new Storage, new engines, nothing shared — recovers
// from the bucket alone and serves the data through the PromQL adapter.

import (
	"context"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	awss3 "github.com/aws/aws-sdk-go-v2/service/s3"
	fsserver "github.com/go-faster/fs/server"
	"github.com/go-faster/fs/storagemem"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage"
	"github.com/oteldb/storage/backend"
	"github.com/oteldb/storage/backend/s3"
	"github.com/oteldb/storage/signal/metric"
)

// s3Backend starts an in-process S3 server and returns a fresh durable backend over it. The
// returned backends share the same bucket, so two Storage instances over them see the same
// objects — modeling two processes against one object store.
func s3Backend(t *testing.T) func() backend.Backend {
	t.Helper()

	const bucket = "oteldb"
	store := storagemem.New()
	require.NoError(t, store.CreateBucket(context.Background(), bucket))

	srv := httptest.NewServer(fsserver.NewHandler(store))
	t.Cleanup(srv.Close)

	client := awss3.New(awss3.Options{
		Region:       "us-east-1",
		BaseEndpoint: aws.String(srv.URL),
		UsePathStyle: true,
		Credentials:  credentials.NewStaticCredentialsProvider("test", "test", ""),
	})

	return func() backend.Backend { return s3.New(s3.NewAWS(client, bucket), "") }
}

func TestFullVerticalOnS3(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	newBackend := s3Backend(t)

	// Process 1: ingest and flush to the object store, then close.
	s1, err := storage.Open(ctx, storage.Options{}, storage.WithBackend(newBackend()))
	require.NoError(t, err)
	writeSeries(t, s1, "http_requests", metric.KindGauge, map[string][]smpl{
		"/a": {{1000, 1}, {1010, 2}, {1020, 3}},
		"/b": {{1000, 10}, {1010, 20}},
	})
	require.NoError(t, s1.Close(ctx)) // flushes parts + indexes to S3

	// Process 2: a fresh Storage over the same bucket recovers from S3 at Open and serves the
	// data — labels and values reconstructed from the object store, nothing shared in memory.
	s2, err := storage.Open(ctx, storage.Options{}, storage.WithBackend(newBackend()))
	require.NoError(t, err)
	t.Cleanup(func() { _ = s2.Close(ctx) })

	got := vectorByRoute(t, mustInstant(t, promEngine(), queryable(s2), "http_requests", time.Unix(0, 1020*sec)))
	assert.Equal(t, map[string]float64{"/a": 3, "/b": 20}, got, "S3-recovered instant vector")

	// An aggregation over the recovered series works too.
	total, err := mustInstant(t, promEngine(), queryable(s2), "sum(http_requests)", time.Unix(0, 1020*sec)).Vector()
	require.NoError(t, err)
	require.Len(t, total, 1)
	assert.InDelta(t, 23.0, total[0].F, 1e-9)
}
