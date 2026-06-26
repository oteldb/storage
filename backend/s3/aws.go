package s3

import (
	"bytes"
	"context"
	"io"

	"github.com/aws/aws-sdk-go-v2/aws"
	awss3 "github.com/aws/aws-sdk-go-v2/service/s3"
	smithy "github.com/aws/smithy-go"
	"github.com/go-faster/errors"
)

// AWSAPI is the subset of the aws-sdk-go-v2 *s3.Client the [NewAWS] adapter uses. The real
// client satisfies it; tests can fake it. It also satisfies s3.ListObjectsV2APIClient (used
// by the paginator).
type AWSAPI interface {
	GetObject(ctx context.Context, in *awss3.GetObjectInput, optFns ...func(*awss3.Options)) (*awss3.GetObjectOutput, error)
	PutObject(ctx context.Context, in *awss3.PutObjectInput, optFns ...func(*awss3.Options)) (*awss3.PutObjectOutput, error)
	HeadObject(ctx context.Context, in *awss3.HeadObjectInput, optFns ...func(*awss3.Options)) (*awss3.HeadObjectOutput, error)
	DeleteObject(ctx context.Context, in *awss3.DeleteObjectInput, optFns ...func(*awss3.Options)) (*awss3.DeleteObjectOutput, error)
	ListObjectsV2(ctx context.Context, in *awss3.ListObjectsV2Input, optFns ...func(*awss3.Options)) (*awss3.ListObjectsV2Output, error)
}

// NewAWS returns an [ObjectStore] backed by an aws-sdk-go-v2 S3 client over the given bucket.
// Compose it with [New] to get a [backend.Backend]:
//
//	store := s3.NewAWS(awss3.NewFromConfig(cfg), "my-bucket")
//	b := s3.New(store, "oteldb/")
//
// This adapter is verified by compilation and exercised against real/MinIO S3 in
// integration tests; the Backend's contract logic is covered by the conformance suite over
// the in-memory fake.
func NewAWS(api AWSAPI, bucket string) ObjectStore {
	return &awsStore{api: api, bucket: bucket}
}

type awsStore struct {
	api    AWSAPI
	bucket string
}

func (s *awsStore) GetObject(ctx context.Context, key string) ([]byte, error) {
	out, err := s.api.GetObject(ctx, &awss3.GetObjectInput{Bucket: &s.bucket, Key: &key})
	if err != nil {
		if isNotFound(err) {
			return nil, errors.Wrapf(ErrObjectNotFound, "get %q", key)
		}

		return nil, errors.Wrapf(err, "get %q", key)
	}
	defer func() { _ = out.Body.Close() }()

	data, err := io.ReadAll(out.Body)
	if err != nil {
		return nil, errors.Wrapf(err, "read body %q", key)
	}

	return data, nil
}

func (s *awsStore) PutObject(ctx context.Context, key string, data []byte) error {
	if _, err := s.api.PutObject(ctx, &awss3.PutObjectInput{
		Bucket: &s.bucket, Key: &key, Body: bytes.NewReader(data),
	}); err != nil {
		return errors.Wrapf(err, "put %q", key)
	}

	return nil
}

func (s *awsStore) PutObjectIfAbsent(ctx context.Context, key string, data []byte) (bool, error) {
	// If-None-Match: * makes the PUT succeed only if no object exists at the key; a 412
	// PreconditionFailed means another writer won the race.
	_, err := s.api.PutObject(ctx, &awss3.PutObjectInput{
		Bucket: &s.bucket, Key: &key, Body: bytes.NewReader(data), IfNoneMatch: aws.String("*"),
	})
	if err != nil {
		if isPreconditionFailed(err) {
			return false, nil
		}

		return false, errors.Wrapf(err, "put-if-absent %q", key)
	}

	return true, nil
}

func (s *awsStore) HeadObject(ctx context.Context, key string) (bool, error) {
	_, err := s.api.HeadObject(ctx, &awss3.HeadObjectInput{Bucket: &s.bucket, Key: &key})
	if err != nil {
		if isNotFound(err) {
			return false, nil
		}

		return false, errors.Wrapf(err, "head %q", key)
	}

	return true, nil
}

func (s *awsStore) DeleteObject(ctx context.Context, key string) error {
	if _, err := s.api.DeleteObject(ctx, &awss3.DeleteObjectInput{Bucket: &s.bucket, Key: &key}); err != nil {
		return errors.Wrapf(err, "delete %q", key)
	}

	return nil
}

func (s *awsStore) ListObjects(ctx context.Context, prefix string) ([]string, error) {
	p := awss3.NewListObjectsV2Paginator(s.api, &awss3.ListObjectsV2Input{Bucket: &s.bucket, Prefix: &prefix})

	var keys []string

	for p.HasMorePages() {
		page, err := p.NextPage(ctx)
		if err != nil {
			return nil, errors.Wrapf(err, "list %q", prefix)
		}

		for i := range page.Contents {
			if k := page.Contents[i].Key; k != nil {
				keys = append(keys, *k)
			}
		}
	}

	return keys, nil
}

// isNotFound reports whether err is an S3 "absent object" error (GET/HEAD on a missing key).
func isNotFound(err error) bool {
	var ae smithy.APIError
	if errors.As(err, &ae) {
		switch ae.ErrorCode() {
		case "NoSuchKey", "NotFound":
			return true
		}
	}

	return false
}

// isPreconditionFailed reports whether err is a 412 from a conditional (If-None-Match) PUT.
func isPreconditionFailed(err error) bool {
	var ae smithy.APIError
	if errors.As(err, &ae) {
		return ae.ErrorCode() == "PreconditionFailed"
	}

	return false
}
