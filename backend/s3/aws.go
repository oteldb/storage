package s3

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"strings"

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

// GetObjectRange fetches the object's [off, off+n) bytes with an HTTP Range header. S3 answers a
// range starting past the object's end with 416, which under the clamping contract is an empty
// result rather than an error. Implements [RangeObjectStore].
func (s *awsStore) GetObjectRange(ctx context.Context, key string, off, n int64) ([]byte, error) {
	if n == 0 {
		return []byte{}, nil
	}

	// HTTP byte ranges are inclusive on both ends, and the server clamps the upper bound to the
	// object, so asking past the end is how a caller takes a trailer without knowing the size.
	rng := fmt.Sprintf("bytes=%d-%d", off, off+n-1)

	out, err := s.api.GetObject(ctx, &awss3.GetObjectInput{Bucket: &s.bucket, Key: &key, Range: &rng})
	if err != nil {
		switch {
		case isNotFound(err):
			return nil, errors.Wrapf(ErrObjectNotFound, "get %q", key)
		case isRangeNotSatisfiable(err):
			return []byte{}, nil
		default:
			return nil, errors.Wrapf(err, "get %q range %s", key, rng)
		}
	}
	defer func() { _ = out.Body.Close() }()

	data, err := io.ReadAll(out.Body)
	if err != nil {
		return nil, errors.Wrapf(err, "read body %q range %s", key, rng)
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
	// If-None-Match: * makes the PUT succeed only if no object exists at the key; a lost
	// condition (412, or 409 against a concurrent writer) means another writer won the race.
	_, err := s.api.PutObject(ctx, &awss3.PutObjectInput{
		Bucket: &s.bucket, Key: &key, Body: bytes.NewReader(data), IfNoneMatch: aws.String("*"),
	})
	if err != nil {
		if isConditionLost(err) {
			return false, nil
		}

		return false, errors.Wrapf(err, "put-if-absent %q", key)
	}

	return true, nil
}

// GetObjectVersion fetches the object together with its ETag, the token the conditional PUT
// below matches on.
func (s *awsStore) GetObjectVersion(ctx context.Context, key string) ([]byte, string, error) {
	out, err := s.api.GetObject(ctx, &awss3.GetObjectInput{Bucket: &s.bucket, Key: &key})
	if err != nil {
		if isNotFound(err) {
			return nil, "", errors.Wrapf(ErrObjectNotFound, "get %q", key)
		}

		return nil, "", errors.Wrapf(err, "get %q", key)
	}
	defer func() { _ = out.Body.Close() }()

	data, err := io.ReadAll(out.Body)
	if err != nil {
		return nil, "", errors.Wrapf(err, "read body %q", key)
	}

	return data, unquoteETag(out.ETag), nil
}

// PutObjectIfVersion is the conditional replace: If-Match on the ETag the committer read, or
// If-None-Match: * when it expects no object at all. S3 evaluates the precondition atomically
// with the write, so concurrent committers resolve to one winner; the losers get 412
// PreconditionFailed, 409 ConditionalRequestConflict when they raced a concurrent write to the
// key, or 404 NoSuchKey when they matched an ETag on a key that has since been deleted.
func (s *awsStore) PutObjectIfVersion(
	ctx context.Context, key string, data []byte, etag string,
) (string, bool, error) {
	in := &awss3.PutObjectInput{Bucket: &s.bucket, Key: &key, Body: bytes.NewReader(data)}
	if etag == "" {
		in.IfNoneMatch = aws.String("*")
	} else {
		in.IfMatch = aws.String(quoteETag(etag))
	}

	out, err := s.api.PutObject(ctx, in)
	if err != nil {
		if isConditionLost(err) || isNotFound(err) {
			return "", false, nil
		}

		return "", false, errors.Wrapf(err, "put-if-version %q", key)
	}

	return unquoteETag(out.ETag), true, nil
}

// quoteETag renders an ETag as the conditional headers require it, quoted.
func quoteETag(etag string) string {
	if strings.HasPrefix(etag, `"`) {
		return etag
	}

	return `"` + etag + `"`
}

// unquoteETag strips the quoting S3 puts around an ETag, so the token stored as a
// [backend.Version] is stable however the SDK rendered it.
func unquoteETag(etag *string) string {
	if etag == nil {
		return ""
	}

	return strings.Trim(*etag, `"`)
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

// isRangeNotSatisfiable reports whether err is a 416 — a range starting at or past the object's
// end, which the clamping contract treats as "no bytes there", not as a failure.
func isRangeNotSatisfiable(err error) bool {
	var ae smithy.APIError
	if errors.As(err, &ae) {
		return ae.ErrorCode() == "InvalidRange" || ae.ErrorCode() == "RequestedRangeNotSatisfiable"
	}

	return false
}

// isConditionLost reports whether err says a conditional PUT's precondition did not hold. S3 has
// two answers for that: 412 PreconditionFailed when the condition was evaluated and failed, and
// 409 ConditionalRequestConflict when a concurrent write to the same key raced this one. Both mean
// this writer did not win and must re-read; only the 412 is reachable when nothing else is
// writing, which is why the 409 is easy to miss until a second writer appears.
//
// 409 OperationAborted is deliberately not here: it reports a conflicting operation still in
// progress rather than a lost precondition, so the remedy is to retry the same request, not to
// tell the caller another writer owns the key.
func isConditionLost(err error) bool {
	var ae smithy.APIError
	if errors.As(err, &ae) {
		switch ae.ErrorCode() {
		case "PreconditionFailed", "ConditionalRequestConflict":
			return true
		}
	}

	return false
}
