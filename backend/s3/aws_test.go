package s3_test

import (
	"bytes"
	"context"
	"io"
	"slices"
	"strconv"
	"strings"
	"sync"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	awss3 "github.com/aws/aws-sdk-go-v2/service/s3"
	s3types "github.com/aws/aws-sdk-go-v2/service/s3/types"
	smithy "github.com/aws/smithy-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/backend"
	"github.com/oteldb/storage/backend/backendtest"
	"github.com/oteldb/storage/backend/s3"
)

// fakeAWS is a faithful in-memory simulation of the S3 operations the adapter calls: it
// returns the real smithy API errors (NoSuchKey/NotFound/PreconditionFailed), honors the
// If-None-Match conditional put, and paginates ListObjectsV2 (small page size) so the
// adapter's pagination and error-translation are exercised. apiErr small helper below.
type fakeAWS struct {
	mu   sync.Mutex
	objs map[string][]byte
}

func newFakeAWS() *fakeAWS { return &fakeAWS{objs: make(map[string][]byte)} }

func apiErr(code string) error { return &smithy.GenericAPIError{Code: code, Message: code} }

func (f *fakeAWS) GetObject(_ context.Context, in *awss3.GetObjectInput, _ ...func(*awss3.Options)) (*awss3.GetObjectOutput, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	v, ok := f.objs[*in.Key]
	if !ok {
		return nil, apiErr("NoSuchKey")
	}

	return &awss3.GetObjectOutput{Body: io.NopCloser(bytes.NewReader(clone(v)))}, nil
}

func (f *fakeAWS) PutObject(_ context.Context, in *awss3.PutObjectInput, _ ...func(*awss3.Options)) (*awss3.PutObjectOutput, error) {
	data, err := io.ReadAll(in.Body)
	if err != nil {
		return nil, err
	}

	f.mu.Lock()
	defer f.mu.Unlock()

	if in.IfNoneMatch != nil {
		if _, exists := f.objs[*in.Key]; exists {
			return nil, apiErr("PreconditionFailed")
		}
	}

	f.objs[*in.Key] = clone(data)

	return &awss3.PutObjectOutput{}, nil
}

func (f *fakeAWS) HeadObject(_ context.Context, in *awss3.HeadObjectInput, _ ...func(*awss3.Options)) (*awss3.HeadObjectOutput, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	if _, ok := f.objs[*in.Key]; !ok {
		return nil, apiErr("NotFound")
	}

	return &awss3.HeadObjectOutput{}, nil
}

func (f *fakeAWS) DeleteObject(_ context.Context, in *awss3.DeleteObjectInput, _ ...func(*awss3.Options)) (*awss3.DeleteObjectOutput, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	delete(f.objs, *in.Key) // idempotent

	return &awss3.DeleteObjectOutput{}, nil
}

func (f *fakeAWS) ListObjectsV2(_ context.Context, in *awss3.ListObjectsV2Input, _ ...func(*awss3.Options)) (*awss3.ListObjectsV2Output, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	prefix := ""
	if in.Prefix != nil {
		prefix = *in.Prefix
	}

	var keys []string
	for k := range f.objs {
		if strings.HasPrefix(k, prefix) {
			keys = append(keys, k)
		}
	}
	slices.Sort(keys)

	// Paginate at a small page size to exercise the adapter's paginator loop.
	const pageSize = 2

	start := 0
	if in.ContinuationToken != nil {
		start, _ = strconv.Atoi(*in.ContinuationToken)
	}

	end := min(start+pageSize, len(keys))
	out := &awss3.ListObjectsV2Output{}
	for _, k := range keys[start:end] {
		out.Contents = append(out.Contents, s3types.Object{Key: aws.String(k)})
	}

	if end < len(keys) {
		out.IsTruncated = aws.Bool(true)
		out.NextContinuationToken = aws.String(strconv.Itoa(end))
	}

	return out, nil
}

func TestAWSAdapterConformance(t *testing.T) {
	t.Parallel()
	backendtest.Run(t, func(*testing.T) backend.Backend {
		return s3.New(s3.NewAWS(newFakeAWS(), "bucket"), "oteldb/")
	})
}

func TestAWSAdapterPaginationSpansPages(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	b := s3.New(s3.NewAWS(newFakeAWS(), "bucket"), "")

	// Write more keys than one page (pageSize=2) to force the paginator across pages.
	want := make([]string, 0, 5)
	for i := range 5 {
		k := "p/" + strconv.Itoa(i)
		require.NoError(t, b.Write(ctx, k, []byte("v")))
		want = append(want, k)
	}

	got, err := b.List(ctx, "p/")
	require.NoError(t, err)
	slices.Sort(want)
	assert.Equal(t, want, got, "all keys returned across paginator pages")
}
