package s3_test

import (
	"bytes"
	"context"
	"fmt"
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
	mu      sync.Mutex
	objs    map[string][]byte
	uploads map[string]map[int32][]byte
	nextID  int
	starts  int
	parts   int
}

func newFakeAWS() *fakeAWS {
	return &fakeAWS{objs: make(map[string][]byte), uploads: make(map[string]map[int32][]byte)}
}

func apiErr(code string) error { return &smithy.GenericAPIError{Code: code, Message: code} }

func (f *fakeAWS) GetObject(_ context.Context, in *awss3.GetObjectInput, _ ...func(*awss3.Options)) (*awss3.GetObjectOutput, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	v, ok := f.objs[*in.Key]
	if !ok {
		return nil, apiErr("NoSuchKey")
	}

	if in.Range != nil {
		var err error
		if v, err = sliceRange(v, *in.Range); err != nil {
			return nil, err
		}
	}

	return &awss3.GetObjectOutput{
		Body: io.NopCloser(bytes.NewReader(clone(v))),
		ETag: aws.String(`"` + etagOf(v) + `"`), // real S3 quotes the ETag; the adapter must unquote it
	}, nil
}

// sliceRange models S3's handling of an inclusive "bytes=lo-hi" header: the upper bound is clamped
// to the object, and a lower bound at or past its end is a 416. Without it the fake would answer
// every ranged GET with the whole object and the adapter's range arithmetic would go untested.
func sliceRange(v []byte, header string) ([]byte, error) {
	var lo, hi int64
	if _, err := fmt.Sscanf(header, "bytes=%d-%d", &lo, &hi); err != nil {
		return nil, apiErr("InvalidArgument")
	}

	if lo >= int64(len(v)) {
		return nil, apiErr("InvalidRange")
	}

	return v[lo:min(hi+1, int64(len(v)))], nil
}

func (f *fakeAWS) PutObject(_ context.Context, in *awss3.PutObjectInput, _ ...func(*awss3.Options)) (*awss3.PutObjectOutput, error) {
	data, err := io.ReadAll(in.Body)
	if err != nil {
		return nil, err
	}

	f.mu.Lock()
	defer f.mu.Unlock()

	current, exists := f.objs[*in.Key]

	if in.IfNoneMatch != nil && exists {
		return nil, apiErr("PreconditionFailed")
	}

	// S3 answers an If-Match against an absent key with 404, not 412.
	if in.IfMatch != nil {
		if !exists {
			return nil, apiErr("NoSuchKey")
		}

		if strings.Trim(*in.IfMatch, `"`) != etagOf(current) {
			return nil, apiErr("PreconditionFailed")
		}
	}

	stored := clone(data)
	f.objs[*in.Key] = stored

	return &awss3.PutObjectOutput{ETag: aws.String(`"` + etagOf(stored) + `"`)}, nil
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

// uploadKey names an in-progress upload by key and id, so two uploads racing the same key stay
// distinct the way real S3 keeps them.
func uploadKey(key, uploadID string) string { return key + "\x00" + uploadID }

func (f *fakeAWS) CreateMultipartUpload(
	_ context.Context, in *awss3.CreateMultipartUploadInput, _ ...func(*awss3.Options),
) (*awss3.CreateMultipartUploadOutput, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	f.nextID++
	f.starts++
	id := strconv.Itoa(f.nextID)
	f.uploads[uploadKey(*in.Key, id)] = make(map[int32][]byte)

	return &awss3.CreateMultipartUploadOutput{UploadId: aws.String(id)}, nil
}

func (f *fakeAWS) UploadPart(
	_ context.Context, in *awss3.UploadPartInput, _ ...func(*awss3.Options),
) (*awss3.UploadPartOutput, error) {
	data, err := io.ReadAll(in.Body)
	if err != nil {
		return nil, err
	}

	f.mu.Lock()
	defer f.mu.Unlock()

	parts, ok := f.uploads[uploadKey(*in.Key, *in.UploadId)]
	if !ok {
		return nil, apiErr("NoSuchUpload")
	}

	stored := clone(data)
	parts[*in.PartNumber] = stored
	f.parts++

	return &awss3.UploadPartOutput{ETag: aws.String(`"` + etagOf(stored) + `"`)}, nil
}

func (f *fakeAWS) CompleteMultipartUpload(
	_ context.Context, in *awss3.CompleteMultipartUploadInput, _ ...func(*awss3.Options),
) (*awss3.CompleteMultipartUploadOutput, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	uk := uploadKey(*in.Key, *in.UploadId)

	parts, ok := f.uploads[uk]
	if !ok {
		return nil, apiErr("NoSuchUpload")
	}

	var obj []byte
	for _, p := range in.MultipartUpload.Parts {
		data, ok := parts[*p.PartNumber]
		if !ok {
			return nil, apiErr("InvalidPart")
		}

		// The ETag is the part's identity: a complete naming one the part does not carry is how a
		// mismatched or re-uploaded part is caught, so the fake must check it too.
		if strings.Trim(*p.ETag, `"`) != etagOf(data) {
			return nil, apiErr("InvalidPart")
		}

		obj = append(obj, data...)
	}

	delete(f.uploads, uk)
	f.objs[*in.Key] = obj

	return &awss3.CompleteMultipartUploadOutput{ETag: aws.String(`"` + etagOf(obj) + `"`)}, nil
}

func (f *fakeAWS) AbortMultipartUpload(
	_ context.Context, in *awss3.AbortMultipartUploadInput, _ ...func(*awss3.Options),
) (*awss3.AbortMultipartUploadOutput, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	uk := uploadKey(*in.Key, *in.UploadId)
	if _, ok := f.uploads[uk]; !ok {
		return nil, apiErr("NoSuchUpload")
	}

	delete(f.uploads, uk)

	return &awss3.AbortMultipartUploadOutput{}, nil
}

// pendingUploads reports how many uploads were started and neither completed nor aborted. A
// streamed write that leaves one behind is billed storage no listing can find.
func (f *fakeAWS) pendingUploads() int {
	f.mu.Lock()
	defer f.mu.Unlock()

	return len(f.uploads)
}

// startedUploads and uploadedParts count what the writer actually did, which is what separates a
// streamed object from one the writer held whole and put in a single request.
func (f *fakeAWS) startedUploads() int {
	f.mu.Lock()
	defer f.mu.Unlock()

	return f.starts
}

func (f *fakeAWS) uploadedParts() int {
	f.mu.Lock()
	defer f.mu.Unlock()

	return f.parts
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
