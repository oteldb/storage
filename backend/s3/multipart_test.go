package s3_test

import (
	"context"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awss3 "github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/go-faster/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/backend"
	"github.com/oteldb/storage/backend/backendtest"
	"github.com/oteldb/storage/backend/s3"
	"github.com/oteldb/storage/reliability"
)

// streamedBytes is comfortably past the writer's part threshold, so a streamed write of it starts
// an upload and sends more than one part. The tests use the real threshold rather than a lowered
// one: a knob only tests would ever move is a second configuration to keep honest.
const streamedBytes = 20 << 20

func payload(n int) []byte {
	b := make([]byte, n)
	for i := range b {
		b[i] = byte(i*7 + 1)
	}

	return b
}

// writeStreamed appends want to a writer for key in small pieces, the way a streamed column's
// compression frames arrive.
func writeStreamed(t *testing.T, w backend.ObjectWriter, want []byte) {
	t.Helper()

	const frame = 64 << 10

	for off := 0; off < len(want); off += frame {
		n, err := w.Write(want[off:min(off+frame, len(want))])
		require.NoError(t, err)
		require.Equal(t, min(frame, len(want)-off), n)
	}
}

func TestMultipartConformance(t *testing.T) {
	t.Parallel()
	backendtest.Run(t, func(*testing.T) backend.Backend {
		return s3.New(s3.NewAWS(newFakeAWS(), "bucket"), "oteldb/")
	})
}

// TestStreamsWritesFollowsTheStore is the capability rule: the Backend may claim
// [backend.ObjectCreator] only when the store beneath can actually upload in parts, because a
// merge sizes its output part against that answer.
func TestStreamsWritesFollowsTheStore(t *testing.T) {
	t.Parallel()

	assert.False(t, backend.StreamsWrites(s3.New(newFakeStore(), "")),
		"a store without multipart buffers whole objects and must say so")
	assert.True(t, backend.StreamsWrites(s3.New(s3.NewAWS(newFakeAWS(), "bucket"), "")))

	retryCfg := reliability.RetryConfig{MaxAttempts: 3, PerTryTimeout: time.Second}

	assert.False(t, backend.StreamsWrites(s3.New(newFakeStore(), "", s3.WithRetry(retryCfg))))
	assert.True(t, backend.StreamsWrites(s3.New(s3.NewAWS(newFakeAWS(), "bucket"), "", s3.WithRetry(retryCfg))),
		"the retry wrapper must forward the capability, not swallow it")
}

func TestMultipartStreamsLargeObject(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	api := newFakeAWS()
	b := s3.New(s3.NewAWS(api, "bucket"), "oteldb/")
	want := payload(streamedBytes)

	w, err := backend.CreateObject(ctx, b, "part/col")
	require.NoError(t, err)
	defer w.Abort()

	writeStreamed(t, w, want)

	_, err = b.Read(ctx, "part/col")
	require.ErrorIs(t, err, backend.ErrNotExist, "an uncompleted upload is not an object")

	require.NoError(t, w.Commit(ctx))

	got, err := b.Read(ctx, "part/col")
	require.NoError(t, err)
	assert.Equal(t, want, got)

	assert.Equal(t, 1, api.startedUploads())
	assert.Equal(t, 3, api.uploadedParts(), "two full parts plus the remainder")
	assert.Zero(t, api.pendingUploads(), "a committed upload leaves nothing in progress")
}

// TestMultipartSmallObjectSkipsUpload pins the degenerate path: everything a part writes but its
// big columns is far under the threshold, and starting an upload for each would multiply the
// request count of a flush by three for no benefit.
func TestMultipartSmallObjectSkipsUpload(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	api := newFakeAWS()
	b := s3.New(s3.NewAWS(api, "bucket"), "oteldb/")
	want := payload(64 << 10)

	w, err := backend.CreateObject(ctx, b, "part/marks")
	require.NoError(t, err)

	writeStreamed(t, w, want)
	require.NoError(t, w.Commit(ctx))

	got, err := b.Read(ctx, "part/marks")
	require.NoError(t, err)
	assert.Equal(t, want, got)

	assert.Zero(t, api.startedUploads(), "an object under the part threshold commits as one put")
}

func TestMultipartAbortDiscardsUpload(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	api := newFakeAWS()
	b := s3.New(s3.NewAWS(api, "bucket"), "oteldb/")

	w, err := backend.CreateObject(ctx, b, "part/col")
	require.NoError(t, err)

	writeStreamed(t, w, payload(streamedBytes))
	require.Positive(t, api.startedUploads(), "the test needs an upload in flight to abort")

	w.Abort()
	w.Abort() // idempotent

	assert.Zero(t, api.pendingUploads(), "an aborted upload must not be left behind to be billed")

	_, err = b.Read(ctx, "part/col")
	require.ErrorIs(t, err, backend.ErrNotExist)

	require.Error(t, w.Commit(ctx), "an aborted writer must not publish a truncated object")
}

// TestMultipartAbortSurvivesCancellation is why the abort strips its context's cancellation. A
// merge that aborts is usually a merge whose context just died; without stripping it the abort
// never reaches the store and the parts stay until the bucket's lifecycle rule reaps them.
func TestMultipartAbortSurvivesCancellation(t *testing.T) {
	t.Parallel()

	api := newFakeAWS()
	b := s3.New(s3.NewAWS(api, "bucket"), "oteldb/")

	ctx, cancel := context.WithCancel(context.Background())

	w, err := backend.CreateObject(ctx, b, "part/col")
	require.NoError(t, err)

	writeStreamed(t, w, payload(streamedBytes))
	require.Positive(t, api.startedUploads())

	cancel()
	w.Abort()

	assert.Zero(t, api.pendingUploads(), "the abort must outlive the context that opened the writer")
}

// TestConditionalPutNeverUsesMultipart is mandatory rather than thorough: the conditional put is
// the commit point of the bucket index and of every manifest. A multipart complete carries no
// precondition, so routing one through it would turn a CAS into an unconditional overwrite —
// split-brain, not slowness.
func TestConditionalPutNeverUsesMultipart(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	api := newFakeAWS()
	b := s3.New(s3.NewAWS(api, "bucket"), "oteldb/")
	big := payload(streamedBytes)

	created, err := b.PutIfAbsent(ctx, "index", big)
	require.NoError(t, err)
	require.True(t, created)

	_, version, err := b.ReadVersioned(ctx, "index")
	require.NoError(t, err)

	_, ok, err := b.CompareAndSwap(ctx, "index", version, big)
	require.NoError(t, err)
	require.True(t, ok)

	assert.Zero(t, api.startedUploads(), "a conditional put must stay a single conditional request")
}

// TestMultipartWriteAfterCommit covers the writer's own guard: the buffer is released at commit, so
// a late write must fail rather than silently vanish.
func TestMultipartWriteAfterCommit(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	b := s3.New(s3.NewAWS(newFakeAWS(), "bucket"), "oteldb/")

	w, err := backend.CreateObject(ctx, b, "part/col")
	require.NoError(t, err)

	_, err = w.Write([]byte("kept"))
	require.NoError(t, err)
	require.NoError(t, w.Commit(ctx))

	_, err = w.Write([]byte("late"))
	require.Error(t, err)
	require.Error(t, w.Commit(ctx))

	got, err := b.Read(ctx, "part/col")
	require.NoError(t, err)
	assert.Equal(t, []byte("kept"), got)
}

// failingMultipartAWS scripts a failure into one of the multipart calls, so each of the writer's
// error paths is reachable. A nil-or-empty answer stands for a store that returns success without
// the token the next call needs.
type failingMultipartAWS struct {
	*fakeAWS

	createErr   error
	uploadErr   error
	completeErr error
	putErr      error
	abortErr    error
	emptyUpload bool
	emptyETag   bool
	nilUploadID bool

	// uploadFailsAfter makes UploadPart fail once this many parts have landed, so the failure hits
	// the last part — the one Commit flushes, not Write.
	uploadFailsAfter int
}

func (f *failingMultipartAWS) AbortMultipartUpload(
	ctx context.Context, in *awss3.AbortMultipartUploadInput, optFns ...func(*awss3.Options),
) (*awss3.AbortMultipartUploadOutput, error) {
	if f.abortErr != nil {
		return nil, f.abortErr
	}

	return f.fakeAWS.AbortMultipartUpload(ctx, in, optFns...)
}

func (f *failingMultipartAWS) PutObject(
	ctx context.Context, in *awss3.PutObjectInput, optFns ...func(*awss3.Options),
) (*awss3.PutObjectOutput, error) {
	if f.putErr != nil {
		return nil, f.putErr
	}

	return f.fakeAWS.PutObject(ctx, in, optFns...)
}

func (f *failingMultipartAWS) CreateMultipartUpload(
	ctx context.Context, in *awss3.CreateMultipartUploadInput, optFns ...func(*awss3.Options),
) (*awss3.CreateMultipartUploadOutput, error) {
	switch {
	case f.createErr != nil:
		return nil, f.createErr
	case f.nilUploadID:
		return &awss3.CreateMultipartUploadOutput{}, nil
	case f.emptyUpload:
		return &awss3.CreateMultipartUploadOutput{UploadId: aws.String("")}, nil
	}

	return f.fakeAWS.CreateMultipartUpload(ctx, in, optFns...)
}

func (f *failingMultipartAWS) UploadPart(
	ctx context.Context, in *awss3.UploadPartInput, optFns ...func(*awss3.Options),
) (*awss3.UploadPartOutput, error) {
	if f.uploadErr != nil {
		return nil, f.uploadErr
	}

	if f.uploadFailsAfter > 0 && f.uploadedParts() >= f.uploadFailsAfter {
		return nil, errors.New("boom")
	}

	out, err := f.fakeAWS.UploadPart(ctx, in, optFns...)
	if err == nil && f.emptyETag {
		out.ETag = nil
	}

	return out, err
}

func (f *failingMultipartAWS) CompleteMultipartUpload(
	ctx context.Context, in *awss3.CompleteMultipartUploadInput, optFns ...func(*awss3.Options),
) (*awss3.CompleteMultipartUploadOutput, error) {
	if f.completeErr != nil {
		return nil, f.completeErr
	}

	return f.fakeAWS.CompleteMultipartUpload(ctx, in, optFns...)
}

func TestMultipartWriterFailures(t *testing.T) {
	t.Parallel()

	boom := errors.New("boom")

	tests := []struct {
		name    string
		api     *failingMultipartAWS
		onWrite bool // the failure surfaces from Write rather than from Commit
		small   bool // stay under the part threshold, so the writer never starts an upload
	}{
		{name: "create fails", api: &failingMultipartAWS{createErr: boom}, onWrite: true},
		{name: "create returns no id", api: &failingMultipartAWS{nilUploadID: true}, onWrite: true},
		{name: "create returns an empty id", api: &failingMultipartAWS{emptyUpload: true}, onWrite: true},
		{name: "upload fails", api: &failingMultipartAWS{uploadErr: boom}, onWrite: true},
		{name: "upload returns no etag", api: &failingMultipartAWS{emptyETag: true}, onWrite: true},
		{name: "complete fails", api: &failingMultipartAWS{completeErr: boom}},
		{name: "the last part fails", api: &failingMultipartAWS{uploadFailsAfter: 2}},
		{name: "put fails", api: &failingMultipartAWS{putErr: boom}, small: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			ctx := context.Background()
			tt.api.fakeAWS = newFakeAWS()
			b := s3.New(s3.NewAWS(tt.api, "bucket"), "oteldb/")

			w, err := backend.CreateObject(ctx, b, "part/col")
			require.NoError(t, err)
			defer w.Abort()

			size := streamedBytes
			if tt.small {
				size = 1 << 10
			}

			want := payload(size)
			writeErr := writeAll(w, want)

			if tt.onWrite {
				require.Error(t, writeErr)

				return
			}

			require.NoError(t, writeErr)
			require.Error(t, w.Commit(ctx))

			_, err = b.Read(ctx, "part/col")
			require.ErrorIs(t, err, backend.ErrNotExist, "a failed commit publishes nothing")
		})
	}
}

// writeAll is [writeStreamed] for the failure cases, where the first error is the result rather
// than a fatal assertion.
func writeAll(w backend.ObjectWriter, data []byte) error {
	const frame = 64 << 10

	for off := 0; off < len(data); off += frame {
		if _, err := w.Write(data[off:min(off+frame, len(data))]); err != nil {
			return err
		}
	}

	return nil
}

// TestAbortReportsRealFailures is the other side of the unknown-upload translation: an abort that
// failed for any other reason is a genuine failure, and the store must say so even though the
// writer has nowhere to report it.
func TestAbortReportsRealFailures(t *testing.T) {
	t.Parallel()

	api := &failingMultipartAWS{fakeAWS: newFakeAWS(), abortErr: errors.New("boom")}

	store, ok := s3.NewAWS(api, "bucket").(s3.MultipartObjectStore)
	require.True(t, ok)

	ctx := context.Background()

	id, err := store.CreateMultipartUpload(ctx, "k")
	require.NoError(t, err)
	require.Error(t, store.AbortMultipartUpload(ctx, "k", id))
}

// TestAbortOfUnknownUploadIsNotAnError covers the adapter's one error translation: an upload the
// store no longer knows — already aborted, already completed, or reaped by the lifecycle rule —
// leaves nothing to abort, so reporting a failure would make cleanup paths noisy for no reason.
func TestAbortOfUnknownUploadIsNotAnError(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	store, ok := s3.NewAWS(newFakeAWS(), "bucket").(s3.MultipartObjectStore)
	require.True(t, ok)

	require.NoError(t, store.AbortMultipartUpload(ctx, "k", "never-existed"))

	id, err := store.CreateMultipartUpload(ctx, "k")
	require.NoError(t, err)
	require.NoError(t, store.AbortMultipartUpload(ctx, "k", id))
	require.NoError(t, store.AbortMultipartUpload(ctx, "k", id), "abort is idempotent")
}
