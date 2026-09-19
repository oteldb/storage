package s3_test

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/go-faster/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/backend"
	"github.com/oteldb/storage/backend/s3"
	"github.com/oteldb/storage/reliability"
)

var errFault = errors.New("transient fault")

// faultStore is an [s3.ObjectStore] that injects latency and failures to exercise the retry/hedge
// wrapper: the first GET may stall (getDelay), the first getFails GETs fail transiently, and CAS can
// return a scripted error. It otherwise behaves like a tiny in-memory store.
type faultStore struct {
	mu       sync.Mutex
	objs     map[string][]byte
	getN     atomic.Int32
	casN     atomic.Int32
	getDelay time.Duration // applied on the first GET only
	getFails int           // first N GETs fail transiently
	notFound bool          // every GET reports not-found
	casErr   error         // error returned by every PutObjectIfAbsent
}

func newFaultStore() *faultStore { return &faultStore{objs: map[string][]byte{}} }

func (f *faultStore) GetObject(ctx context.Context, key string) ([]byte, error) {
	n := f.getN.Add(1)

	if f.getDelay > 0 && n == 1 {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(f.getDelay):
		}
	}

	if int(n) <= f.getFails {
		return nil, errFault
	}

	if f.notFound {
		return nil, s3.ErrObjectNotFound
	}

	f.mu.Lock()
	defer f.mu.Unlock()

	if v, ok := f.objs[key]; ok {
		return v, nil
	}

	return nil, s3.ErrObjectNotFound
}

func (f *faultStore) PutObject(_ context.Context, key string, data []byte) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.objs[key] = data

	return nil
}

func (f *faultStore) PutObjectIfAbsent(_ context.Context, key string, data []byte) (bool, error) {
	f.casN.Add(1)

	if f.casErr != nil {
		return false, f.casErr
	}

	f.mu.Lock()
	defer f.mu.Unlock()

	if _, ok := f.objs[key]; ok {
		return false, nil
	}

	f.objs[key] = data

	return true, nil
}

func (f *faultStore) GetObjectVersion(ctx context.Context, key string) ([]byte, string, error) {
	data, err := f.GetObject(ctx, key)
	if err != nil {
		return nil, "", err
	}

	return data, "v", nil
}

func (f *faultStore) PutObjectIfVersion(
	_ context.Context, key string, data []byte, _ string,
) (string, bool, error) {
	f.casN.Add(1)

	if f.casErr != nil {
		return "", false, f.casErr
	}

	f.mu.Lock()
	defer f.mu.Unlock()
	f.objs[key] = data

	return "v", true, nil
}

func (f *faultStore) HeadObject(_ context.Context, key string) (bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	_, ok := f.objs[key]

	return ok, nil
}

func (f *faultStore) DeleteObject(_ context.Context, key string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	delete(f.objs, key)

	return nil
}

func (f *faultStore) ListObjects(_ context.Context, _ string) ([]string, error) { return nil, nil }

// TestS3GetHedgesSlowRead: a stuck first GET is bypassed by the hedge and a fresh re-issue wins, so
// the read returns promptly instead of waiting out the slow request.
func TestS3GetHedgesSlowRead(t *testing.T) {
	t.Parallel()

	fs := newFaultStore()
	fs.objs["oteldb/k"] = []byte("value")
	fs.getDelay = 3 * time.Second

	b := s3.New(fs, "oteldb/", s3.WithRetry(reliability.RetryConfig{
		MaxAttempts: 3, PerTryTimeout: 5 * time.Second, HedgeDelay: 30 * time.Millisecond,
	}))

	start := time.Now()
	got, err := b.Read(context.Background(), "k")

	require.NoError(t, err)
	assert.Equal(t, []byte("value"), got)
	assert.Less(t, time.Since(start), time.Second, "hedge bypassed the stuck first GET")
	assert.GreaterOrEqual(t, fs.getN.Load(), int32(2), "a hedged re-issue was fired")
}

// TestS3GetRetriesTransient: transient GET failures are retried until one succeeds.
func TestS3GetRetriesTransient(t *testing.T) {
	t.Parallel()

	fs := newFaultStore()
	fs.objs["oteldb/k"] = []byte("v")
	fs.getFails = 2 // first two GETs fail, third succeeds

	b := s3.New(fs, "oteldb/", s3.WithRetry(reliability.RetryConfig{MaxAttempts: 3, PerTryTimeout: time.Second}))

	got, err := b.Read(context.Background(), "k")
	require.NoError(t, err)
	assert.Equal(t, []byte("v"), got)
	assert.Equal(t, int32(3), fs.getN.Load())
}

// TestS3GetNotFoundNotRetried: a genuine not-found is permanent — it must not be retried.
func TestS3GetNotFoundNotRetried(t *testing.T) {
	t.Parallel()

	fs := newFaultStore()
	fs.notFound = true

	b := s3.New(fs, "oteldb/", s3.WithRetry(reliability.RetryConfig{MaxAttempts: 4, PerTryTimeout: time.Second, HedgeDelay: time.Hour}))

	_, err := b.Read(context.Background(), "missing")
	require.ErrorIs(t, err, backend.ErrNotExist)
	assert.Equal(t, int32(1), fs.getN.Load(), "not-found short-circuits, no retry")
}

// TestS3CASNotRetriedOnAmbiguous: a conditional put is retried only when the request provably never
// reached the server; an ambiguous error (it may have applied) is not retried, preserving CAS.
func TestS3CASNotRetriedOnAmbiguous(t *testing.T) {
	t.Parallel()

	fs := newFaultStore()
	fs.casErr = context.DeadlineExceeded // ambiguous: the put may have succeeded server-side

	b := s3.New(fs, "oteldb/", s3.WithRetry(reliability.RetryConfig{MaxAttempts: 4, PerTryTimeout: time.Second}))

	_, err := b.PutIfAbsent(context.Background(), "k", []byte("v"))
	require.Error(t, err)
	assert.Equal(t, int32(1), fs.casN.Load(), "ambiguous CAS failure is not retried")
}

// TestS3DisabledByDefault: without WithRetry, the bare store is used (no extra attempts).
func TestS3DisabledByDefault(t *testing.T) {
	t.Parallel()

	fs := newFaultStore()
	fs.getFails = 1

	b := s3.New(fs, "oteldb/")
	_, err := b.Read(context.Background(), "k")
	require.Error(t, err, "no retry wrapper ⇒ the single transient failure surfaces")
	assert.Equal(t, int32(1), fs.getN.Load())
}

// rangeFaultStore is a [faultStore] that also serves ranged reads, so a test can tell a real ranged
// read from the whole-object fallback [s3.Backend.ReadAt] uses when the store cannot range.
type rangeFaultStore struct {
	*faultStore

	rangeN     atomic.Int32
	rangeFails int // first N ranged reads fail transiently
}

func newRangeFaultStore() *rangeFaultStore {
	return &rangeFaultStore{faultStore: newFaultStore()}
}

func (f *rangeFaultStore) GetObjectRange(_ context.Context, key string, off, n int64) ([]byte, error) {
	if int(f.rangeN.Add(1)) <= f.rangeFails {
		return nil, errFault
	}

	f.mu.Lock()
	defer f.mu.Unlock()

	v, ok := f.objs[key]
	if !ok {
		return nil, s3.ErrObjectNotFound
	}

	if off >= int64(len(v)) {
		return []byte{}, nil
	}

	return v[off:min(off+n, int64(len(v)))], nil
}

// TestS3RetryForwardsRangedReads is the regression guard for a wrapper that silently drops an
// optional capability. WithRetry replaces the store, so a wrapper without GetObjectRange makes
// ReadAt fall back to reading the whole object — correct, but it defeats every ranged read on the
// query and merge paths, with nothing to show for it.
func TestS3RetryForwardsRangedReads(t *testing.T) {
	t.Parallel()

	fs := newRangeFaultStore()
	fs.objs["oteldb/k"] = []byte("0123456789")

	b := s3.New(fs, "oteldb/", s3.WithRetry(reliability.RetryConfig{MaxAttempts: 3, PerTryTimeout: time.Second}))

	got, err := backend.ReadAt(context.Background(), b, "k", 3, 4)
	require.NoError(t, err)
	assert.Equal(t, []byte("3456"), got)
	assert.Equal(t, int32(1), fs.rangeN.Load(), "served by a real ranged read")
	assert.Zero(t, fs.getN.Load(), "the whole object was never fetched")
}

// TestS3RetryRetriesRangedReads: a forwarded ranged read gets the same retry policy as a whole GET.
func TestS3RetryRetriesRangedReads(t *testing.T) {
	t.Parallel()

	fs := newRangeFaultStore()
	fs.objs["oteldb/k"] = []byte("0123456789")
	fs.rangeFails = 2

	b := s3.New(fs, "oteldb/", s3.WithRetry(reliability.RetryConfig{MaxAttempts: 3, PerTryTimeout: time.Second}))

	got, err := backend.ReadAt(context.Background(), b, "k", 0, 2)
	require.NoError(t, err)
	assert.Equal(t, []byte("01"), got)
	assert.Equal(t, int32(3), fs.rangeN.Load())
}

// TestS3RetryDoesNotInventRangedReads: the mirror image — the wrapper must not advertise a
// capability its inner store lacks, or ReadAt would route to a method that cannot serve it.
func TestS3RetryDoesNotInventRangedReads(t *testing.T) {
	t.Parallel()

	fs := newFaultStore() // no GetObjectRange
	fs.objs["oteldb/k"] = []byte("0123456789")

	b := s3.New(fs, "oteldb/", s3.WithRetry(reliability.RetryConfig{MaxAttempts: 2, PerTryTimeout: time.Second}))

	got, err := backend.ReadAt(context.Background(), b, "k", 3, 4)
	require.NoError(t, err)
	assert.Equal(t, []byte("3456"), got)
	assert.Positive(t, fs.getN.Load(), "fell back to the whole object, as it must")
}
