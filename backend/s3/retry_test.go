package s3_test

import (
	"context"
	"slices"
	"strconv"
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
// return a scripted error. Unlike [fakeStore] it stores and returns values without copying, versions
// every object "v", and lists nothing; Head and Delete are fakeStore's.
type faultStore struct {
	*fakeStore

	getN     atomic.Int32
	casN     atomic.Int32
	getDelay time.Duration // applied on the first GET only
	getFails int           // first N GETs fail transiently
	notFound bool          // every GET reports not-found
	casErr   error         // error returned by every PutObjectIfAbsent
}

func newFaultStore() *faultStore { return &faultStore{fakeStore: newFakeStore()} }

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

// multipartParts is the [s3.MultipartObjectStore] half of a fault store, kept free of the store it
// is composed with so the range and multipart capabilities can be mixed without their embedded
// method sets colliding. Parts are held by number and concatenated on complete, like the real thing.
type multipartParts struct {
	mu      sync.Mutex
	uploads map[string]map[int32][]byte
	nextID  int
	objs    map[string][]byte // where a completed upload lands; the store's own map

	startN      atomic.Int32
	partN       atomic.Int32
	completeN   atomic.Int32
	abortN      atomic.Int32
	completeErr error
}

func newMultipartParts(objs map[string][]byte) *multipartParts {
	return &multipartParts{uploads: map[string]map[int32][]byte{}, objs: objs}
}

func (m *multipartParts) CreateMultipartUpload(_ context.Context, key string) (string, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.startN.Add(1)
	m.nextID++
	id := key + "#" + strconv.Itoa(m.nextID)
	m.uploads[id] = map[int32][]byte{}

	return id, nil
}

func (m *multipartParts) UploadPart(
	_ context.Context, _, uploadID string, partNum int32, data []byte,
) (string, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.partN.Add(1)

	parts, ok := m.uploads[uploadID]
	if !ok {
		return "", errors.New("no such upload")
	}

	parts[partNum] = slices.Clone(data)

	return strconv.Itoa(int(partNum)), nil
}

func (m *multipartParts) CompleteMultipartUpload(_ context.Context, key, uploadID string, etags []string) error {
	m.completeN.Add(1)

	if m.completeErr != nil {
		return m.completeErr
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	parts, ok := m.uploads[uploadID]
	if !ok {
		return errors.New("no such upload")
	}

	var obj []byte
	for i := range etags {
		obj = append(obj, parts[int32(i+1)]...)
	}

	delete(m.uploads, uploadID)
	m.objs[key] = obj

	return nil
}

func (m *multipartParts) AbortMultipartUpload(_ context.Context, _, uploadID string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.abortN.Add(1)
	delete(m.uploads, uploadID)

	return nil
}

func (m *multipartParts) pending() int {
	m.mu.Lock()
	defer m.mu.Unlock()

	return len(m.uploads)
}

type multipartFaultStore struct {
	*faultStore
	*multipartParts
}

func newMultipartFaultStore() *multipartFaultStore {
	fs := newFaultStore()

	return &multipartFaultStore{faultStore: fs, multipartParts: newMultipartParts(fs.objs)}
}

type rangeMultipartFaultStore struct {
	*rangeFaultStore
	*multipartParts
}

func newRangeMultipartFaultStore() *rangeMultipartFaultStore {
	fs := newRangeFaultStore()

	return &rangeMultipartFaultStore{rangeFaultStore: fs, multipartParts: newMultipartParts(fs.objs)}
}

// TestS3RetryForwardsCapabilityCombinations pins the wrapper's whole capability matrix at once. The
// retry wrapper replaces the store, so each optional capability it fails to forward is silently
// lost — ranged reads become whole-object reads, and streamed writes go back into RAM. The ranged
// half is asserted by behavior, not by type: the Backend always has a ReadAt, and what the missing
// capability costs is that the method fetches the whole object to serve it.
func TestS3RetryForwardsCapabilityCombinations(t *testing.T) {
	t.Parallel()

	cfg := reliability.RetryConfig{MaxAttempts: 3, PerTryTimeout: time.Second}

	// Each case builds its store and hands back the ranged-read counter, nil when it has none.
	tests := []struct {
		name          string
		build         func() (s3.ObjectStore, func() int32)
		wantRange     bool
		wantMultipart bool
	}{
		{
			name:  "neither",
			build: func() (s3.ObjectStore, func() int32) { return newFaultStore(), nil },
		},
		{
			name: "range only",
			build: func() (s3.ObjectStore, func() int32) {
				fs := newRangeFaultStore()

				return fs, fs.rangeN.Load
			},
			wantRange: true,
		},
		{
			name:          "multipart only",
			build:         func() (s3.ObjectStore, func() int32) { return newMultipartFaultStore(), nil },
			wantMultipart: true,
		},
		{
			name: "both",
			build: func() (s3.ObjectStore, func() int32) {
				fs := newRangeMultipartFaultStore()

				return fs, fs.rangeN.Load
			},
			wantRange:     true,
			wantMultipart: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			ctx := context.Background()
			store, ranged := tt.build()
			b := s3.New(store, "oteldb/", s3.WithRetry(cfg))

			assert.Equal(t, tt.wantMultipart, backend.StreamsWrites(b))

			require.NoError(t, b.Write(ctx, "k", []byte("0123456789")))

			got, err := backend.ReadAt(ctx, b, "k", 3, 4)
			require.NoError(t, err)
			assert.Equal(t, []byte("3456"), got)

			if !tt.wantRange {
				return
			}

			assert.Positive(t, ranged(), "a forwarded range capability serves the read")
		})
	}
}

// TestS3RetryStreamsThroughWrapper is the forwarding proven by behavior rather than by assertion:
// the bytes must actually reach the store as parts.
func TestS3RetryStreamsThroughWrapper(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	fs := newRangeMultipartFaultStore()
	b := s3.New(fs, "oteldb/", s3.WithRetry(reliability.RetryConfig{MaxAttempts: 3, PerTryTimeout: time.Second}))
	want := payload(streamedBytes)

	w, err := backend.CreateObject(ctx, b, "part/col")
	require.NoError(t, err)
	defer w.Abort()

	writeStreamed(t, w, want)
	require.NoError(t, w.Commit(ctx))

	got, err := b.Read(ctx, "part/col")
	require.NoError(t, err)
	assert.Equal(t, want, got)
	assert.Equal(t, int32(1), fs.startN.Load())
	assert.Equal(t, int32(3), fs.partN.Load())
}

// TestS3RetryDoesNotRepeatComplete is the conservative half of the retry policy. A multipart
// complete is the object's commit point and S3 can answer it 200 with an error body, so an
// uncertain result does not say whether the object became visible. Re-sending it is a decision
// this layer cannot make, exactly as for the conditional put.
func TestS3RetryDoesNotRepeatComplete(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	fs := newMultipartFaultStore()
	fs.completeErr = errFault
	b := s3.New(fs, "oteldb/", s3.WithRetry(reliability.RetryConfig{MaxAttempts: 3, PerTryTimeout: time.Second}))

	w, err := backend.CreateObject(ctx, b, "part/col")
	require.NoError(t, err)

	writeStreamed(t, w, payload(streamedBytes))
	require.Error(t, w.Commit(ctx))
	assert.Equal(t, int32(1), fs.completeN.Load(), "a transient failure of the commit point is not re-sent")

	w.Abort()
	assert.Zero(t, fs.pending(), "the failed writer still cleans up after itself")
}
