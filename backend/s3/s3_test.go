package s3_test

import (
	"context"
	"slices"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/backend"
	"github.com/oteldb/storage/backend/backendtest"
	"github.com/oteldb/storage/backend/s3"
)

// fakeStore is an in-memory [s3.ObjectStore] that faithfully mirrors the S3 semantics the
// Backend relies on: Get returns ErrObjectNotFound when absent, Delete is idempotent,
// conditional put is atomic, and List order is unspecified (returned unsorted so the test
// exercises the Backend's own sort). Values are copied in and out, like a real store.
type fakeStore struct {
	mu   sync.Mutex
	objs map[string][]byte
}

func newFakeStore() *fakeStore { return &fakeStore{objs: make(map[string][]byte)} }

func clone(b []byte) []byte {
	if b == nil {
		return []byte{}
	}

	return slices.Clone(b)
}

func (f *fakeStore) GetObject(_ context.Context, key string) ([]byte, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	v, ok := f.objs[key]
	if !ok {
		return nil, s3.ErrObjectNotFound
	}

	return clone(v), nil
}

func (f *fakeStore) PutObject(_ context.Context, key string, data []byte) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.objs[key] = clone(data)

	return nil
}

func (f *fakeStore) PutObjectIfAbsent(_ context.Context, key string, data []byte) (bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	if _, ok := f.objs[key]; ok {
		return false, nil
	}

	f.objs[key] = clone(data)

	return true, nil
}

func (f *fakeStore) HeadObject(_ context.Context, key string) (bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	_, ok := f.objs[key]

	return ok, nil
}

func (f *fakeStore) DeleteObject(_ context.Context, key string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	delete(f.objs, key) // idempotent, like S3

	return nil
}

func (f *fakeStore) ListObjects(_ context.Context, prefix string) ([]string, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	var keys []string
	for k := range f.objs { // map order ⇒ deliberately unsorted
		if len(k) >= len(prefix) && k[:len(prefix)] == prefix {
			keys = append(keys, k)
		}
	}

	return keys, nil
}

func TestS3Conformance(t *testing.T) {
	t.Parallel()
	backendtest.Run(t, func(*testing.T) backend.Backend {
		return s3.New(newFakeStore(), "")
	})
}

func TestS3ConformanceWithRootPrefix(t *testing.T) {
	t.Parallel()
	backendtest.Run(t, func(*testing.T) backend.Backend {
		return s3.New(newFakeStore(), "root/")
	})
}

// TestS3RootPrefixIsolation confirms the root prefix is applied to stored keys and stripped
// from listings, so two Backends sharing one store but rooted differently do not collide.
func TestS3RootPrefixIsolation(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	store := newFakeStore()
	a := s3.New(store, "a/")
	b := s3.New(store, "b/")

	require.NoError(t, a.Write(ctx, "metrics/0", []byte("av")))
	require.NoError(t, b.Write(ctx, "metrics/0", []byte("bv")))

	av, err := a.Read(ctx, "metrics/0")
	require.NoError(t, err)
	assert.Equal(t, []byte("av"), av)

	keys, err := a.List(ctx, "")
	require.NoError(t, err)
	assert.Equal(t, []string{"metrics/0"}, keys, "a sees only its own keys, prefix stripped")

	// The underlying store holds both, rooted distinctly.
	raw, err := store.ListObjects(ctx, "")
	require.NoError(t, err)
	slices.Sort(raw)
	assert.Equal(t, []string{"a/metrics/0", "b/metrics/0"}, raw)
}
