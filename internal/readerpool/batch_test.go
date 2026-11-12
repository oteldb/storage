package readerpool

import (
	"bytes"
	"context"
	"io/fs"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/go-faster/errors"

	"github.com/stretchr/testify/require"
)

func batchContext(s *Scope, b *Batch) context.Context {
	return s.Context(context.WithValue(context.Background(), batchKey{}, b))
}

func TestBatchReusesLeaseWithoutPoolLock(t *testing.T) {
	t.Parallel()
	p := newPool(1)
	s := testScope()
	b := newBatch(p)
	ctx := batchContext(s, b)
	raw := &testReader{Reader: bytes.NewReader([]byte("abcdef"))}
	opens := 0
	open := func() (Reader, error) { opens++; return raw, nil }
	got, ok, err := Append(ctx, 1, "data", nil, 0, 3, open)
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, "abc", string(got))
	// A warm range must not need the global directory/LRU lock.
	p.mu.Lock()
	got, ok, err = Append(ctx, 1, "data", got[:0], 3, 3, open)
	p.mu.Unlock()
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, "def", string(got))
	require.Equal(t, 1, opens)
	require.NoError(t, b.Reset())
	require.Empty(t, b.entries)
	require.NoError(t, b.Close())
	require.NoError(t, b.Close())
	_, ok, err = Append(ctx, 1, "data", nil, 0, 1, open)
	require.True(t, ok)
	require.ErrorIs(t, err, fs.ErrClosed)
	require.NoError(t, p.closeScope(s.id))
	require.EqualValues(t, 1, raw.closed.Load())
}

func TestBatchRetainsOldGeneration(t *testing.T) {
	t.Parallel()
	p := newPool(3)
	s := testScope()
	b := newBatch(p)
	ctx := batchContext(s, b)
	old := &testReader{Reader: bytes.NewReader([]byte("old"))}
	next := &testReader{Reader: bytes.NewReader([]byte("new"))}
	current := old
	open := func() (Reader, error) { return current, nil }
	_, ok, err := Append(ctx, 1, "data", nil, 0, 3, open)
	require.NoError(t, err)
	require.True(t, ok)
	finish := p.beginMutation(1, "data")
	fresh := batchContext(s, b)
	_, _, err = Append(fresh, 1, "data", nil, 0, 3, open)
	require.ErrorIs(t, err, errGenerationChanged)
	current = next
	finish()
	got, _, err := Append(ctx, 1, "data", nil, 0, 3, open)
	require.NoError(t, err)
	require.Equal(t, "old", string(got))
	got, _, err = Append(batchContext(s, b), 1, "data", nil, 0, 3, open)
	require.NoError(t, err)
	require.Equal(t, "new", string(got))
	require.NoError(t, b.Reset())
	p.pruneOld()
	require.EqualValues(t, 1, old.closed.Load())
	require.NoError(t, b.Close())
	require.NoError(t, p.closeScope(s.id))
	require.Zero(t, p.opened)
}

func TestBatchLimitsAndReset(t *testing.T) {
	t.Parallel()
	for _, limit := range []int{1, 16} {
		t.Run(strconv.Itoa(limit), func(t *testing.T) {
			t.Parallel()
			p := newPool(limit)
			s := testScope()
			b := newBatch(p)
			ctx := batchContext(s, b)
			opened := 0
			open := func() (Reader, error) { opened++; return &testReader{Reader: bytes.NewReader([]byte("data"))}, nil }
			for i := range batchReaderLimit + 2 {
				_, ok, err := Append(ctx, 1, strconv.Itoa(i), nil, 0, 4, open)
				require.NoError(t, err)
				if i < min(limit, batchReaderLimit) {
					require.True(t, ok)
				}
				require.LessOrEqual(t, p.opened, limit)
				require.LessOrEqual(t, len(b.entries), batchReaderLimit)
			}
			require.LessOrEqual(t, opened, limit)
			require.NoError(t, b.Reset())
			require.Empty(t, b.entries)
			_, ok, err := Append(ctx, 1, "after-reset", nil, 0, 4, open)
			require.NoError(t, err)
			require.True(t, ok)
			require.NoError(t, b.Close())
			require.NoError(t, p.closeScope(s.id))
			require.Zero(t, p.opened)
		})
	}
}

type batchFailureReader struct{ *testReader }

func (r *batchFailureReader) ReadAt([]byte, int64) (int, error) { return 0, fs.ErrInvalid }
func (r *batchFailureReader) Close() error                      { r.testReader.Close(); return fs.ErrPermission }

func TestBatchReadAndCloseErrors(t *testing.T) {
	t.Parallel()
	p := newPool(1)
	s := testScope()
	b := newBatch(p)
	ctx := batchContext(s, b)
	raw := &batchFailureReader{testReader: &testReader{Reader: bytes.NewReader([]byte("data"))}}
	open := func() (Reader, error) { return raw, nil }
	_, ok, err := Append(ctx, 1, "data", nil, 0, 4, open)
	require.True(t, ok)
	require.ErrorIs(t, err, fs.ErrInvalid)
	require.NoError(t, p.closeScope(s.id))
	require.ErrorIs(t, b.Close(), fs.ErrPermission)
	require.EqualValues(t, 1, raw.closed.Load())
	require.Zero(t, p.opened)
}

func TestBatchConcurrentResetAndClose(t *testing.T) {
	t.Parallel()
	p := newPool(4)
	s := testScope()
	b := newBatch(p)
	ctx := batchContext(s, b)
	var opens atomic.Int32
	open := func() (Reader, error) { opens.Add(1); return &testReader{Reader: bytes.NewReader([]byte("data"))}, nil }
	_, ok, err := Append(ctx, 1, "data", nil, 0, 4, open)
	require.NoError(t, err)
	require.True(t, ok)
	var workers sync.WaitGroup
	for range 8 {
		workers.Go(func() {
			var dst []byte
			for range 100 {
				got, ok, err := Append(ctx, 1, "data", dst[:0], 0, 4, open)
				if errors.Is(err, fs.ErrClosed) {
					return
				}
				if err != nil || !ok || string(got) != "data" {
					t.Errorf("range: %q %v %v", got, ok, err)
					return
				}
				dst = got
			}
		})
	}
	for range 20 {
		require.NoError(t, b.Reset())
	}
	require.NoError(t, b.Close())
	workers.Wait()
	require.NoError(t, p.closeScope(s.id))
	require.Zero(t, p.opened)
	require.EqualValues(t, 1, opens.Load())
}

func TestBatchRemembersDeclineAndChecksGeneration(t *testing.T) {
	t.Parallel()
	p := newPool(1)
	occupied := testScope()
	s := testScope()
	b := newBatch(p)
	ctx := batchContext(s, b)
	open := func() (Reader, error) { return &testReader{Reader: bytes.NewReader([]byte("data"))}, nil }
	held, ok, err := p.acquire(occupied, 1, "occupied", open)
	require.NoError(t, err)
	require.True(t, ok)
	_, ok, err = Append(ctx, 1, "declined", nil, 0, 4, open)
	require.NoError(t, err)
	require.False(t, ok)
	p.mu.Lock()
	_, ok, err = Append(ctx, 1, "declined", nil, 0, 4, open)
	p.mu.Unlock()
	require.NoError(t, err)
	require.False(t, ok, "a declined lease need not rescan the global LRU")
	finish := p.beginMutation(1, "declined")
	_, ok, err = Append(ctx, 1, "declined", nil, 0, 4, open)
	require.True(t, ok)
	require.ErrorIs(t, err, errGenerationChanged)
	finish()
	require.NoError(t, b.Close())
	require.NoError(t, held.Close())
	require.NoError(t, p.closeScope(occupied.id))
	require.NoError(t, p.closeScope(s.id))
	require.Zero(t, p.opened)
}

func TestBatchOpenErrorCanRetry(t *testing.T) {
	t.Parallel()
	p := newPool(1)
	scope := testScope()
	b := newBatch(p)
	ctx := batchContext(scope, b)
	sentinel := errors.New("open failed")
	_, ok, err := Append(ctx, 1, "data", nil, 0, 4, func() (Reader, error) { return nil, sentinel })
	require.True(t, ok)
	require.ErrorIs(t, err, sentinel)
	require.Empty(t, b.entries)
	require.Zero(t, p.opened)
	data, ok, err := Append(ctx, 1, "data", nil, 0, 4, func() (Reader, error) { return &testReader{Reader: bytes.NewReader([]byte("data"))}, nil })
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, "data", string(data))
	require.NoError(t, b.Close())
	require.NoError(t, p.closeScope(scope.id))
}

type callbackCloseReader struct {
	*testReader

	closeCallback func()
}

func (r *callbackCloseReader) Close() error { r.closeCallback(); return r.testReader.Close() }

func TestBatchClosesReadersOutsideCoordinationLock(t *testing.T) {
	t.Parallel()
	p := newPool(1)
	scope := testScope()
	b := newBatch(p)
	ctx := batchContext(scope, b)
	raw := &callbackCloseReader{testReader: &testReader{Reader: bytes.NewReader([]byte("data"))}}
	raw.closeCallback = func() {
		_, ok, err := Append(ctx, 1, "data", nil, 0, 1, func() (Reader, error) { return nil, fs.ErrInvalid })
		require.True(t, ok)
		require.ErrorIs(t, err, fs.ErrClosed)
	}
	_, ok, err := Append(ctx, 1, "data", nil, 0, 4, func() (Reader, error) { return raw, nil })
	require.NoError(t, err)
	require.True(t, ok)
	require.NoError(t, p.closeScope(scope.id))
	require.NoError(t, b.Close())
	require.Zero(t, p.opened)
}
