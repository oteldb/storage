package readerpool

import (
	"bytes"
	"context"
	"io"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

type testReader struct {
	*bytes.Reader

	closed atomic.Int32
}

func (r *testReader) Size() int64  { return r.Reader.Size() }
func (r *testReader) Close() error { r.closed.Add(1); return nil }
func testScope() *Scope {
	s := &Scope{id: scopeIDs.Add(1), active: new(atomic.Int64), generation: new(atomic.Uint64), mutating: new(atomic.Int64), memory: new(atomic.Int64)}
	s.generation.Store(s.id)
	return s
}

func TestPoolReusesHandleAndIndependentCursor(t *testing.T) {
	t.Parallel()
	p := newPool(2)
	s := testScope()
	var opened int
	open := func() (Reader, error) { opened++; return &testReader{Reader: bytes.NewReader([]byte("abcdef"))}, nil }
	a, ok, err := p.acquire(s, 1, "data", open)
	require.NoError(t, err)
	require.True(t, ok)
	b, ok, err := p.acquire(s, 1, "data", open)
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, 1, opened)
	buf := make([]byte, 2)
	_, err = a.Read(buf)
	require.NoError(t, err)
	require.Equal(t, "ab", string(buf))
	_, err = b.Read(buf)
	require.NoError(t, err)
	require.Equal(t, "ab", string(buf))
	require.NoError(t, a.Close())
	require.NoError(t, b.Close())
	require.Equal(t, 1, p.opened)
	require.NoError(t, p.closeScope(s.id))
	require.Zero(t, p.opened)
}

func TestPoolSaturationDoesNotWaitOrOpen(t *testing.T) {
	t.Parallel()
	p := newPool(1)
	a, b := testScope(), testScope()
	var opened int
	raw := &testReader{Reader: bytes.NewReader([]byte("old"))}
	open := func() (Reader, error) { opened++; return raw, nil }
	first, ok, err := p.acquire(a, 1, "data", open)
	require.NoError(t, err)
	require.True(t, ok)
	second, ok, err := p.acquire(b, 1, "data", open)
	require.NoError(t, err)
	require.False(t, ok)
	require.Nil(t, second)
	require.Equal(t, 1, opened)
	require.NoError(t, p.closeScope(a.id))
	require.Zero(t, raw.closed.Load(), "an active lease survives owner retirement")
	buf := make([]byte, 3)
	_, err = first.ReadAt(buf, 0)
	require.NoError(t, err)
	require.Equal(t, "old", string(buf))
	second, ok, err = p.acquire(b, 1, "data", open)
	require.NoError(t, err)
	require.False(t, ok)
	require.Nil(t, second)
	require.NoError(t, first.Close())
	require.Equal(t, int32(1), raw.closed.Load())
	require.Zero(t, p.opened)
}

func TestPoolDoesNotEvictAnActivePart(t *testing.T) {
	t.Parallel()
	p := newPool(1)
	a, b := testScope(), testScope()
	a.Acquire()
	opened := 0
	open := func() (Reader, error) { opened++; return &testReader{Reader: bytes.NewReader([]byte("data"))}, nil }
	r, _, err := p.acquire(a, 1, "data", open)
	require.NoError(t, err)
	require.NoError(t, r.Close())
	r, ok, err := p.acquire(b, 1, "other", open)
	require.NoError(t, err)
	require.False(t, ok)
	require.Nil(t, r)
	require.Equal(t, 1, opened)
	a.Release()
	r, ok, err = p.acquire(b, 1, "other", open)
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, 2, opened)
	require.NoError(t, r.Close())
	require.NoError(t, p.closeScope(b.id))
	require.Zero(t, p.opened)
}

func TestUnscopedOpenAndRebind(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	require.False(t, Scoped(ctx))
	r, ok, err := Open(ctx, 1, "key", func() (Reader, error) { return &testReader{Reader: bytes.NewReader(nil)}, nil })
	require.NoError(t, err)
	require.True(t, ok)
	require.NoError(t, r.Close())
	s := NewScope()
	defer func() { require.NoError(t, s.Close()) }()
	next, cancel := context.WithCancel(ctx)
	cancel()
	scoped := Rebind(next, s.Context(ctx))
	require.True(t, Scoped(scoped))
	require.ErrorIs(t, scoped.Err(), context.Canceled)
	require.Equal(t, ctx, Rebind(ctx, ctx))
}

func TestLeaseSeek(t *testing.T) {
	t.Parallel()
	p := newPool(1)
	s := testScope()
	r, _, err := p.acquire(s, 2, "key", func() (Reader, error) { return &testReader{Reader: bytes.NewReader([]byte("abc"))}, nil })
	require.NoError(t, err)
	pos, err := r.Seek(-1, io.SeekEnd)
	require.NoError(t, err)
	require.Equal(t, int64(2), pos)
	buf := make([]byte, 1)
	_, err = r.Read(buf)
	require.NoError(t, err)
	require.Equal(t, "c", string(buf))
	require.NoError(t, r.Close())
	require.NoError(t, r.Close())
	_, err = r.Read(buf)
	require.Error(t, err)
	require.NoError(t, p.closeScope(s.id))
}

func TestBlockedOpenDoesNotBlockUnrelatedHit(t *testing.T) {
	t.Parallel()
	p := newPool(2)
	s := testScope()
	open := func() (Reader, error) { return &testReader{Reader: bytes.NewReader([]byte("data"))}, nil }
	r, _, err := p.acquire(s, 1, "warm", open)
	require.NoError(t, err)
	require.NoError(t, r.Close())
	blocked := make(chan struct{})
	release := make(chan struct{})
	defer func() {
		select {
		case <-release:
		default:
			close(release)
		}
	}()
	cold := make(chan Reader, 1)
	go func() {
		r, _, _ := p.acquire(s, 1, "cold", func() (Reader, error) { close(blocked); <-release; return open() })
		cold <- r
	}()
	<-blocked
	hit := make(chan Reader, 1)
	go func() { r, _, _ := p.acquire(s, 1, "warm", open); hit <- r }()
	select {
	case r := <-hit:
		require.NotNil(t, r)
		require.NoError(t, r.Close())
	case <-time.After(5 * time.Second):
		t.Fatal("unrelated warm read waited for filesystem open")
	}
	close(release)
	r = <-cold
	require.NotNil(t, r)
	require.NoError(t, r.Close())
	require.NoError(t, p.closeScope(s.id))
}

func TestNewOwnerCannotOpenDuringPublication(t *testing.T) {
	t.Parallel()
	p := newPool(1)
	s := testScope()
	published := false
	finish := p.beginMutation(1, "key")
	calls := 0
	open := func() (Reader, error) {
		calls++
		data := "old"
		if published {
			data = "new"
		}
		return &testReader{Reader: bytes.NewReader([]byte(data))}, nil
	}
	r, supported, err := p.acquire(s, 1, "key", open)
	require.True(t, supported)
	require.Error(t, err)
	require.Nil(t, r)
	require.Zero(t, calls)
	published = true
	finish()
	r, supported, err = p.acquire(s, 1, "key", open)
	require.NoError(t, err)
	require.True(t, supported)
	require.Equal(t, 1, calls)
	buf := make([]byte, 3)
	_, err = r.ReadAt(buf, 0)
	require.NoError(t, err)
	require.Equal(t, "new", string(buf))
	require.NoError(t, r.Close())
	require.NoError(t, p.closeScope(s.id))
}

type limitedReader struct {
	*testReader

	live *atomic.Int64
}

func (r *limitedReader) Close() error {
	if r.closed.Add(1) == 1 {
		r.live.Add(-1)
	}
	return nil
}

func TestDescriptorPressureReclaimsOnlyIdle(t *testing.T) {
	t.Parallel()
	p := newPool(8)
	s := testScope()
	s.Acquire()
	defer s.Release()
	var live atomic.Int64
	open := func() (Reader, error) {
		if live.Load() >= 2 {
			return nil, syscall.EMFILE
		}
		live.Add(1)
		return &limitedReader{testReader: &testReader{Reader: bytes.NewReader([]byte("data"))}, live: &live}, nil
	}
	active, ok, err := p.acquire(s, 1, "active", open)
	require.NoError(t, err)
	require.True(t, ok)
	idle, ok, err := p.acquire(s, 1, "idle", open)
	require.NoError(t, err)
	require.True(t, ok)
	require.NoError(t, idle.Close())
	next, ok, err := p.acquire(s, 1, "next", open)
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, int64(2), live.Load())
	data := make([]byte, 4)
	_, err = active.ReadAt(data, 0)
	require.NoError(t, err)
	require.Equal(t, "data", string(data))
	require.NoError(t, active.Close())
	require.NoError(t, next.Close())
	require.NoError(t, p.closeScope(s.id))
	require.Zero(t, live.Load())
}
