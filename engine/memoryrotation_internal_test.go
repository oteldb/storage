package engine

import (
	"context"
	"sync"
	"testing"

	"github.com/go-faster/errors"
	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/backend"
	"github.com/oteldb/storage/query/fetch"
)

func TestMemoryRunFlushPreservesConcurrentAppend(t *testing.T) {
	t.Parallel()
	e, _ := newFaultWALEngine(t, 0)
	e.cfg.Backend, e.cfg.Prefix = backend.Memory(), "run-rotation"
	enableMemoryPublicationTest(t, e)
	s := apiSeries()
	_, err := e.Append(s, 10, 1)
	require.NoError(t, err)
	started, proceed := make(chan struct{}), make(chan struct{})
	release := sync.OnceFunc(func() { close(proceed) })
	t.Cleanup(release)
	first := true
	e.cfg.FlushAdmission = func(context.Context, int64) (func(), error) {
		if first {
			first = false
			close(started)
			<-proceed
		}
		return func() {}, nil
	}
	done := make(chan error, 1)
	go func() { done <- e.Flush(t.Context()) }()
	<-started
	_, err = e.Append(s, 20, 2)
	require.NoError(t, err)
	count, err := e.Count(t.Context(), fetch.Request{Start: 0, End: 100})
	require.NoError(t, err)
	require.Equal(t, 1, count)
	release()
	require.NoError(t, <-done)
	require.Nil(t, e.readView.frozen)
	require.NotNil(t, e.readView.active)
	require.NotEmpty(t, e.readView.parts.entries)
	it, err := e.Fetch(t.Context(), fetch.Request{Start: 0, End: 100})
	require.NoError(t, err)
	batch, err := it.Next(t.Context())
	require.NoError(t, err)
	require.Equal(t, []int64{10, 20}, batch.Timestamps)
	require.Equal(t, []float64{1, 2}, batch.Values)
	require.NoError(t, it.Close())
}

func TestMemoryRunFlushFailureRestoresDescriptors(t *testing.T) {
	t.Parallel()
	e, _ := newFaultWALEngine(t, 0)
	e.cfg.Backend, e.cfg.Prefix = backend.Memory(), "run-retry"
	enableMemoryPublicationTest(t, e)
	s := apiSeries()
	_, err := e.Append(s, 10, 1)
	require.NoError(t, err)
	original := e.readView.active
	failure := errors.New("injected build failure")
	fail := true
	e.cfg.FlushAdmission = func(context.Context, int64) (func(), error) {
		if fail {
			fail = false
			return nil, failure
		}
		return func() {}, nil
	}
	require.ErrorIs(t, e.Flush(t.Context()), failure)
	require.Same(t, original, e.readView.active, "failure reattaches the existing run descriptors")
	require.Nil(t, e.readView.frozen)
	_, err = e.Append(s, 20, 2)
	require.NoError(t, err)
	require.NoError(t, e.Flush(t.Context()))
	it, err := e.Fetch(t.Context(), fetch.Request{Start: 0, End: 100})
	require.NoError(t, err)
	batch, err := it.Next(t.Context())
	require.NoError(t, err)
	require.Equal(t, []int64{10, 20}, batch.Timestamps)
	require.NoError(t, it.Close())
}
