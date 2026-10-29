package engineguard

import (
	"testing"

	"github.com/go-faster/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestWithReadersRetainsWriterOwnership(t *testing.T) {
	t.Parallel()
	var m Mutex
	m.Lock()
	err := errors.New("I/O error")
	require.ErrorIs(t, m.WithReaders(func() error {
		assert.False(t, m.writer.TryLock())
		func() { m.RLock(); defer m.RUnlock() }()
		m.LockState()
		m.UnlockState()
		return err
	}), err)
	assert.False(t, m.state.TryRLock(), "state exclusion restored after an I/O error")
	m.Unlock()
	require.True(t, m.state.TryRLock())
	m.state.RUnlock()
}

func TestWithReadersRestoresLockOnPanic(t *testing.T) {
	t.Parallel()
	var m Mutex
	m.Lock()
	require.Panics(t, func() {
		_ = m.WithReaders(func() error { panic("I/O panic") })
	})
	assert.False(t, m.state.TryRLock())
	m.Unlock()
}

func TestWithWritersRetainsCatalogOwnership(t *testing.T) {
	t.Parallel()
	var m Mutex
	m.Lock()
	require.NoError(t, m.WithWriters(func() error {
		assert.False(t, m.catalog.TryLock())
		done := make(chan struct{})
		go func() {
			m.LockWriter()
			m.UnlockWriter()
			m.LockState()
			m.UnlockState()
			close(done)
		}()
		<-done
		return nil
	}))
	assert.False(t, m.writer.TryLock())
	assert.False(t, m.state.TryRLock())
	m.Unlock()
	m.Lock()
	require.Panics(t, func() {
		_ = m.WithWriters(func() error { panic("publication panic") })
	})
	assert.False(t, m.writer.TryLock())
	assert.False(t, m.state.TryRLock())
	m.Unlock()
}
