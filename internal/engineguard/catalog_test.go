package engineguard

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestTryLockCatalog(t *testing.T) {
	t.Parallel()
	for _, tt := range []struct {
		name      string
		lock      func(*Mutex)
		unlock    func(*Mutex)
		available bool
	}{
		{name: "available", available: true},
		{name: "ingestion owns state", lock: (*Mutex).LockWriter, unlock: (*Mutex).UnlockWriter, available: true},
		{name: "derived cache owns state", lock: (*Mutex).LockState, unlock: (*Mutex).UnlockState, available: true},
		{name: "catalog owns state", lock: (*Mutex).Lock, unlock: (*Mutex).Unlock},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			var m Mutex
			if tt.lock != nil {
				tt.lock(&m)
				defer tt.unlock(&m)
			}
			acquired := m.TryLockCatalog()
			require.Equal(t, tt.available, acquired)
			if acquired {
				require.False(t, m.catalog.TryLock())
				m.UnlockCatalog()
				require.True(t, m.TryLockCatalog())
				m.UnlockCatalog()
			}
		})
	}
}

func TestTryCatalogOwnershipDoesNotTakeWriterOrState(t *testing.T) {
	t.Parallel()
	var m Mutex
	require.True(t, m.TryLockCatalog())
	defer m.UnlockCatalog()
	require.True(t, m.TryLockWriter())
	m.UnlockWriter()
	m.LockState()
	m.UnlockState()
}
