package pool

import (
	"strconv"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestByteIntMapEmptyKeySurvivesGrowth(t *testing.T) {
	t.Parallel()
	for _, tt := range []struct {
		name string
		key  []byte
	}{
		{name: "nil"},
		{name: "empty", key: []byte{}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			m := NewByteIntMap()
			defer m.PutBack()
			m.Put(tt.key, 42)
			keys := max(len(m.hashes), 1024)
			for i := range keys {
				m.Put([]byte(strconv.Itoa(i)), i)
			}
			value, ok := m.Get(nil)
			require.True(t, ok)
			require.Equal(t, 42, value)
			require.Equal(t, keys+1, m.Len())
			found := 0
			m.ForEach(func(key []byte, value int) {
				if len(key) == 0 {
					found++
					require.Equal(t, 42, value)
				}
			})
			require.Equal(t, 1, found)
			old, exists := m.Put([]byte{}, 43)
			require.True(t, exists)
			require.Equal(t, 42, old)
		})
	}
}

func TestByteIntMapEmptyKeyAfterReset(t *testing.T) {
	t.Parallel()
	m := NewByteIntMap()
	defer m.PutBack()
	for i := range 1024 {
		m.Put([]byte(strconv.Itoa(i)), i)
	}
	capacity := len(m.hashes)
	for round := range 2 {
		m.Reset()
		require.GreaterOrEqual(t, len(m.hashes), capacity, "pooled reuse retains the grown storage")
		m.Put(nil, round)
		for i := range len(m.hashes) {
			m.Put([]byte(strconv.Itoa(i)), i)
		}
		value, ok := m.Get([]byte{})
		require.True(t, ok)
		require.Equal(t, round, value)
	}
}
