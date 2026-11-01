package backend

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestPageEntryWeight(t *testing.T) {
	t.Parallel()
	for _, tt := range []struct {
		name   string
		key    readCacheKey
		bytes  int
		weight uint32
	}{
		{name: "whole object", bytes: 1024, weight: 1024},
		{name: "empty range", key: readCacheKey{generation: &pageGeneration{}}, weight: 256},
		{name: "small frame", key: readCacheKey{generation: &pageGeneration{}}, bytes: 16, weight: 272},
		{name: "index page", key: readCacheKey{generation: &pageGeneration{}}, bytes: 16384, weight: 16640},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			require.Equal(t, tt.weight, weighCacheEntry(tt.key, make([]byte, tt.bytes)))
		})
	}
}
