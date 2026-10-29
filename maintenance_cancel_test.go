package storage

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestMaintenanceCancellationStopsNewStages(t *testing.T) {
	t.Parallel()
	for _, before := range []bool{false, true} {
		name := "during-flush"
		if before {
			name = "before-start"
		}
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			s, err := InMemory()
			require.NoError(t, err)
			t.Cleanup(func() { require.NoError(t, s.Close(context.Background())) })
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			if before {
				cancel()
			}
			var flushes, merges int
			s.maintainOneEngine(ctx, "default", "/metrics", nil, engineOps{
				reload:  func() error { return nil },
				flush:   func() error { flushes++; cancel(); return ctx.Err() },
				merge:   func() error { merges++; return nil },
				ecParts: func() []ecPartRef { return nil },
			})
			assert.Zero(t, merges, "canceled maintenance must not start a merge")
			if before {
				assert.Zero(t, flushes)
			} else {
				assert.Equal(t, 1, flushes)
			}
		})
	}
}
