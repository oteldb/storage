package mergestream_test

import (
	"testing"

	"github.com/go-faster/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/internal/mergestream"
)

func TestCheckForward(t *testing.T) {
	t.Parallel()

	for _, tt := range []struct {
		name            string
		pos, start, end int
		ok              bool
	}{
		{"contiguous", 10, 10, 20, true},
		{"gap skipped forward", 10, 15, 20, true},
		{"empty range at the cursor", 10, 10, 10, true},
		{"from the start", 0, 0, 0, true},
		{"overlapping the consumed rows", 10, 9, 20, false},
		{"entirely behind", 10, 0, 5, false},
		{"inverted", 10, 20, 15, false},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			err := mergestream.CheckForward(tt.pos, tt.start, tt.end)
			if tt.ok {
				assert.NoError(t, err)

				return
			}

			require.Error(t, err)
			require.ErrorIs(t, err, mergestream.ErrNotForward)
			assert.Contains(t, err.Error(), "behind cursor")
		})
	}
}

func TestCheckForwardWrapsRatherThanReturningTheSentinel(t *testing.T) {
	t.Parallel()

	err := mergestream.CheckForward(7, 3, 4)
	require.Error(t, err)
	assert.NotEqual(t, mergestream.ErrNotForward, err, "the sentinel is wrapped, not returned bare")
	assert.Equal(t, mergestream.ErrNotForward, errors.Unwrap(err))
}
