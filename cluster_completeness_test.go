package storage

import (
	"context"
	"math"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/signal"
)

func TestReadGapOverlaps(t *testing.T) {
	t.Parallel()

	g := readGap{after: 100}

	for _, tt := range []struct {
		name       string
		start, end int64
		want       bool
	}{
		{"wholly before", 0, 99, false},
		{"ending on the bound", 0, 100, true},
		{"wholly after", 201, 300, true},
		{"spanning", 0, 1000, true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, tt.want, g.overlaps(tt.start, tt.end))
		})
	}
}

// TestReadGapIsPerSignalAndBounded: a gap disclaims only what the parts do not cover, and only for
// the signal that lost its head.
func TestReadGapIsPerSignalAndBounded(t *testing.T) {
	t.Parallel()

	s := &Storage{}
	s.noteReadGap(signal.Metric, "default", 100)

	require.True(t, s.hasReadGap(signal.Metric, "default"))
	assert.True(t, s.readGapOverlaps(signal.Metric, "default", 200, 300))
	assert.False(t, s.readGapOverlaps(signal.Metric, "default", 0, 99), "older windows are still served here")
	assert.False(t, s.hasReadGap(signal.Log, "default"), "a gap is per signal")
}

// TestCanAnswerWidensZeroWindow: the enumeration RPCs spell "no time filter" as a zero window, which
// must be read as unbounded — otherwise a label listing would slip past a gap that covers the data
// it would enumerate.
func TestCanAnswerWidensZeroWindow(t *testing.T) {
	t.Parallel()

	s, err := InMemory()
	require.NoError(t, err)
	t.Cleanup(func() { _ = s.Close(context.Background()) })

	s.noteReadGap(signal.Metric, "default", math.MinInt64)

	assert.False(t, s.canAnswer(context.Background(), rpcOpSeries, signal.Metric, "default", 0, 0),
		"an unbounded listing certainly reaches into the gap")
}
