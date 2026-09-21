package mergestream_test

import (
	"math"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/oteldb/storage/internal/mergestream"
)

func TestBudget(t *testing.T) {
	t.Parallel()

	for _, tt := range []struct {
		name           string
		budget         mergestream.Budget
		disk, resident int64
		want           bool
	}{
		{"unbounded", mergestream.Budget{}, math.MaxInt64, math.MaxInt64, false},
		{"under both", mergestream.Budget{DiskBytes: 10, ResidentBytes: 10}, 9, 9, false},
		{"disk exactly", mergestream.Budget{DiskBytes: 10}, 10, 0, true},
		{"disk over", mergestream.Budget{DiskBytes: 10}, 11, 0, true},
		{"resident exactly", mergestream.Budget{ResidentBytes: 10}, 0, 10, true},
		{"resident alone", mergestream.Budget{DiskBytes: 1 << 40, ResidentBytes: 10}, 5, 10, true},
		{"disk alone", mergestream.Budget{DiskBytes: 10, ResidentBytes: 1 << 40}, 10, 5, true},
		{"negative does not bound", mergestream.Budget{DiskBytes: -1, ResidentBytes: -1}, 1 << 40, 1 << 40, false},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, tt.want, tt.budget.Reached(tt.disk, tt.resident))
		})
	}
}

func TestBudgetBounded(t *testing.T) {
	t.Parallel()

	assert.False(t, mergestream.Budget{}.Bounded())
	assert.False(t, mergestream.Budget{DiskBytes: -1}.Bounded())
	assert.True(t, mergestream.Budget{DiskBytes: 1}.Bounded())
	assert.True(t, mergestream.Budget{ResidentBytes: 1}.Bounded())
}
