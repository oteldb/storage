package recordengine

import (
	"math"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func scanInRange(buf *recordCols, start, end int64) bool {
	if buf == nil {
		return false
	}

	for _, t := range buf.ts {
		if t >= start && t <= end {
			return true
		}
	}

	return false
}

func headBuf(ts ...int64) *recordCols {
	buf := newRecordCols(headTestSchema, 0, fullSel(headTestSchema))
	for _, t := range ts {
		buf.appendClone(headTestRec(t))
	}

	return buf
}

func TestBufInRange(t *testing.T) {
	t.Parallel()

	trimmedEmpty := headBuf(10, 20)
	trimmedEmpty.keep(0, 0)

	// Bounds stay [10, 50] after dropping the rows at the edges, wider than the surviving 30.
	trimmedInner := headBuf(10, 30, 50)
	trimmedInner.keep(1, 2)

	reattached := headBuf(100)
	reattached.appendRange(headBuf(5, 500), 0, 2)

	tests := []struct {
		name       string
		buf        *recordCols
		start, end int64
		want       bool
	}{
		{"nil", nil, math.MinInt64, math.MaxInt64, false},
		{"empty", headBuf(), math.MinInt64, math.MaxInt64, false},
		{"trimmed to empty keeps stale bounds", trimmedEmpty, 0, 100, false},
		{"window below", headBuf(10, 20), 0, 9, false},
		{"window above", headBuf(10, 20), 21, 30, false},
		{"touches min", headBuf(10, 20), 0, 10, true},
		{"touches max", headBuf(10, 20), 20, 30, true},
		{"covers all", headBuf(10, 20), 10, 20, true},
		{"unbounded", headBuf(10, 20), math.MinInt64, math.MaxInt64, true},
		{"gap between rows", headBuf(10, 20), 11, 19, false},
		{"inside stale bounds, no live row", trimmedInner, 40, 50, false},
		{"inside stale bounds, live row", trimmedInner, 25, 35, true},
		{"covers stale bounds", trimmedInner, 0, 100, true},
		{"reattached rows widen bounds", reattached, 400, 600, true},
		{"reattached, window between rows", reattached, 6, 99, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			assert.Equal(t, tt.want, bufInRange(tt.buf, tt.start, tt.end))
			assert.Equal(t, scanInRange(tt.buf, tt.start, tt.end), bufInRange(tt.buf, tt.start, tt.end))
		})
	}
}

// FuzzBufInRangeMatchesScan drives a head-shaped buffer through the operations that change its rows —
// appends, the trims that drop rows without narrowing the bounds, and a reattach-style bulk append —
// and checks every window against a plain scan.
func FuzzBufInRangeMatchesScan(f *testing.F) {
	f.Add([]byte{0, 10, 0, 20, 0, 30, 1, 1, 2, 2, 5, 25, 3, 0, 0})
	f.Add([]byte{0, 200, 2, 0, 0, 3, 7, 9, 0, 1})

	f.Fuzz(func(t *testing.T, script []byte) {
		buf := headBuf()

		for i := 0; i+2 < len(script); i += 3 {
			op, a, b := script[i]%4, int64(int8(script[i+1])), int64(int8(script[i+2]))

			switch op {
			case 0:
				buf.appendClone(headTestRec(a))
			case 1:
				lo, hi := clampRange(int(a), int(b), buf.len())
				buf.keep(lo, hi)
			case 2:
				var idx []int
				for r := range buf.len() {
					if (r+int(a))%3 != 0 {
						idx = append(idx, r)
					}
				}

				buf.gatherRows(idx)
			case 3:
				buf.appendRange(headBuf(a, b), 0, 2)
			}

			start, end := min(a, b), max(a, b)
			require.Equal(t, scanInRange(buf, start, end), bufInRange(buf, start, end),
				"window [%d,%d] over %v", start, end, buf.ts)
		}
	})
}

func clampRange(a, b, n int) (lo, hi int) {
	if n == 0 {
		return 0, 0
	}

	lo, hi = min(max(a, 0), n), min(max(b, 0), n)
	if lo > hi {
		lo, hi = hi, lo
	}

	return lo, hi
}
