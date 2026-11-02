package chunk

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestU128RunsSizeBounds(t *testing.T) {
	t.Parallel()
	maxInt := int(^uint(0) >> 1)
	first := U128{Hi: 1, Lo: 7}
	for _, tc := range []struct {
		name         string
		runs         []U128Run
		rows, groups int
		first        U128
	}{
		{"empty", nil, 0, 0, U128{}},
		{"ignored", []U128Run{{Count: -1}, {Count: 0}}, 0, 0, U128{}},
		{"coalesced", []U128Run{{Count: 127}, {Count: 0}, {Count: 1}}, 128, 1, U128{}},
		{"separated", []U128Run{{Count: 1}, {Value: U128{Lo: 1}, Count: 128}}, 129, 2, U128{}},
		{"skipped prefix", []U128Run{{Value: U128{Lo: 9}, Count: -1}, {Value: first, Count: 3}}, 3, 1, first},
		{"negative gap", []U128Run{{Value: first, Count: 127}, {Value: U128{Lo: 9}, Count: -1}, {Value: first, Count: 1}}, 128, 1, first},
		{"full wrap", []U128Run{{Value: first, Count: maxInt}, {Value: first, Count: maxInt}, {Value: first, Count: 2}}, 0, 1, first},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			encoded := EncodeU128Runs(nil, tc.runs)
			maxPrefix := int(^uint(0)>>1) - len(encoded)
			rows, size, groups, first := u128RunsSize(tc.runs, maxPrefix)
			require.Equal(t, tc.rows, rows)
			require.Equal(t, len(encoded), size)
			require.Equal(t, tc.groups, groups)
			require.Equal(t, tc.first, first)
			require.PanicsWithValue(t, "chunk: U128 run encoding exceeds addressable size", func() {
				_, _, _, _ = u128RunsSize(tc.runs, maxPrefix+1)
			})
		})
	}
}
