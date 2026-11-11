package partsync

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestRejectSeriesCatalogBytes(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name string
		raw  []byte
		bad  bool
	}{
		{name: "empty"},
		{name: "short", raw: []byte("BI")},
		{name: "legacy", raw: []byte{'B', 'I', 8}},
		{name: "other magic", raw: []byte{'B', 'X', 9}},
		{name: "series header only", raw: []byte{'B', 'I', 9}, bad: true},
		{name: "future header", raw: []byte{'B', 'I', 255}, bad: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			err := rejectSeriesCatalogBytes(tc.raw)
			if tc.bad {
				require.ErrorIs(t, err, errSeparateSeries)
			} else {
				require.NoError(t, err)
			}
		})
	}
}
