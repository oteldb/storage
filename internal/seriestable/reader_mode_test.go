package seriestable

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/backend"
	"github.com/oteldb/storage/readbudget"
)

func TestStreamDescriptorsReaderBudget(t *testing.T) {
	t.Parallel()
	for _, initialized := range []bool{false, true} {
		name := "before-reader"
		if initialized {
			name = "after-reader"
		}
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			be := backend.Cached(backend.Memory(), 1<<20)
			data := descriptors(64)
			run := buildRun(t, be, "reader-mode", data)
			limit := 2*run.MaxPageBytes() + 4096
			budget := readbudget.New(limit)
			r, err := run.NewReader(readbudget.With(t.Context(), budget))
			require.NoError(t, err)
			if initialized {
				r.rawReader()
			}
			r.StreamDescriptors()
			for _, s := range data {
				raw, found, err := r.LookupRaw(s.Hash())
				require.NoError(t, err)
				require.True(t, found)
				require.Equal(t, s.AppendHashInput(nil), raw)
			}
			require.Zero(t, budget.Remaining())
			require.NoError(t, r.Close())
			require.Equal(t, limit, budget.Remaining())
		})
	}
}
