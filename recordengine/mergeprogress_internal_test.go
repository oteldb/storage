package recordengine

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/encoding/compress"
)

func TestMergeStepProgress(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name              string
		parts             int
		retain            int64
		refuse            bool
		failure           error
		changed, deferred bool
	}{
		{name: "empty"},
		{name: "singleton compression", parts: 1, changed: true},
		{name: "compaction", parts: 2, changed: true},
		{name: "admission deferred", parts: 2, refuse: true, deferred: true},
		{name: "retention before deferral", parts: 3, retain: 50, refuse: true, changed: true, deferred: true},
		{name: "admission error", parts: 2, failure: context.Canceled},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			ctx := context.Background()
			times := make([][]int64, tc.parts)
			for i := range times {
				times[i] = []int64{int64(i*100 + 1), int64(i*100 + 2)}
			}
			e := compressionEngine(t, times...)
			e.cfg.MergeAdmission = func(context.Context, int64, bool) (func(), bool, error) {
				return func() {}, !tc.refuse, tc.failure
			}
			before := e.PartPrefixes()
			changed, err := e.MergeStep(ctx, MergeOptions{RetainFrom: tc.retain, Background: true})
			if tc.failure != nil {
				require.ErrorIs(t, err, tc.failure)
			} else {
				require.NoError(t, err)
			}
			assert.Equal(t, tc.changed, changed)
			assert.Equal(t, tc.deferred, e.MergeDeferred())
			if !changed {
				assert.Equal(t, before, e.PartPrefixes())
			}
			if tc.name == "singleton compression" {
				assert.Equal(t, 1, e.PartCount())
				for _, c := range e.parts[0].reader.Manifest().Columns {
					if !c.Const {
						assert.Equal(t, compress.AlgorithmZSTD, c.Compress)
					}
				}
			}
		})
	}
}
