package otlpdecode_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/plog"

	"github.com/oteldb/storage/otlp/otlpdecode"
)

// TestConvertLogsTimeFallback pins the rule for the two optional OTLP timestamps: an unset event
// time falls back to the observed time, and a record with neither is refused rather than stored at
// the unix epoch. convertBoth also asserts the pdata path answers the same way.
func TestConvertLogsTimeFallback(t *testing.T) {
	t.Parallel()

	const (
		event    = 1_700_000_000_000_000_000
		observed = 1_700_000_001_000_000_000
	)

	for _, tt := range []struct {
		name      string
		event     pcommon.Timestamp
		observed  pcommon.Timestamp
		want      int64
		wantDrops int
	}{
		{name: "no time", wantDrops: 1},
		{name: "observed only", observed: observed, want: observed},
		{name: "both", event: event, observed: observed, want: event},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			ld := plog.NewLogs()
			rec := ld.ResourceLogs().AppendEmpty().ScopeLogs().AppendEmpty().LogRecords().AppendEmpty()
			rec.Body().SetStr("hello")
			rec.SetTimestamp(tt.event)
			rec.SetObservedTimestamp(tt.observed)

			direct, viaPdata := convertBoth(t, ld)
			assert.Equal(t, viaPdata, direct)

			var c otlpdecode.LogsConverter

			got, dropped, err := c.Convert(marshal(t, ld))
			require.NoError(t, err)
			assert.Equal(t, tt.wantDrops, dropped)

			records := got.Resources[0].Scopes[0].Records
			if tt.wantDrops > 0 {
				assert.Empty(t, records)
				return
			}

			require.Len(t, records, 1)
			assert.Equal(t, tt.want, records[0].Timestamp)
			assert.Equal(t, int64(tt.observed), records[0].ObservedTimestamp)
		})
	}
}
