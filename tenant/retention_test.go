package tenant

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/signal"
)

func TestRetentionAgeFor(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name                string
		def, override, want time.Duration
	}{
		{"longer", time.Hour, 2 * time.Hour, 2 * time.Hour},
		{"shorter", time.Hour, time.Minute, time.Minute},
		{"zero inherits", time.Hour, 0, time.Hour},
		{"negative inherits", time.Hour, -time.Minute, time.Hour},
		{"unbounded default", 0, time.Hour, time.Hour},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			for sig := signal.Metric; sig <= signal.Exemplar; sig++ {
				r := Retention{MaxAge: tc.def, PerSignal: map[signal.Signal]SignalRetention{sig: {MaxAge: tc.override}}}
				require.Equal(t, tc.want, r.AgeFor(sig))
				for other := signal.Metric; other <= signal.Exemplar; other++ {
					if other != sig {
						require.Equal(t, tc.def, r.AgeFor(other))
					}
				}
			}
		})
	}
	r := Retention{MaxAge: time.Hour, PerSignal: map[signal.Signal]SignalRetention{0: {MaxAge: time.Minute}, 200: {MaxAge: time.Minute}}}
	require.Equal(t, time.Hour, r.AgeFor(0))
	require.Equal(t, time.Hour, r.AgeFor(200))
}

func TestRetentionValidate(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name  string
		value Retention
		valid bool
	}{
		{"empty", Retention{}, true},
		{"independent bounds", Retention{MaxAge: time.Hour, MaxBytes: 100, PerSignal: map[signal.Signal]SignalRetention{signal.Metric: {MaxAge: 24 * time.Hour, MaxBytes: 50}}}, true},
		{"unbounded pooled", Retention{PerSignal: map[signal.Signal]SignalRetention{signal.Trace: {MaxBytes: 200}}}, true},
		{"negative age", Retention{MaxAge: -1}, false},
		{"negative bytes", Retention{MaxBytes: -1}, false},
		{"negative signal age", Retention{PerSignal: map[signal.Signal]SignalRetention{signal.Log: {MaxAge: -1}}}, false},
		{"negative signal bytes", Retention{PerSignal: map[signal.Signal]SignalRetention{signal.Profile: {MaxBytes: -1}}}, false},
		{"unknown signal", Retention{PerSignal: map[signal.Signal]SignalRetention{200: {}}}, false},
		{"zero signal", Retention{PerSignal: map[signal.Signal]SignalRetention{0: {}}}, false},
		{"signal above pool", Retention{MaxBytes: 10, PerSignal: map[signal.Signal]SignalRetention{signal.Log: {MaxBytes: 11}}}, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if tc.valid {
				require.NoError(t, tc.value.Validate())
			} else {
				require.Error(t, tc.value.Validate())
			}
		})
	}
}
