package signal

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSignalStringAndParse(t *testing.T) {
	t.Parallel()

	cases := []struct {
		s    Signal
		name string
	}{
		{Metric, "metric"},
		{Log, "log"},
		{Trace, "trace"},
		{Profile, "profile"},
	}
	for _, tc := range cases {
		assert.Equal(t, tc.name, tc.s.String())

		got, err := ParseSignal(tc.name)
		require.NoError(t, err)
		assert.Equal(t, tc.s, got)
	}

	assert.Equal(t, "unknown", Signal(99).String())

	_, err := ParseSignal("nope")
	require.ErrorIs(t, err, ErrUnknownSignal)
}

func TestTenantIDString(t *testing.T) {
	t.Parallel()
	assert.Equal(t, "default", TenantID("").String())
	assert.Equal(t, "acme", TenantID("acme").String())
}
