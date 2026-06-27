package reliability_test

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/oteldb/storage/reliability"
)

func TestEnabled(t *testing.T) {
	t.Parallel()

	assert.False(t, reliability.RetryConfig{}.Enabled(), "zero value is plain calls")
	assert.True(t, reliability.Default().Enabled())
	assert.True(t, reliability.LossyEnvironment().Enabled())
	assert.True(t, reliability.RetryConfig{HedgeDelay: 1}.Enabled())
}

func TestPresetsAreSane(t *testing.T) {
	t.Parallel()

	for name, c := range map[string]reliability.RetryConfig{
		"default": reliability.Default(),
		"lossy":   reliability.LossyEnvironment(),
	} {
		assert.GreaterOrEqualf(t, c.MaxAttempts, 1, "%s attempts", name)
		assert.Positivef(t, c.PerTryTimeout, "%s per-try", name)
		assert.Positivef(t, c.HedgeDelay, "%s hedge", name)
		assert.LessOrEqualf(t, c.BaseBackoff, c.MaxBackoff, "%s backoff bounds", name)
	}

	// Lossy abandons a hung attempt sooner and hedges earlier than the mild default.
	assert.Less(t, reliability.LossyEnvironment().PerTryTimeout, reliability.Default().PerTryTimeout)
	assert.Less(t, reliability.LossyEnvironment().HedgeDelay, reliability.Default().HedgeDelay)
}
