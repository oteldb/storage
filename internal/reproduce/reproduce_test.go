package reproduce_test

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/oteldb/storage/internal/reproduce"
)

func TestEnabledParsesTheEnvironment(t *testing.T) {
	for _, tt := range []struct {
		value string
		want  bool
	}{
		{"1", true},
		{"true", true},
		{"TRUE", true},
		{"0", false},
		{"false", false},
		{"", false},
		{"yes", false},
	} {
		t.Run(tt.value, func(t *testing.T) {
			t.Setenv(reproduce.EnvVar, tt.value)
			assert.Equal(t, tt.want, reproduce.Enabled())
		})
	}
}

// fakeTB records what Unfixed did to it, so the gate can be tested without skipping this test.
type fakeTB struct {
	testing.TB

	skipped bool
	format  string
	args    []any
}

func (f *fakeTB) Helper() {}

func (f *fakeTB) Skipf(format string, args ...any) {
	f.skipped = true
	f.format, f.args = format, args
}

func TestUnfixedSkipsUnlessEnabled(t *testing.T) {
	t.Setenv(reproduce.EnvVar, "")

	var tb fakeTB
	reproduce.Unfixed(&tb, 392, "the index is committed without CAS")

	assert.True(t, tb.skipped)
	assert.Contains(t, tb.args, 392)
	assert.Contains(t, tb.args, reproduce.EnvVar)
}

func TestUnfixedRunsWhenEnabled(t *testing.T) {
	t.Setenv(reproduce.EnvVar, "1")

	var tb fakeTB
	reproduce.Unfixed(&tb, 392, "the index is committed without CAS")

	assert.False(t, tb.skipped)
}
