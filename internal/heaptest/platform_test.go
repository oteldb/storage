package heaptest

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
)

type skipRecorder struct {
	testing.TB

	reason string
}

func (*skipRecorder) Helper() {}
func (r *skipRecorder) Skipf(format string, args ...any) {
	r.reason = fmt.Sprintf(format, args...)
}

func TestSkipUnsupported(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		goos string
		skip bool
	}{
		{"linux", false},
		{"darwin", true},
		{"windows", true},
		{"freebsd", false},
	} {
		t.Run(tc.goos, func(t *testing.T) {
			t.Parallel()

			recorder := &skipRecorder{TB: t}
			skipUnsupported(recorder, tc.goos)
			if tc.skip {
				assert.Contains(t, recorder.reason, tc.goos)
				assert.Contains(t, recorder.reason, "https://github.com/oteldb/storage/issues/780")
			} else {
				assert.Empty(t, recorder.reason)
			}
		})
	}
}
