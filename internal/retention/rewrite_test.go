package retention

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestDue(t *testing.T) {
	t.Parallel()
	start := time.Unix(0, 0)
	for _, tt := range []struct {
		name      string
		last, now time.Time
		want      bool
	}{
		{"first rewrite", time.Time{}, start, true},
		{"foreground", start, time.Time{}, true},
		{"same instant", start, start, false},
		{"before interval", start, start.Add(RewriteInterval - time.Nanosecond), false},
		{"at interval", start, start.Add(RewriteInterval), true},
		{"after interval", start, start.Add(2 * RewriteInterval), true},
		{"clock moves back", start, start.Add(-time.Second), false},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, tt.want, Due(tt.last, tt.now))
		})
	}
}
