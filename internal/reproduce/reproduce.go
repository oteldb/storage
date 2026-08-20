// Package reproduce gates the tests that demonstrate a defect the tree has not fixed yet.
//
// Such a test is a liability in two opposite ways. Left failing it turns CI red and trains everyone
// to ignore it; left plainly skipped it rots quietly, and nobody notices when the behavior it
// describes changes. Gating it on an environment variable keeps CI green while keeping the test one
// command away, so it can be run deliberately — to confirm a defect still bites, to watch a fix
// flip it, or to check that a change did not fix it by accident.
package reproduce

import (
	"os"
	"strconv"
	"testing"
)

// EnvVar enables the reproducers when it parses as a true boolean ("1", "true").
const EnvVar = "OTELDB_STORAGE_REPRODUCE"

// Enabled reports whether the reproducers are enabled.
func Enabled() bool {
	on, err := strconv.ParseBool(os.Getenv(EnvVar))

	return err == nil && on
}

// Unfixed skips the test unless [EnvVar] enables it, naming the issue it reproduces and what the
// tree does today. Call it first in the test body; delete the call with the fix.
//
//	reproduce.Unfixed(t, 392, "the bucket index is committed without compare-and-swap")
func Unfixed(tb testing.TB, issue int, behavior string) {
	tb.Helper()

	if Enabled() {
		return
	}

	tb.Skipf("reproduces #%d: %s (set %s=1 to run it)", issue, behavior, EnvVar)
}
