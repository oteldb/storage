package heaptest

import (
	"runtime"
	"testing"
)

// SkipUnsupported temporarily skips heap and allocation guards on macOS and Windows.
func SkipUnsupported(tb testing.TB) {
	tb.Helper()
	skipUnsupported(tb, runtime.GOOS)
}

func skipUnsupported(tb testing.TB, goos string) {
	tb.Helper()

	switch goos {
	case "darwin", "windows":
		tb.Skipf("heap/allocation guards temporarily disabled on %s; see https://github.com/oteldb/storage/issues/780", goos)
	}
}
