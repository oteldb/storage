package crashmodel_test

import (
	"testing"

	"github.com/oteldb/storage/internal/vfs/crashmodel"
	"github.com/oteldb/storage/internal/vfs/faultfs"
)

// TestModel drives the scenario table against the fake. It runs in an ordinary `go test ./...`,
// which keeps the table honest between the rare runs of the dm-flakey driver in
// crashmodel_linux_test.go: a scenario that stops describing the model fails here first.
func TestModel(t *testing.T) {
	t.Parallel()

	for _, s := range crashmodel.Scenarios() {
		t.Run(s.Name, func(t *testing.T) {
			t.Parallel()

			f := faultfs.New()
			s.Run(t, f)
			s.CheckModel(t, f.Crash())
		})
	}
}
