package faultfs_test

import (
	"testing"

	"github.com/oteldb/storage/internal/vfs"
	"github.com/oteldb/storage/internal/vfs/faultfs"
	"github.com/oteldb/storage/internal/vfs/vfstest"
)

// TestFaultFSConformance holds the fake to the same answers a real directory gives. Without it the
// durability tests below would be measuring the fake's imagination.
func TestFaultFSConformance(t *testing.T) {
	t.Parallel()

	vfstest.Conformance(t, func(*testing.T) vfs.FS { return faultfs.New() })
}
