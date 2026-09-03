//go:build !unix

package vfs

import "os"

// syncDir is a no-op off unix: there is no directory handle to fsync, and the platform's
// atomic-replace primitive carries the ordering a directory sync provides elsewhere.
func syncDir(*os.Root, string) error { return nil }
