//go:build !linux

package memlimit

// cgroupBytes and hostBytes have no portable equivalent, so on a platform without /proc and cgroups
// only GOMEMLIMIT answers and the caller falls back to its own default.
func cgroupBytes() int64 { return 0 }

func hostBytes() int64 { return 0 }
