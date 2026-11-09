//go:build !windows

package readerpool

func systemDescriptorPressure(error) bool { return false }
