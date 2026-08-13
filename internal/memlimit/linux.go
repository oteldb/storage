//go:build linux

package memlimit

import (
	"bufio"
	"bytes"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
)

const (
	procCgroup    = "/proc/self/cgroup"
	procMountInfo = "/proc/self/mountinfo"
	procMemInfo   = "/proc/meminfo"

	defaultCgroupRoot = "/sys/fs/cgroup"
)

func cgroupBytes() int64 {
	if n := cgroupV2Bytes(defaultCgroupRoot, procCgroup, procMountInfo); n > 0 {
		return n
	}

	return cgroupV1Bytes(defaultCgroupRoot, procCgroup)
}

func hostBytes() int64 { return memTotal(procMemInfo) }

// cgroupV2Bytes reads memory.max for the process's own cgroup and every ancestor up to the mount
// root, returning the smallest limit any of them sets. An ancestor's limit binds the process just
// as its own does, and a leaf that inherits its limit reports the literal "max".
func cgroupV2Bytes(fallbackRoot, cgroupFile, mountInfoFile string) int64 {
	rel, ok := cgroupPath(cgroupFile, "")
	if !ok {
		return 0
	}

	root := fallbackRoot
	if mp, ok := mountPoint(mountInfoFile, "cgroup2"); ok {
		root = mp
	}

	var limit int64

	for dir := filepath.Join(root, filepath.FromSlash(rel)); ; dir = filepath.Dir(dir) {
		if n, ok := readLimit(filepath.Join(dir, "memory.max")); ok && (limit == 0 || n < limit) {
			limit = n
		}

		if dir == root || !strings.HasPrefix(dir, root) {
			break
		}
	}

	return limit
}

// cgroupV1Bytes reads the memory controller's limit_in_bytes, which reports "no limit" as a number
// so large it is indistinguishable from one — hence the sanity bound against the host's memory.
func cgroupV1Bytes(root, cgroupFile string) int64 {
	rel, _ := cgroupPath(cgroupFile, "memory")

	for _, p := range []string{
		filepath.Join(root, "memory", filepath.FromSlash(rel), "memory.limit_in_bytes"),
		filepath.Join(root, "memory", "memory.limit_in_bytes"),
	} {
		n, ok := readLimit(p)
		if !ok {
			continue
		}

		if host := hostBytes(); host > 0 && n >= host {
			continue // the unlimited sentinel, not a limit
		}

		return n
	}

	return 0
}

// cgroupPath returns the process's path within the named v1 controller, or within the v2 hierarchy
// (the "0::" line) when controller is empty.
func cgroupPath(file, controller string) (string, bool) {
	data, err := os.ReadFile(file) //nolint:gosec // a /proc path, or a test's temp dir
	if err != nil {
		return "", false
	}

	for line := range bytes.Lines(data) {
		// hierarchy-ID:controller-list:path
		parts := strings.SplitN(strings.TrimSpace(string(line)), ":", 3)
		if len(parts) != 3 {
			continue
		}

		if controller == "" {
			if parts[0] == "0" && parts[1] == "" {
				return parts[2], true
			}

			continue
		}

		if slices.Contains(strings.Split(parts[1], ","), controller) {
			return parts[2], true
		}
	}

	return "", false
}

// mountPoint returns where fstype is mounted, per mountinfo's post-separator fields.
func mountPoint(file, fstype string) (string, bool) {
	f, err := os.Open(file) //nolint:gosec // a /proc path, or a test's temp dir
	if err != nil {
		return "", false
	}
	defer func() { _ = f.Close() }()

	sc := bufio.NewScanner(f)
	for sc.Scan() {
		fields := strings.Fields(sc.Text())

		sep := -1

		for i, v := range fields {
			if v == "-" {
				sep = i

				break
			}
		}

		if sep < 0 || sep+1 >= len(fields) || fields[sep+1] != fstype || sep < 5 {
			continue
		}

		return fields[4], true
	}

	return "", false
}

func readLimit(path string) (int64, bool) {
	data, err := os.ReadFile(path) //nolint:gosec // a cgroupfs path assembled from /proc
	if err != nil {
		return 0, false
	}

	n, err := strconv.ParseInt(strings.TrimSpace(string(data)), 10, 64)
	if err != nil || n <= 0 {
		return 0, false // "max", or a value no budget can be taken from
	}

	return n, true
}

// memTotal reads MemTotal (in kB) from a meminfo-formatted file.
func memTotal(file string) int64 {
	f, err := os.Open(file) //nolint:gosec // a /proc path, or a test's temp dir
	if err != nil {
		return 0
	}
	defer func() { _ = f.Close() }()

	sc := bufio.NewScanner(f)
	for sc.Scan() {
		fields := strings.Fields(sc.Text())
		if len(fields) < 2 || fields[0] != "MemTotal:" {
			continue
		}

		n, err := strconv.ParseInt(fields[1], 10, 64)
		if err != nil {
			return 0
		}

		return n * 1024
	}

	return 0
}
