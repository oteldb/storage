//go:build linux

package memlimit

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func writeFile(t *testing.T, path, content string) {
	t.Helper()

	require.NoError(t, os.MkdirAll(filepath.Dir(path), 0o755))
	require.NoError(t, os.WriteFile(path, []byte(content), 0o600))
}

func TestCgroupV2Bytes(t *testing.T) {
	t.Parallel()

	const cgroupFile = "0::/kubepods/burstable/pod123/container\n"

	cases := []struct {
		name   string
		limits map[string]string // path under the cgroup root → memory.max contents
		want   int64
	}{
		{
			name:   "the leaf sets the limit",
			limits: map[string]string{"kubepods/burstable/pod123/container": "4294967296"},
			want:   4 << 30,
		},
		{
			// A pod-level limit binds a container that inherits it, so the walk must not stop at a
			// leaf reporting "max".
			name: "an ancestor limit is inherited",
			limits: map[string]string{
				"kubepods/burstable/pod123/container": "max",
				"kubepods/burstable/pod123":           "4294967296",
			},
			want: 4 << 30,
		},
		{
			name: "the tightest limit in the chain wins",
			limits: map[string]string{
				"kubepods/burstable/pod123/container": "2147483648",
				"kubepods/burstable/pod123":           "4294967296",
			},
			want: 2 << 30,
		},
		{
			name:   "no limit anywhere",
			limits: map[string]string{"kubepods/burstable/pod123/container": "max"},
			want:   0,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			root := t.TempDir()

			cg := filepath.Join(root, "cgroup")
			writeFile(t, cg, cgroupFile)

			for rel, content := range tc.limits {
				writeFile(t, filepath.Join(root, "fs", rel, "memory.max"), content)
			}

			got := cgroupV2Bytes(filepath.Join(root, "fs"), cg, filepath.Join(root, "absent"))
			assert.Equal(t, tc.want, got)
		})
	}
}

func TestCgroupV2BytesWithoutV2(t *testing.T) {
	t.Parallel()

	root := t.TempDir()

	cg := filepath.Join(root, "cgroup")
	writeFile(t, cg, "7:memory:/some/path\n")

	assert.Zero(t, cgroupV2Bytes(root, cg, filepath.Join(root, "absent")),
		"a v1-only hierarchy has no 0:: line and must not be read as unlimited-at-root")
}

func TestCgroupV1Bytes(t *testing.T) {
	t.Parallel()

	root := t.TempDir()

	cg := filepath.Join(root, "cgroup")
	writeFile(t, cg, "9:cpu,cpuacct:/x\n7:memory:/docker/abc\n")
	writeFile(t, filepath.Join(root, "fs", "memory", "docker", "abc", "memory.limit_in_bytes"), "1073741824")

	assert.Equal(t, int64(1<<30), cgroupV1Bytes(filepath.Join(root, "fs"), cg))
}

func TestCgroupV1UnlimitedSentinel(t *testing.T) {
	t.Parallel()

	root := t.TempDir()

	cg := filepath.Join(root, "cgroup")
	writeFile(t, cg, "7:memory:/\n")
	// The "no limit" value is a real number larger than any host's RAM, so it must be rejected
	// against the host total rather than handed back as a budget.
	writeFile(t, filepath.Join(root, "fs", "memory", "memory.limit_in_bytes"), "9223372036854771712")

	assert.Zero(t, cgroupV1Bytes(filepath.Join(root, "fs"), cg))
}

func TestMountPoint(t *testing.T) {
	t.Parallel()

	root := t.TempDir()
	mi := filepath.Join(root, "mountinfo")
	//nolint:dupword // mountinfo names the source and the fstype, which for these two are the same word
	writeFile(t, mi, ""+
		"25 30 0:23 / /proc rw,relatime - proc proc rw\n"+
		"1665 1662 0:29 / /custom/cgroup ro,nosuid - cgroup2 cgroup2 rw,nsdelegate\n")

	mp, ok := mountPoint(mi, "cgroup2")
	require.True(t, ok)
	assert.Equal(t, "/custom/cgroup", mp)

	_, ok = mountPoint(mi, "tmpfs")
	assert.False(t, ok, "an unmounted fstype must report absent, not an empty path")
}

func TestMemTotal(t *testing.T) {
	t.Parallel()

	root := t.TempDir()
	mi := filepath.Join(root, "meminfo")
	writeFile(t, mi, "MemFree:  1024 kB\nMemTotal:       32793112 kB\n")

	assert.Equal(t, int64(32793112)*1024, memTotal(mi))
	assert.Zero(t, memTotal(filepath.Join(root, "absent")))
}

// TestBytesOnThisMachine pins the contract the merge cap depends on: a budget is either a positive
// figure or an explicit "unknown", never a negative or absurd one.
func TestBytesOnThisMachine(t *testing.T) {
	t.Parallel()

	assert.GreaterOrEqual(t, Bytes(), int64(0))
}
