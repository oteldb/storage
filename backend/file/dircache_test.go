package file

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDirCache(t *testing.T) {
	t.Parallel()

	for _, tt := range []struct {
		name   string
		run    func(c *dirCache)
		dir    string
		expect bool
	}{
		{name: "root", run: func(*dirCache) {}, dir: ".", expect: true},
		{name: "unknown", run: func(*dirCache) {}, dir: "t1", expect: false},
		{
			name:   "marked",
			run:    func(c *dirCache) { c.mark([]string{"t1/m", "t1"}, c.snapshot()) },
			dir:    "t1/m",
			expect: true,
		},
		{
			name: "forgotten",
			run: func(c *dirCache) {
				c.mark([]string{"t1"}, c.snapshot())
				c.forget("t1")
			},
			dir:    "t1",
			expect: false,
		},
		{
			name: "removal during publish discards marks",
			run: func(c *dirCache) {
				epoch := c.snapshot()
				c.forget("t2")
				c.mark([]string{"t1"}, epoch)
			},
			dir:    "t1",
			expect: false,
		},
		{
			name: "forget keeps siblings",
			run: func(c *dirCache) {
				c.mark([]string{"t1/a", "t1/b", "t1"}, c.snapshot())
				c.forget("t1/a")
			},
			dir:    "t1/b",
			expect: true,
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			c := newDirCache()
			tt.run(c)
			assert.Equal(t, tt.expect, c.durable(tt.dir))
		})
	}
}

func TestDirCacheSharedPerRoot(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()

	a, err := New(dir)
	require.NoError(t, err)

	b, err := New(dir)
	require.NoError(t, err)

	assert.Same(t, a.dirs, b.dirs)
}

func TestDirCacheSharedAcrossSymlinkedRoot(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	target := filepath.Join(dir, "real")
	alias := filepath.Join(dir, "alias")

	a, err := New(target)
	require.NoError(t, err)

	if err := os.Symlink(target, alias); err != nil {
		t.Skipf("symlinks unavailable: %v", err)
	}

	b, err := New(alias)
	require.NoError(t, err)

	assert.Same(t, a.dirs, b.dirs)
}

func TestDirCacheClearedWhenRootReopened(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()

	a, err := New(dir)
	require.NoError(t, err)

	a.dirs.mark([]string{"t1"}, a.dirs.snapshot())

	_, err = New(dir)
	require.NoError(t, err)

	assert.False(t, a.dirs.durable("t1"))
}
