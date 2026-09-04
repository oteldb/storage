package faultfs_test

import (
	"bytes"
	"fmt"
	"io/fs"
	"os"
	"path"
	"sort"
	"strconv"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/internal/vfs"
	"github.com/oteldb/storage/internal/vfs/faultfs"
)

// envCrashSeed replays a randomized-crash failure: the tests log it with their seed when they fail,
// and setting it re-runs them against that seed.
const envCrashSeed = "OTELDB_STORAGE_CRASH_SEED"

// seed returns the seed a test draws its crash from — its own fixed one, so the suite never flakes,
// unless the environment names another to replay or explore with.
func seed(t *testing.T, fixed uint64) uint64 {
	t.Helper()

	s := fixed

	if v, ok := os.LookupEnv(envCrashSeed); ok {
		parsed, err := strconv.ParseUint(v, 10, 64)
		require.NoError(t, err, "%s must be a uint64", envCrashSeed)
		s = parsed
	}

	t.Cleanup(func() {
		if t.Failed() {
			t.Logf("replay with %s=%d", envCrashSeed, s)
		}
	})

	return s
}

// dump renders a filesystem as a comparable string: every directory and every file with its bytes.
func dump(t *testing.T, f vfs.FS) string {
	t.Helper()

	var (
		lines []string
		walk  func(dir string)
	)

	walk = func(dir string) {
		ents, err := f.ReadDir(dir)
		require.NoError(t, err)

		for _, e := range ents {
			name := path.Join(dir, e.Name())

			if e.IsDir() {
				lines = append(lines, "d "+name)
				walk(name)

				continue
			}

			body, err := f.ReadFile(name)
			require.NoError(t, err)
			lines = append(lines, fmt.Sprintf("f %s %q", name, body))
		}
	}

	walk(".")
	sort.Strings(lines)

	return fmt.Sprint(lines)
}

// mixed builds a filesystem holding one of each durability state: fully published, published name
// with an unsynced tail, synced bytes with an unsynced name, and an unsynced directory.
func mixed(t *testing.T) *faultfs.FS {
	t.Helper()

	f := faultfs.New()

	publish(t, f, ".tmp-done", "done", "published")
	require.NoError(t, f.SyncDir("."))

	w, err := f.OpenFile("done", os.O_WRONLY, 0)
	require.NoError(t, err)
	_, err = w.Write([]byte("-tail"))
	require.NoError(t, err)
	require.NoError(t, w.Close())

	publish(t, f, ".tmp-pending", "pending", "unnamed")
	require.NoError(t, f.MkdirAll("sub", 0o750))

	return f
}

// TestCrashWithZeroIsCrash pins the default: the randomized model with no unsynced survival is the
// deterministic worst case every existing caller already gets.
func TestCrashWithZeroIsCrash(t *testing.T) {
	t.Parallel()

	f := mixed(t)

	assert.Equal(t, dump(t, f.Crash()), dump(t, f.CrashWith(faultfs.CrashConfig{})))
	assert.Equal(t, dump(t, f.Crash()),
		dump(t, f.CrashWith(faultfs.CrashConfig{UnsyncedPercent: 0, Seed: seed(t, 7)})))
}

// TestCrashWithFullIsLive pins the other end: a disk that wrote everything back loses nothing, so
// the clone is the pre-crash state.
func TestCrashWithFullIsLive(t *testing.T) {
	t.Parallel()

	f := mixed(t)

	assert.Equal(t, dump(t, f), dump(t, f.CrashWith(faultfs.CrashConfig{UnsyncedPercent: 100})))
}

// TestCrashWithDeterministic is the reproducibility contract. The clone walks Go maps, whose order
// changes on every range, so this has to hold across repeated runs in one process — not just across
// two calls.
func TestCrashWithDeterministic(t *testing.T) {
	t.Parallel()

	f := faultfs.New()
	for i := range 8 {
		publish(t, f, fmt.Sprintf(".tmp-%d", i), fmt.Sprintf("obj-%d", i), fmt.Sprintf("body-%d", i))
	}

	require.NoError(t, f.MkdirAll("a/b", 0o750))

	cfg := faultfs.CrashConfig{UnsyncedPercent: 50, Seed: seed(t, 42)}
	want := dump(t, f.CrashWith(cfg))

	for range 32 {
		require.Equal(t, want, dump(t, f.CrashWith(cfg)))
	}
}

// blocks writes n blocks of distinct non-zero bytes into name, syncing (and publishing) only the
// first, so everything past the first block is a candidate for writeback.
func blocks(t *testing.T, f vfs.FS, name string, n, size int) []byte {
	t.Helper()

	body := make([]byte, 0, n*size)
	for i := range n {
		body = append(body, bytes.Repeat([]byte{byte('a' + i)}, size)...)
	}

	w, err := f.OpenFile(name, os.O_CREATE|os.O_WRONLY, 0o600)
	require.NoError(t, err)
	_, err = w.Write(body[:size])
	require.NoError(t, err)
	require.NoError(t, w.Sync())
	require.NoError(t, f.SyncDir("."))
	_, err = w.Write(body[size:])
	require.NoError(t, err)
	require.NoError(t, w.Close())

	return body
}

// TestCrashWithZeroHole is the state [FS.Crash] structurally cannot reach and the one a log replayer
// gets wrong: a block that never made it back comes back as zeros, and a later block that did made
// the file longer than its synced length. A run of zeros is then not the end of the log.
func TestCrashWithZeroHole(t *testing.T) {
	t.Parallel()

	const (
		size  = 4096
		count = 8
	)

	f := faultfs.New()
	body := blocks(t, f, "log", count, size)

	var (
		found bool
		used  uint64
	)

	for s := range uint64(64) {
		got, err := f.CrashWith(faultfs.CrashConfig{UnsyncedPercent: 50, Seed: s}).ReadFile("log")
		require.NoError(t, err)

		hole, tail := -1, -1

		for i := 1; i*size < len(got); i++ {
			block := got[i*size : (i+1)*size]

			switch {
			case bytes.Equal(block, make([]byte, size)):
				if hole < 0 {
					hole = i
				}
			case hole >= 0:
				tail = i
			}

			// A block either landed whole or not at all; nothing in between.
			require.True(t, bytes.Equal(block, body[i*size:(i+1)*size]) ||
				bytes.Equal(block, make([]byte, size)), "seed %d block %d is neither written nor zero", s, i)
		}

		require.Equal(t, body[:size], got[:size], "seed %d lost synced bytes", s)

		if hole >= 0 && tail > hole {
			found, used = true, s
			require.Greater(t, len(got), size, "seed %d: the file must outgrow its synced length", s)

			break
		}
	}

	require.True(t, found, "no seed in [0,64) produced a hole followed by a surviving block")
	t.Logf("hole reproduced at %s=%d", envCrashSeed, used)
}

// TestCrashWithPerBlockSurvival shows the draws are per block rather than all-or-nothing: over a
// span of seeds the surviving length lands strictly between the synced prefix and the whole file.
func TestCrashWithPerBlockSurvival(t *testing.T) {
	t.Parallel()

	const (
		size  = 4096
		count = 8
	)

	f := faultfs.New()
	blocks(t, f, "log", count, size)

	lengths := map[int]struct{}{}

	for s := range uint64(64) {
		got, err := f.CrashWith(faultfs.CrashConfig{UnsyncedPercent: 50, Seed: s}).ReadFile("log")
		require.NoError(t, err)
		lengths[len(got)] = struct{}{}
	}

	assert.Greater(t, len(lengths), 2, "survival collapsed to keeping none or all of the tail")

	for n := range lengths {
		assert.GreaterOrEqual(t, n, size)
		assert.LessOrEqual(t, n, count*size)
	}
}

// TestCrashWithEntrySurvival covers the third draw: an unsynced rename may or may not have reached
// the directory, and that is decided independently of the bytes the name reaches.
func TestCrashWithEntrySurvival(t *testing.T) {
	t.Parallel()

	f := faultfs.New()
	publish(t, f, ".tmp-obj", "obj", "synced")

	w, err := f.OpenFile("obj", os.O_WRONLY, 0)
	require.NoError(t, err)
	_, err = w.Write([]byte("-unsynced"))
	require.NoError(t, err)
	require.NoError(t, w.Close())

	var (
		missing  bool
		partial  bool
		complete bool
	)

	for s := range uint64(64) {
		got, err := f.CrashWith(faultfs.CrashConfig{UnsyncedPercent: 50, Seed: s}).ReadFile("obj")

		switch {
		case err != nil:
			require.ErrorIs(t, err, fs.ErrNotExist)

			missing = true
		case string(got) == "synced":
			partial = true
		case string(got) == "synced-unsynced":
			complete = true
		default:
			t.Fatalf("seed %d: unexpected body %q", s, got)
		}
	}

	assert.True(t, missing, "the rename never survived")
	assert.True(t, partial, "the name survived without the unsynced tail")
	assert.True(t, complete, "the name survived with the unsynced tail")
}

// TestCrashWithUnreachableParent covers the one filter the draws do not override: a file whose
// bytes and whose own directory entry are both durable is still gone if the directory holding *that
// directory* was never synced, because no path reaches it.
func TestCrashWithUnreachableParent(t *testing.T) {
	t.Parallel()

	f := faultfs.New()
	require.NoError(t, f.MkdirAll("sub", 0o750))
	publish(t, f, "sub/.tmp-obj", "sub/obj", "bytes")
	require.NoError(t, f.SyncDir("sub"))

	got, ok := f.Durable("sub/obj")
	require.True(t, ok, "the file's own entry is durable")
	require.Equal(t, []byte("bytes"), got)

	_, err := f.Crash().ReadFile("sub/obj")
	require.ErrorIs(t, err, fs.ErrNotExist, "the directory naming sub was never synced")

	_, err = f.CrashWith(faultfs.CrashConfig{UnsyncedPercent: 100}).ReadFile("sub/obj")
	require.NoError(t, err, "a disk that wrote everything back keeps the entry for sub too")
}

// TestCrashWithClamps keeps a nonsense percentage from being a nonsense filesystem.
func TestCrashWithClamps(t *testing.T) {
	t.Parallel()

	f := mixed(t)

	assert.Equal(t, dump(t, f.Crash()), dump(t, f.CrashWith(faultfs.CrashConfig{UnsyncedPercent: -5})))
	assert.Equal(t, dump(t, f), dump(t, f.CrashWith(faultfs.CrashConfig{UnsyncedPercent: 250})))
}

// TestCrashWithLeavesReceiver keeps the clone a clone: crashing must not disturb the filesystem it
// was taken from, so one state can be crashed under many seeds.
func TestCrashWithLeavesReceiver(t *testing.T) {
	t.Parallel()

	f := mixed(t)
	before := dump(t, f)

	for s := range uint64(16) {
		f.CrashWith(faultfs.CrashConfig{UnsyncedPercent: 50, Seed: s})
	}

	assert.Equal(t, before, dump(t, f))
}
