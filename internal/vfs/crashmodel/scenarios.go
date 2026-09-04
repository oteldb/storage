package crashmodel

import (
	"os"
	"path"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/internal/vfs"
)

// dir is the directory every scenario works in. It is created and published first, so the baseline
// — "this directory is on the disk" — is the same on both drivers and only the operations under
// test are in question.
const dir = "d"

// Scenarios returns the crash stories, one per distinction the model makes.
func Scenarios() []Scenario {
	return []Scenario{
		{
			Name: "FileSyncedDirNot",
			Why:  "syncing a file commits its bytes, not the name that reaches them",
			Run: func(t *testing.T, f vfs.FS) {
				t.Helper()
				base(t, f)
				writeSync(t, f, "d/a", "alpha")
			},
			Expect: []Expect{unpublished("d/a", "alpha")},
		},
		{
			Name: "FileSyncedDirSynced",
			Why:  "a file sync followed by a directory sync is the durable sequence",
			Run: func(t *testing.T, f vfs.FS) {
				t.Helper()
				base(t, f)
				writeSync(t, f, "d/b", "beta")
				require.NoError(t, f.SyncDir(dir))
			},
			Expect: []Expect{durable("d/b", "beta")},
		},
		{
			Name: "NeitherSynced",
			Why:  "a write that was never synced is not owed to anyone",
			Run: func(t *testing.T, f vfs.FS) {
				t.Helper()
				base(t, f)
				write(t, f, "d/c", "gamma")
			},
			Expect: []Expect{unpublished("d/c", "gamma")},
		},
		{
			Name: "DirSyncedFileNot",
			Why:  "publishing a name says nothing about the bytes behind it, which may be none",
			Run: func(t *testing.T, f vfs.FS) {
				t.Helper()
				base(t, f)
				write(t, f, "d/e", "epsilon")
				require.NoError(t, f.SyncDir(dir))
			},
			// The model keeps the name and no bytes. A real disk may have persisted any prefix of
			// the unsynced tail, or dropped the name outright — but never bytes never written.
			Expect: []Expect{{
				Name: "d/e", Present: true, Data: "",
				Allow: prefixes("epsilon", 0), AllowAbsent: true,
			}},
		},
		{
			Name: "RenameSyncedDir",
			Why:  "the publish sequence — write, sync, rename, sync the directory — is durable",
			Run: func(t *testing.T, f vfs.FS) {
				t.Helper()
				base(t, f)
				writeSync(t, f, "d/tmp5", "zeta")
				require.NoError(t, f.Rename("d/tmp5", "d/pub5"))
				require.NoError(t, f.SyncDir(dir))
			},
			Expect: []Expect{durable("d/pub5", "zeta"), erased("d/tmp5")},
		},
		{
			Name: "RenameUnsyncedDir",
			Why:  "a rename is a name change, so it is owed only once the directory is synced",
			Run: func(t *testing.T, f vfs.FS) {
				t.Helper()
				base(t, f)
				writeSync(t, f, "d/tmp6", "eta")
				require.NoError(t, f.Rename("d/tmp6", "d/pub6"))
			},
			Expect: []Expect{unpublished("d/pub6", "eta"), unpublished("d/tmp6", "eta")},
		},
		{
			Name: "RenameOverDurableSyncedDir",
			Why:  "a rename over a durable name replaces it once the directory is synced",
			Run: func(t *testing.T, f vfs.FS) {
				t.Helper()
				base(t, f)
				commit(t, f, "d/tgt7", "old")
				writeSync(t, f, "d/tmp7", "new")
				require.NoError(t, f.Rename("d/tmp7", "d/tgt7"))
				require.NoError(t, f.SyncDir(dir))
			},
			Expect: []Expect{durable("d/tgt7", "new"), erased("d/tmp7")},
		},
		{
			Name: "RenameOverDurableUnsyncedDir",
			Why:  "without the directory sync the replacement may or may not have happened, but a rename is never half-applied",
			Run: func(t *testing.T, f vfs.FS) {
				t.Helper()
				base(t, f)
				commit(t, f, "d/tgt8", "old")
				writeSync(t, f, "d/tmp8", "new")
				require.NoError(t, f.Rename("d/tmp8", "d/tgt8"))
			},
			Expect: []Expect{
				// The name was durable before, so it must still resolve — to one version or the
				// other, never to a mixture and never to nothing.
				{Name: "d/tgt8", Present: true, Data: "old", Allow: []string{"old", "new"}},
				unpublished("d/tmp8", "new"),
			},
		},
		{
			Name: "RemoveSyncedDir",
			Why:  "a removal committed by a directory sync stays removed; the file must not come back",
			Run: func(t *testing.T, f vfs.FS) {
				t.Helper()
				base(t, f)
				commit(t, f, "d/r", "bye")
				require.NoError(t, f.Remove("d/r"))
				require.NoError(t, f.SyncDir(dir))
			},
			Expect: []Expect{erased("d/r")},
		},
		{
			Name: "RemoveUnsyncedDir",
			Why:  "an uncommitted removal may not have happened, so the durable file may still be there",
			Run: func(t *testing.T, f vfs.FS) {
				t.Helper()
				base(t, f)
				commit(t, f, "d/s", "ciao")
				require.NoError(t, f.Remove("d/s"))
			},
			Expect: []Expect{
				{Name: "d/s", Present: true, Data: "ciao", Allow: []string{"ciao"}, AllowAbsent: true},
			},
		},
		{
			Name: "AppendSyncedToDurableName",
			Why:  "once the name is durable a file sync alone commits the appended bytes — the WAL's guarantee",
			Run: func(t *testing.T, f vfs.FS) {
				t.Helper()
				base(t, f)
				commit(t, f, "d/u", "head")
				appendSync(t, f, "d/u", "TAIL")
			},
			Expect: []Expect{durable("d/u", "headTAIL")},
		},
		{
			Name: "AppendUnsyncedToDurableName",
			Why:  "an unsynced append may be lost in whole or in part, but it cannot take committed bytes with it",
			Run: func(t *testing.T, f vfs.FS) {
				t.Helper()
				base(t, f)
				commit(t, f, "d/v", "head")
				appendOnly(t, f, "d/v", "TAIL")
			},
			Expect: []Expect{{
				Name: "d/v", Present: true, Data: "head",
				// Any prefix down to the committed bytes, and no shorter.
				Allow: prefixes("headTAIL", len("head")),
			}},
		},
		{
			Name: "SubdirNotPublished",
			Why:  "syncing a directory does not publish the directory's own name in its parent",
			Run: func(t *testing.T, f vfs.FS) {
				t.Helper()
				base(t, f)
				require.NoError(t, f.MkdirAll("d/sub", 0o750))
				writeSync(t, f, "d/sub/x", "xi")
				require.NoError(t, f.SyncDir("d/sub"))
			},
			Expect: []Expect{unpublished("d/sub/x", "xi")},
		},
		{
			Name: "LinkSyncedDir",
			Why:  "a hard link is a new name, durable on the same terms as any other",
			Run: func(t *testing.T, f vfs.FS) {
				t.Helper()
				base(t, f)
				commit(t, f, "d/src", "linked")
				require.NoError(t, f.Link("d/src", "d/dst"))
				require.NoError(t, f.SyncDir(dir))
			},
			Expect: []Expect{durable("d/src", "linked"), durable("d/dst", "linked")},
		},
	}
}

// durable is a name that must survive with exactly these bytes, on the model and on the disk.
func durable(name, data string) Expect {
	return Expect{Name: name, Present: true, Data: data, Allow: []string{data}}
}

// erased is a name whose disappearance was committed: it must not come back.
func erased(name string) Expect {
	return Expect{Name: name, AllowAbsent: true}
}

// unpublished is a name whose directory was never synced. The model drops it; a real filesystem may
// have kept it, but then only with bytes that were actually written.
func unpublished(name, body string) Expect {
	return Expect{Name: name, Allow: prefixes(body, 0), AllowAbsent: true}
}

// prefixes returns every prefix of s at least keep bytes long: the contents a disk may legally come
// back with when the bytes past keep were written but never synced.
func prefixes(s string, keep int) []string {
	out := make([]string, 0, len(s)-keep+1)
	for i := keep; i <= len(s); i++ {
		out = append(out, s[:i])
	}

	return out
}

// base creates [dir] and publishes it in the root.
func base(t *testing.T, f vfs.FS) {
	t.Helper()
	require.NoError(t, f.MkdirAll(dir, 0o750))
	require.NoError(t, f.SyncDir(path.Dir(dir)))
}

// write creates name and writes body without syncing anything.
func write(t *testing.T, f vfs.FS, name, body string) {
	t.Helper()
	writeFile(t, f, name, body, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, false)
}

// writeSync creates name, writes body and syncs the file, leaving the name unpublished.
func writeSync(t *testing.T, f vfs.FS, name, body string) {
	t.Helper()
	writeFile(t, f, name, body, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, true)
}

// commit writes body to name and publishes it, the fully durable sequence.
func commit(t *testing.T, f vfs.FS, name, body string) {
	t.Helper()
	writeSync(t, f, name, body)
	require.NoError(t, f.SyncDir(dir))
}

func appendOnly(t *testing.T, f vfs.FS, name, body string) {
	t.Helper()
	writeFile(t, f, name, body, os.O_WRONLY|os.O_APPEND, false)
}

func appendSync(t *testing.T, f vfs.FS, name, body string) {
	t.Helper()
	writeFile(t, f, name, body, os.O_WRONLY|os.O_APPEND, true)
}

func writeFile(t *testing.T, f vfs.FS, name, body string, flag int, sync bool) {
	t.Helper()

	w, err := f.OpenFile(name, flag, 0o600)
	require.NoError(t, err)

	defer func() { require.NoError(t, w.Close()) }()

	_, err = w.Write([]byte(body))
	require.NoError(t, err)

	if sync {
		require.NoError(t, w.Sync())
	}
}
