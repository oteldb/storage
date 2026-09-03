package faultfs

import (
	"io"
	"io/fs"
	"os"
	"path"
	"slices"
	"sort"
	"strings"
	"time"

	"github.com/go-faster/errors"

	"github.com/oteldb/storage/internal/vfs"
)

// clean normalizes a name to the form the maps are keyed by, refusing one that escapes the root.
func clean(name string) (string, error) {
	if name == "" {
		return "", &fs.PathError{Op: opOpen, Path: name, Err: fs.ErrInvalid}
	}

	c := path.Clean(name)
	if path.IsAbs(c) || c == ".." || strings.HasPrefix(c, "../") {
		return "", &fs.PathError{Op: opOpen, Path: name, Err: fs.ErrInvalid}
	}

	return c, nil
}

// opOpen is the syscall name a path error carries when the fake refuses a name outright.
const opOpen = "open"

func notExist(op, name string) error {
	return &fs.PathError{Op: op, Path: name, Err: fs.ErrNotExist}
}

// OpenFile implements [vfs.FS].
func (f *FS) OpenFile(name string, flag int, perm fs.FileMode) (vfs.File, error) {
	c, err := clean(name)
	if err != nil {
		return nil, err
	}

	op := OpOpen
	if flag&os.O_CREATE != 0 {
		op = OpCreate
	}

	if err := f.enter(Call{Op: op, Name: c}); err != nil {
		return nil, err
	}

	f.mu.Lock()
	defer f.mu.Unlock()

	n, ok := f.live[c]

	switch {
	case ok && flag&os.O_EXCL != 0 && flag&os.O_CREATE != 0:
		return nil, &fs.PathError{Op: opOpen, Path: name, Err: fs.ErrExist}
	case !ok && flag&os.O_CREATE == 0:
		return nil, notExist(opOpen, name)
	case !ok:
		if _, dirOK := f.dirs[path.Dir(c)]; !dirOK {
			return nil, notExist(opOpen, name)
		}

		n = &node{perm: perm, mod: time.Now()}
		f.live[c] = n
		// A fresh name is visible immediately and durable only once its directory is synced.
		f.links[path.Dir(c)] = append(f.links[path.Dir(c)], c)
	case flag&os.O_TRUNC != 0:
		n.data = n.data[:0]
	}

	return &handle{fs: f, name: c, node: n, write: flag&(os.O_WRONLY|os.O_RDWR) != 0}, nil
}

// ReadFile implements [vfs.FS].
func (f *FS) ReadFile(name string) ([]byte, error) {
	h, err := f.OpenFile(name, os.O_RDONLY, 0)
	if err != nil {
		return nil, err
	}
	defer func() { _ = h.Close() }()

	return io.ReadAll(h)
}

// ReadDir implements [vfs.FS].
func (f *FS) ReadDir(name string) ([]fs.DirEntry, error) {
	c, err := clean(name)
	if err != nil {
		return nil, err
	}

	if err := f.enter(Call{Op: OpReadDir, Name: c}); err != nil {
		return nil, err
	}

	f.mu.Lock()
	defer f.mu.Unlock()

	if _, ok := f.dirs[c]; !ok {
		return nil, notExist("readdir", name)
	}

	var out []fs.DirEntry

	for p, n := range f.live {
		if path.Dir(p) == c {
			out = append(out, &dirEntry{name: path.Base(p), size: int64(len(n.data)), mode: n.perm, mod: n.mod})
		}
	}

	for p, mode := range f.dirs {
		if p != c && path.Dir(p) == c {
			out = append(out, &dirEntry{name: path.Base(p), mode: mode | fs.ModeDir, dir: true})
		}
	}

	sort.Slice(out, func(i, j int) bool { return out[i].Name() < out[j].Name() })

	return out, nil
}

// Stat implements [vfs.FS].
func (f *FS) Stat(name string) (fs.FileInfo, error) {
	c, err := clean(name)
	if err != nil {
		return nil, err
	}

	if err := f.enter(Call{Op: OpStat, Name: c}); err != nil {
		return nil, err
	}

	f.mu.Lock()
	defer f.mu.Unlock()

	if n, ok := f.live[c]; ok {
		return &dirEntry{name: path.Base(c), size: int64(len(n.data)), mode: n.perm, mod: n.mod}, nil
	}

	if mode, ok := f.dirs[c]; ok {
		return &dirEntry{name: path.Base(c), mode: mode | fs.ModeDir, dir: true}, nil
	}

	return nil, notExist("stat", name)
}

// MkdirAll implements [vfs.FS].
func (f *FS) MkdirAll(name string, perm fs.FileMode) error {
	c, err := clean(name)
	if err != nil {
		return err
	}

	if err := f.enter(Call{Op: OpMkdir, Name: c}); err != nil {
		return err
	}

	f.mu.Lock()
	defer f.mu.Unlock()

	for _, d := range chain(c) {
		if _, ok := f.dirs[d]; ok {
			continue
		}

		f.dirs[d] = perm
		f.links[path.Dir(d)] = append(f.links[path.Dir(d)], d)
	}

	return nil
}

// chain returns dir and every parent below the root, outermost first, so MkdirAll records each
// directory it created as a name its own parent must sync.
func chain(dir string) []string {
	var out []string
	for d := dir; d != "."; d = path.Dir(d) {
		out = append(out, d)
	}

	slices.Reverse(out)

	return out
}

// Rename implements [vfs.FS].
func (f *FS) Rename(oldname, newname string) error {
	from, err := clean(oldname)
	if err != nil {
		return err
	}

	to, err := clean(newname)
	if err != nil {
		return err
	}

	if err := f.enter(Call{Op: OpRename, Name: from, To: to}); err != nil {
		return err
	}

	f.mu.Lock()
	defer f.mu.Unlock()

	n, ok := f.live[from]
	if !ok {
		return notExist("rename", oldname)
	}

	if _, dirOK := f.dirs[path.Dir(to)]; !dirOK {
		return notExist("rename", newname)
	}

	delete(f.live, from)
	f.live[to] = n
	f.links[path.Dir(to)] = append(f.links[path.Dir(to)], to)
	f.unlinks[path.Dir(from)] = append(f.unlinks[path.Dir(from)], from)

	return nil
}

// Link implements [vfs.FS].
func (f *FS) Link(oldname, newname string) error {
	from, err := clean(oldname)
	if err != nil {
		return err
	}

	to, err := clean(newname)
	if err != nil {
		return err
	}

	if err := f.enter(Call{Op: OpLink, Name: from, To: to}); err != nil {
		return err
	}

	f.mu.Lock()
	defer f.mu.Unlock()

	n, ok := f.live[from]
	if !ok {
		return notExist("link", oldname)
	}

	if _, exists := f.live[to]; exists {
		return &fs.PathError{Op: "link", Path: newname, Err: fs.ErrExist}
	}

	// A hard link is a second name for the same bytes; the fake copies them, which differs only for
	// a writer that keeps writing through one name and reads back through the other — something no
	// consumer of this seam does (the publish paths link a finished temp file into place).
	f.live[to] = &node{data: slices.Clone(n.data), synced: slices.Clone(n.synced), perm: n.perm, mod: n.mod}
	f.links[path.Dir(to)] = append(f.links[path.Dir(to)], to)

	return nil
}

// Remove implements [vfs.FS].
func (f *FS) Remove(name string) error {
	c, err := clean(name)
	if err != nil {
		return err
	}

	if err := f.enter(Call{Op: OpRemove, Name: c}); err != nil {
		return err
	}

	f.mu.Lock()
	defer f.mu.Unlock()

	if _, ok := f.live[c]; ok {
		delete(f.live, c)
		f.unlinks[path.Dir(c)] = append(f.unlinks[path.Dir(c)], c)

		return nil
	}

	if _, ok := f.dirs[c]; ok {
		for p := range f.live {
			if path.Dir(p) == c {
				return &fs.PathError{Op: "remove", Path: name, Err: errors.New("directory not empty")}
			}
		}

		delete(f.dirs, c)
		f.unlinks[path.Dir(c)] = append(f.unlinks[path.Dir(c)], c)

		return nil
	}

	return notExist("remove", name)
}

// SyncDir implements [vfs.FS]: it commits the name changes made in the directory. A name that
// appeared becomes durable carrying the bytes of its last file sync — nothing more, so a file whose
// directory was synced but whose own bytes were not comes back empty rather than full.
func (f *FS) SyncDir(name string) error {
	c, err := clean(name)
	if err != nil {
		return err
	}

	if err := f.enter(Call{Op: OpSyncDir, Name: c}); err != nil {
		return err
	}

	f.mu.Lock()
	defer f.mu.Unlock()

	if _, ok := f.dirs[c]; !ok {
		return notExist("syncdir", name)
	}

	for _, n := range f.unlinks[c] {
		delete(f.durable, n)
		delete(f.durableDirs, n)
	}

	delete(f.unlinks, c)

	for _, n := range f.links[c] {
		if node, ok := f.live[n]; ok {
			f.durable[n] = slices.Clone(node.synced)

			continue
		}

		if _, ok := f.dirs[n]; ok {
			f.durableDirs[n] = struct{}{}
		}
	}

	delete(f.links, c)

	return nil
}

// Close implements [vfs.FS].
func (f *FS) Close() error { return nil }
