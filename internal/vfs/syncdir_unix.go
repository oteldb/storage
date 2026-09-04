//go:build unix

package vfs

import "os"

// syncDir fsyncs the directory itself, committing the entries created, renamed, linked or removed
// in it. Opening a directory read-only and fsyncing that handle is the POSIX idiom; the bytes of
// the files it names are the files' own business.
func syncDir(root *os.Root, name string) error {
	d, err := root.Open(name)
	if err != nil {
		return err
	}

	err = d.Sync()
	if cerr := d.Close(); err == nil {
		err = cerr
	}

	return err
}
