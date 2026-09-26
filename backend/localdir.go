package backend

// LocalDir is the optional capability of a backend whose objects are files under a local directory:
// Dir returns that directory's absolute path, or "" when there is none. It lets the embedder-facing
// layer refuse to place other files of its own (the WAL) inside the backend's tree.
//
// A wrapper around a [Backend] must forward it, or the capability is silently lost.
type LocalDir interface {
	Dir() string
}

// DirOf returns the local directory b keeps its objects under; false for a backend that does not
// implement [LocalDir] or reports no directory.
func DirOf(b Backend) (string, bool) {
	l, ok := b.(LocalDir)
	if !ok {
		return "", false
	}

	dir := l.Dir()

	return dir, dir != ""
}
