package backend

// NodeLocal is the optional capability of reporting that a backend's objects live on a medium
// private to the process that writes them, so a peer cannot read them. [Memory] and a local
// directory tree implement it; an object store does not.
//
// It reports the medium, not the deployment. A `file` backend rooted on a shared mount (NFS, a
// clustered filesystem) answers true as well, which is the right answer rather than a false
// positive: a shared mount is not a supported shared store, because [Backend.CompareAndSwap] over a
// directory tree is process-local (see backend/file) and two nodes over one mount would lose index
// commits to each other in silence. A backend that genuinely is shared — an object store, or a
// wrapper declaring itself so — answers false, and that answer is exact.
//
// A wrapper around a [Backend] must forward it, or the capability is silently lost.
type NodeLocal interface {
	IsNodeLocal() bool
}

// IsNodeLocal reports whether b keeps its objects on a node-private medium; false for a backend
// that does not implement [NodeLocal]. See [NodeLocal] for the sense in which true is a heuristic.
func IsNodeLocal(b Backend) bool {
	l, ok := b.(NodeLocal)

	return ok && l.IsNodeLocal()
}
