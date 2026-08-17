package backend

// NodeLocal is the optional capability of reporting that a backend's objects live on a medium
// private to the process that writes them, so a peer cannot read them. [Memory] and a local
// directory tree implement it; an object store does not.
//
// It reports the medium, not the deployment, and is therefore a heuristic in exactly one
// direction: a `file` backend rooted on a shared mount (NFS, a clustered filesystem) is a
// legitimate shared store and still answers true. Read a true answer as "assume private unless the
// operator declares otherwise", never as proof — a false answer is exact.
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
