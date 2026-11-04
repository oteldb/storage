package engine

// ReadRevision identifies one engine's reader-visible state. It is comparable and process-local.
type ReadRevision struct {
	engine  *Engine
	version uint64
}

// ReadRevision returns a token changed by every writer publication, including reset and reload.
func (e *Engine) ReadRevision() (ReadRevision, bool) {
	version, ok := e.mu.Revision()
	return ReadRevision{engine: e, version: version}, ok
}
