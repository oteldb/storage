package engine

import "github.com/oteldb/storage/internal/memsize"

func (e *Engine) ownMemoryRun(run *memoryRun) {
	bytes := run.bytes
	prior := run.onRelease
	e.memoryBytes.Add(bytes)
	run.onRelease = func() {
		e.memoryBytes.Add(-bytes)
		if prior != nil {
			prior()
		}
	}
}

func (e *Engine) newMemoryPath() *memoryDirectoryPath {
	bytes := memsize.Of[memoryDirectoryPath]()
	e.memoryBytes.Add(bytes)
	return newMemoryDirectoryPath(func() { e.memoryBytes.Add(-bytes) })
}

// ResidentHeadBytes includes immutable buffers pinned by queries and maintenance, once per owner.
func (e *Engine) ResidentHeadBytes() int64 {
	e.mu.RLock()
	defer e.mu.RUnlock()
	return e.residentHeadBytesLocked()
}

func (e *Engine) residentHeadBytesLocked() int64 {
	if !e.head.memoryRuns {
		return e.head.inFlightBytes()
	}
	return e.head.bytes + e.head.detachedBytes - e.head.detachedRunBytes + e.memoryBytes.Load()
}
