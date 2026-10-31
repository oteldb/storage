// Package engineguard separates engine writer ordering from reader visibility.
package engineguard

import "sync"

// Mutex serializes mutations while allowing readers during a writer's blocking I/O.
// The zero value is ready to use. Like [sync.RWMutex], it must not be copied or locked recursively.
type Mutex struct {
	catalog sync.Mutex
	writer  sync.Mutex
	state   sync.RWMutex
}

// Lock acquires catalog ownership, then the writer gate and mutable state.
func (m *Mutex) Lock() {
	m.catalog.Lock()
	m.LockWriter()
}

// LockWriter orders ingestion and WAL operations without joining catalog publication.
func (m *Mutex) LockWriter() {
	m.writer.Lock()
	m.state.Lock()
}

// TryLockWriter acquires writer and state ownership if no other writer owns it; it may wait for readers.
func (m *Mutex) TryLockWriter() bool {
	if !m.writer.TryLock() {
		return false
	}
	m.state.Lock()
	return true
}

// Unlock releases state, writer, and catalog ownership.
func (m *Mutex) Unlock() {
	m.UnlockWriter()
	m.catalog.Unlock()
}

// UnlockWriter releases an ingestion or WAL operation.
func (m *Mutex) UnlockWriter() {
	m.state.Unlock()
	m.writer.Unlock()
}

// RLock acquires a state snapshot without joining the writer gate.
func (m *Mutex) RLock() { m.state.RLock() }

// RUnlock releases a state snapshot.
func (m *Mutex) RUnlock() { m.state.RUnlock() }

// RLocker returns the reader-facing lock.
func (m *Mutex) RLocker() sync.Locker { return m.state.RLocker() }

// LockState excludes readers without joining the writer gate, for read-side cache updates.
// Only derived caches independent of staged admission and WAL state may change under this lock.
func (m *Mutex) LockState() { m.state.Lock() }

// UnlockState releases a read-side cache update.
func (m *Mutex) UnlockState() { m.state.Unlock() }

// WithReaders temporarily admits readers while retaining exclusive writer ownership.
// The caller holds writer and state ownership; fn must not mutate reader-visible state or acquire this mutex.
func (m *Mutex) WithReaders(fn func() error) error {
	m.state.Unlock()
	defer m.state.Lock()
	return fn()
}

// WithWriters admits ingestion and readers during catalog I/O, retaining catalog ownership.
// The caller holds Lock; fn must not mutate engine state or acquire this mutex.
func (m *Mutex) WithWriters(fn func() error) error {
	m.UnlockWriter()
	defer m.LockWriter()
	return fn()
}
