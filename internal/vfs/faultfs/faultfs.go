// Package faultfs is a [vfs.FS] that models what a machine loses when it stops without warning, so
// a durability claim can be tested instead of argued.
//
// A real filesystem makes two separate promises, and the bugs live in the gap between them: a
// file's bytes reach the disk on fsync, while the *name* that reaches those bytes reaches the disk
// only when its directory is synced. Code that writes a temp file, fsyncs it and renames it into
// place has done the first and not the second — after a power cut the bytes are there and nothing
// names them. This package keeps the two apart and, on [FS.Crash], returns the filesystem a
// pessimistic-but-legal machine would come back with.
//
// Pessimistic is the point: where a real disk *may* have persisted an unsynced write, Crash decides
// it did not. That is the worst outcome the POSIX contract permits, so code that survives it
// survives any filesystem, and a test that passes here does not depend on the ordering a particular
// device happened to give it.
//
// It also injects faults, in the shape [faultbackend] uses one level up: a [Rule] fails or suspends
// the operations it matches, so an error path or an interleaving is stated by the test rather than
// raced for.
package faultfs

import (
	"io/fs"
	"slices"
	"sync"
	"time"
)

// Op is the filesystem operation a [Rule] matches.
type Op int

// The operations a rule can match.
const (
	OpOpen Op = iota
	OpCreate
	OpRead
	OpWrite
	OpSync
	OpSyncDir
	OpRename
	OpLink
	OpRemove
	OpMkdir
	OpStat
	OpReadDir
)

// String implements [fmt.Stringer].
func (o Op) String() string {
	switch o {
	case OpOpen:
		return "open"
	case OpCreate:
		return "create"
	case OpRead:
		return "read"
	case OpWrite:
		return "write"
	case OpSync:
		return "sync"
	case OpSyncDir:
		return "sync-dir"
	case OpRename:
		return "rename"
	case OpLink:
		return "link"
	case OpRemove:
		return "remove"
	case OpMkdir:
		return "mkdir"
	case OpStat:
		return "stat"
	case OpReadDir:
		return "read-dir"
	default:
		return "unknown"
	}
}

// Call is one filesystem operation offered to a [Rule]. To for a rename or link is the destination.
type Call struct {
	Op   Op
	Name string
	To   string
}

// Rule decides what happens to the operations it matches. A rule with no Match matches every
// operation of its Op.
type Rule struct {
	Op    Op
	Match func(Call) bool
	// Err, when non-nil, is returned instead of performing the operation.
	Err error
	// Before, when non-nil, runs before the operation. It may block, which suspends the calling
	// goroutine inside the filesystem (see [Gate]).
	Before func(Call)
	// Times limits how many operations the rule applies to. Zero ⇒ unlimited.
	Times int

	fired int
}

// node is one file: what a reader sees now, and what the last [vfs.File.Sync] committed.
type node struct {
	data   []byte
	synced []byte
	perm   fs.FileMode
	mod    time.Time
}

// FS is an in-memory [vfs.FS] with a durability model and fault injection. The zero value is not
// usable; call [New].
type FS struct {
	mu sync.Mutex

	// live is what a reader sees now; durable is what a power cut would leave behind — a name is
	// present there only once its directory was synced, and its bytes are those of the last file
	// sync, so the two maps diverge exactly where a durability bug lives.
	live    map[string]*node
	durable map[string][]byte

	dirs        map[string]fs.FileMode
	durableDirs map[string]struct{}

	// Name changes pending a directory sync, per directory: links records names that appeared,
	// unlinks names that went away. SyncDir applies them to durable and clears them.
	links   map[string][]string
	unlinks map[string][]string

	rules []*Rule
	log   []Call
}

// New returns an empty filesystem.
func New() *FS {
	return &FS{
		live:        map[string]*node{},
		durable:     map[string][]byte{},
		dirs:        map[string]fs.FileMode{".": 0o750},
		durableDirs: map[string]struct{}{".": {}},
		links:       map[string][]string{},
		unlinks:     map[string][]string{},
	}
}

// Add installs a rule. Rules are consulted in order; the first to match an operation decides it.
func (f *FS) Add(r Rule) *FS {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.rules = append(f.rules, &r)

	return f
}

// Reset removes every rule, leaving the filesystem's contents and its recorded calls in place.
func (f *FS) Reset() {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.rules = nil
}

// Calls returns the operations performed so far, in order.
func (f *FS) Calls() []Call {
	f.mu.Lock()
	defer f.mu.Unlock()

	return slices.Clone(f.log)
}

// intercept records the call and returns the rule deciding it, if any. Caller holds f.mu.
func (f *FS) intercept(c Call) *Rule {
	f.log = append(f.log, c)

	for _, r := range f.rules {
		if r.Op != c.Op || (r.Match != nil && !r.Match(c)) {
			continue
		}

		if r.Times > 0 && r.fired >= r.Times {
			continue
		}

		r.fired++

		return r
	}

	return nil
}

// enter applies the matching rule's Before hook and error. It releases f.mu across Before, so a
// blocking hook suspends this operation without deadlocking the filesystem.
func (f *FS) enter(c Call) error {
	f.mu.Lock()
	r := f.intercept(c)
	f.mu.Unlock()

	if r == nil {
		return nil
	}

	if r.Before != nil {
		r.Before(c)
	}

	return r.Err
}
