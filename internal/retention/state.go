package retention

import "sync/atomic"

// Bound identifies the policy bound that sets a retention cutoff.
type Bound uint8

const (
	// BoundNone means no retention bound is set.
	BoundNone Bound = iota
	// BoundAge means the signal's age limit binds.
	BoundAge
	// BoundSignalBytes means the signal's stored-byte budget binds.
	BoundSignalBytes
	// BoundPooledBytes means the tenant's pooled stored-byte budget binds.
	BoundPooledBytes
)

// String returns the bound's stable display name.
func (b Bound) String() string {
	switch b {
	case BoundAge:
		return "age"
	case BoundSignalBytes:
		return "signal_bytes"
	case BoundPooledBytes:
		return "pooled_bytes"
	default:
		return "none"
	}
}

// Request is the most recent authorized retention request for one engine tenure.
type Request struct {
	Cutoff int64
	Bound  Bound
	Term   uint64
}

// State publishes a coherent retention request without joining merge execution.
type State struct{ request atomic.Pointer[Request] }

// Record publishes a request, including one whose physical cleanup may defer or fail.
func (s *State) Record(cutoff int64, bound Bound, term uint64) {
	if cutoff <= 0 {
		cutoff, bound = 0, BoundNone
	}
	next := Request{Cutoff: cutoff, Bound: bound, Term: term}
	for {
		old := s.request.Load()
		if old != nil && (old.Term > term || *old == next) {
			return
		}
		if s.request.CompareAndSwap(old, &next) {
			return
		}
	}
}

// Load returns the request only while its ownership term is current.
func (s *State) Load(term uint64) Request {
	if r := s.request.Load(); r != nil && r.Term == term {
		return *r
	}
	return Request{}
}
