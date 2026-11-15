package memlimit

import "context"

// State describes a memory pool without retaining its queue.
type State struct {
	LimitBytes, UsedBytes int64
	Waiters               int
	HeadBytes             int64
}

// Wait describes one contended reservation and its live pool state.
type Wait struct {
	RequestedBytes, ReservedBytes int64
	State                         func() State
}

// WaitObserver wraps only queued admission; the callback reports forced admission and errors.
// Observers must call the callback once and preserve its result.
type WaitObserver func(context.Context, Wait, func() (bool, error)) (bool, error)
