package mergestreamtest

import (
	"github.com/oteldb/storage/backend"
	"github.com/oteldb/storage/backend/backendtest"
)

// ErrInjected is the failure [Backend] injects into a chosen write.
var ErrInjected = backendtest.ErrInjected

// Backend is an in-memory [backendtest.Counting]: it counts reads per key, can fail the n-th write,
// and forwards no optional capability, so every read a merge makes, a size probe included, is seen.
type Backend = backendtest.Counting

// NewBackend returns a Backend over [backend.Memory].
func NewBackend() *Backend { return backendtest.NewCounting(backend.Memory()) }
