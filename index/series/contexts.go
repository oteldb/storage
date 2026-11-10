package series

import (
	"sync"
	"sync/atomic"

	"github.com/oteldb/storage/internal/buildbudget"
	"github.com/oteldb/storage/internal/memsize"
	"github.com/oteldb/storage/signal"
)

const contextShards = 64

// Contexts shares immutable resource/scope payloads across indexes within a byte budget.
// It never evicts or recycles borrowed storage; full shards fall back to index-owned copies.
type Contexts struct {
	shards [contextShards]contextShard
	limit  int64
	bytes  atomic.Int64
}

type contextShard struct {
	mu        sync.Mutex
	resources map[string]signal.Resource
	scopes    map[string]signal.Scope
	bytes     int64
}

// NewContexts returns a bounded context pool; budgets below its fixed footprint disable sharing.
func NewContexts(maxBytes int64) *Contexts {
	base := memsize.Of[Contexts]()
	if maxBytes <= base {
		return nil
	}
	c := &Contexts{limit: (maxBytes - base) / contextShards}
	c.bytes.Store(base)
	return c
}

// SizeBytes reports conservatively charged retained bytes, including the fixed shard state.
func (c *Contexts) SizeBytes() int64 {
	if c == nil {
		return 0
	}
	return c.bytes.Load()
}

func (c *Contexts) resource(key []byte, r signal.Resource, own func() signal.Resource) (signal.Resource, bool) {
	if c == nil {
		return own(), false
	}
	s := &c.shards[signal.HashBytes(key).Lo%contextShards]
	s.mu.Lock()
	if stored, ok := s.resources[string(key)]; ok {
		s.mu.Unlock()
		return stored, true
	}
	cost := buildbudget.Add(int64(len(key)+len(r.SchemaURL)), buildbudget.Attributes(r.Attributes))
	cost = buildbudget.Mul(cost, 2)
	cost = buildbudget.Add(cost, 8*memsize.MapEntry[string, signal.Resource]())
	if s.resources == nil {
		cost = buildbudget.Add(cost, memsize.MapBase)
	}
	if cost > c.limit-s.bytes {
		s.mu.Unlock()
		return own(), false
	}
	stored := own()
	if s.resources == nil {
		s.resources = make(map[string]signal.Resource)
	}
	s.resources[string(key)] = stored
	s.bytes += cost
	c.bytes.Add(cost)
	s.mu.Unlock()
	return stored, true
}

func (c *Contexts) scope(key []byte, sc signal.Scope, own func() signal.Scope) (signal.Scope, bool) {
	if c == nil {
		return own(), false
	}
	s := &c.shards[signal.HashBytes(key).Lo%contextShards]
	s.mu.Lock()
	if stored, ok := s.scopes[string(key)]; ok {
		s.mu.Unlock()
		return stored, true
	}
	cost := buildbudget.Add(int64(len(key)+len(sc.Name)+len(sc.Version)+len(sc.SchemaURL)), buildbudget.Attributes(sc.Attributes))
	cost = buildbudget.Mul(cost, 2)
	cost = buildbudget.Add(cost, 8*memsize.MapEntry[string, signal.Scope]())
	if s.scopes == nil {
		cost = buildbudget.Add(cost, memsize.MapBase)
	}
	if cost > c.limit-s.bytes {
		s.mu.Unlock()
		return own(), false
	}
	stored := own()
	if s.scopes == nil {
		s.scopes = make(map[string]signal.Scope)
	}
	s.scopes[string(key)] = stored
	s.bytes += cost
	c.bytes.Add(cost)
	s.mu.Unlock()
	return stored, true
}
