// Package profile is the storage tier's EXPLAIN ANALYZE: a profiled tree of the fetch operators
// with per-node timing and I/O counters, showing where and how much time a query spent (which parts
// were scanned vs pruned, rows in/out, bytes decoded). The library owns the fetch-tier subtree; an
// embedder's query engine splices it under its own language operators (DESIGN §16.5).
//
// It is opt-in and zero-overhead when off: a caller installs a collector with [WithCollector] and
// reads [Collector.Root] after the fetch; operators call [Begin] (a no-op when no collector is in
// ctx, so the default fetch path makes no timing reads or allocations). It is safe for the
// concurrent fan-out a split/cluster fetch performs.
package profile

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"
)

// Node is one operator in the profiled query tree.
type Node struct {
	Name     string           // operator name (e.g. "engine.fetch", "part-scan")
	Dur      time.Duration    // wall time of this operator (including children)
	Counters map[string]int64 // I/O counters (rows, parts_scanned, bytes_decoded, …)
	Children []*Node
}

// SelfDur is the time spent in this node excluding its children.
func (n *Node) SelfDur() time.Duration {
	self := n.Dur
	for _, c := range n.Children {
		self -= c.Dur
	}

	if self < 0 {
		self = 0 // concurrent children can sum past the parent's wall time
	}

	return self
}

// Collector accumulates a profile tree. Safe for concurrent Begin/End across fan-out sub-fetches.
type Collector struct {
	mu   sync.Mutex
	root *Node
}

// Root returns the completed tree (call after the fetch finishes).
func (c *Collector) Root() *Node { return c.root }

type ctxKey struct{}

// Handle is an open operator node; End it (defer) when the operator finishes, and Add counters to it.
// A nil Handle is valid and no-ops, which is what [Begin] returns when no collector is installed.
type Handle struct {
	c     *Collector
	node  *Node
	start time.Time
}

// WithCollector installs a fresh collector rooted at a "query" node and returns it. Operators below
// (via [Begin]) attach to this root. Reading [Collector.Root] after the fetch yields the tree.
func WithCollector(ctx context.Context) (context.Context, *Collector) {
	c := &Collector{root: &Node{Name: "query"}}
	root := &Handle{c: c, node: c.root, start: time.Now()}

	return context.WithValue(ctx, ctxKey{}, root), c
}

// Begin starts a child operator named name under the current node in ctx and returns the child ctx
// (so nested Begin calls attach beneath it) and a [Handle] to End/Add. When no collector is in ctx
// it returns ctx unchanged and a nil Handle, so instrumented operators cost nothing off the
// profiling path.
func Begin(ctx context.Context, name string) (context.Context, *Handle) {
	parent, ok := ctx.Value(ctxKey{}).(*Handle)
	if !ok || parent == nil {
		return ctx, nil
	}

	child := &Node{Name: name}

	parent.c.mu.Lock()
	parent.node.Children = append(parent.node.Children, child)
	parent.c.mu.Unlock()

	h := &Handle{c: parent.c, node: child, start: time.Now()}

	return context.WithValue(ctx, ctxKey{}, h), h
}

// End records the operator's wall time. Safe to call on a nil Handle.
func (h *Handle) End() {
	if h == nil {
		return
	}

	h.c.mu.Lock()
	h.node.Dur = time.Since(h.start)
	h.c.mu.Unlock()
}

// Add increments a counter on the operator (rows, parts, bytes…). Safe on a nil Handle; a zero delta
// is ignored.
func (h *Handle) Add(key string, n int64) {
	if h == nil || n == 0 {
		return
	}

	h.c.mu.Lock()
	if h.node.Counters == nil {
		h.node.Counters = make(map[string]int64, 4)
	}

	h.node.Counters[key] += n
	h.c.mu.Unlock()
}

// Graft attaches an already-built subtree (e.g. a peer's profile returned over the read RPC) as a
// child of the current node in ctx. No-op when no collector is installed or sub is nil.
func Graft(ctx context.Context, sub *Node) {
	parent, ok := ctx.Value(ctxKey{}).(*Handle)
	if !ok || parent == nil || sub == nil {
		return
	}

	parent.c.mu.Lock()
	parent.node.Children = append(parent.node.Children, sub)
	parent.c.mu.Unlock()
}

// Render returns the tree as an EXPLAIN ANALYZE-style indented listing: per node its name, wall
// time, self time (when it has children), and sorted counters, one node per line.
func (n *Node) Render() string {
	var b strings.Builder
	n.render(&b, "", true, true)

	return b.String()
}

func (n *Node) render(b *strings.Builder, prefix string, isRoot, isLast bool) {
	connector := ""
	if !isRoot {
		connector = "├─ "
		if isLast {
			connector = "└─ "
		}
	}

	fmt.Fprintf(b, "%s%s%s  %s", prefix, connector, n.Name, n.Dur.Round(time.Microsecond))

	if len(n.Children) > 0 {
		fmt.Fprintf(b, " (self %s)", n.SelfDur().Round(time.Microsecond))
	}

	for _, k := range sortedKeys(n.Counters) {
		fmt.Fprintf(b, " %s=%d", k, n.Counters[k])
	}

	b.WriteByte('\n')

	childPrefix := prefix
	if !isRoot {
		childPrefix += "│  "
		if isLast {
			childPrefix = prefix + "   "
		}
	}

	for i, c := range n.Children {
		c.render(b, childPrefix, false, i == len(n.Children)-1)
	}
}

func sortedKeys(m map[string]int64) []string {
	if len(m) == 0 {
		return nil
	}

	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}

	sort.Strings(keys)

	return keys
}
