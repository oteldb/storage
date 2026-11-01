package engine

import "sync/atomic"

const (
	memoryDirectoryBits  = 4
	memoryDirectorySlots = 1 << memoryDirectoryBits
	memoryDirectoryDepth = 64 / memoryDirectoryBits
)

type memoryDirectoryNode struct {
	refs atomic.Int64

	children [memoryDirectorySlots]*memoryDirectoryNode
	run      *memoryRun
	owner    *memoryDirectoryPath
	rows     int64
	bytes    int64
	minTS    int64
	maxTS    int64
	runs     int
}

// The complete insertion path is allocated and charged before submitting the WAL write.
type memoryDirectoryPath struct {
	nodes [memoryDirectoryDepth + 1]memoryDirectoryNode
	live  atomic.Int64

	used      int
	closed    bool
	onRelease func()
}

func newMemoryDirectoryPath(onRelease func()) *memoryDirectoryPath {
	p := &memoryDirectoryPath{onRelease: onRelease}
	p.live.Store(1)
	return p
}

func (p *memoryDirectoryPath) take(src *memoryDirectoryNode) *memoryDirectoryNode {
	if p.closed || p.used == len(p.nodes) {
		panic("memory directory path exhausted or closed")
	}
	n := &p.nodes[p.used]
	p.used++
	p.live.Add(1)
	n.owner = p
	n.refs.Store(1)
	if src == nil {
		return n
	}
	n.children = src.children
	for _, child := range n.children {
		child.acquire()
	}
	n.run = src.run
	if n.run != nil {
		n.run.acquire()
	}
	return n
}

func (p *memoryDirectoryPath) close() {
	if p.closed {
		panic("close unowned memory directory path")
	}
	p.closed = true
	p.dropNode()
}

func (p *memoryDirectoryPath) dropNode() {
	if p.live.Add(-1) == 0 {
		if release := p.onRelease; release != nil {
			p.onRelease = nil
			release()
		}
	}
}

// The caller owns src throughout construction. The returned root has one owned reference.
// No allocation or fallible work occurs here; dropping src belongs outside publication ownership.
func (p *memoryDirectoryPath) insert(src *memoryDirectoryNode, key uint64, run *memoryRun) *memoryDirectoryNode {
	return p.insertAt(src, key, run, 0)
}

func (p *memoryDirectoryPath) insertAt(src *memoryDirectoryNode, key uint64, run *memoryRun, depth int) *memoryDirectoryNode {
	n := p.take(src)
	if depth == memoryDirectoryDepth {
		if n.run != nil {
			n.run.release()
		}
		run.acquire()
		n.run = run
	} else {
		shift := uint((memoryDirectoryDepth - depth - 1) * memoryDirectoryBits)
		slot := key >> shift & (memoryDirectorySlots - 1)
		old := n.children[slot]
		n.children[slot] = p.insertAt(old, key, run, depth+1)
		old.release()
	}
	n.refresh()
	return n
}

// Join an older sealed generation to newer active keys without rewriting numeric columns.
// Every key in older must precede every key in newer.
func (p *memoryDirectoryPath) concat(older, newer *memoryDirectoryNode) *memoryDirectoryNode {
	if older == nil {
		newer.acquire()
		return newer
	}
	if newer == nil {
		older.acquire()
		return older
	}
	n := p.take(older)
	if n.run != nil || newer.run != nil {
		panic("overlapping memory directory generations")
	}
	for i, child := range newer.children {
		if child == nil {
			continue
		}
		old := n.children[i]
		if old == nil {
			child.acquire()
			n.children[i] = child
		} else {
			n.children[i] = p.concat(old, child)
			old.release()
		}
	}
	n.refresh()
	return n
}

func (n *memoryDirectoryNode) refresh() {
	n.rows, n.bytes, n.runs = 0, 0, 0
	if n.run != nil {
		n.rows, n.bytes, n.runs = n.run.rows, n.run.bytes, 1
		n.minTS, n.maxTS = n.run.minTS, n.run.maxTS
	}
	for _, child := range n.children {
		if child == nil || child.runs == 0 {
			continue
		}
		if n.runs == 0 {
			n.minTS, n.maxTS = child.minTS, child.maxTS
		} else {
			n.minTS, n.maxTS = min(n.minTS, child.minTS), max(n.maxTS, child.maxTS)
		}
		n.rows += child.rows
		n.bytes += child.bytes
		n.runs += child.runs
	}
}

func (n *memoryDirectoryNode) acquire() {
	if n == nil {
		return
	}
	for {
		refs := n.refs.Load()
		if refs <= 0 {
			panic("acquire released memory directory")
		}
		if n.refs.CompareAndSwap(refs, refs+1) {
			return
		}
	}
}

func (n *memoryDirectoryNode) release() {
	if n == nil {
		return
	}
	refs := n.refs.Add(-1)
	if refs < 0 {
		panic("release unowned memory directory")
	}
	if refs != 0 {
		return
	}
	for i, child := range n.children {
		n.children[i] = nil
		child.release()
	}
	if run := n.run; run != nil {
		n.run = nil
		run.release()
	}
	n.owner.dropNode()
}

// A pinned root owns every borrowed run visited in ascending publication order.
func (n *memoryDirectoryNode) walk(start, end int64, visit func(*memoryRun) bool) bool {
	if n == nil || n.maxTS < start || n.minTS > end {
		return true
	}
	if n.run != nil && !visit(n.run) {
		return false
	}
	for _, child := range n.children {
		if !child.walk(start, end, visit) {
			return false
		}
	}
	return true
}

func (n *memoryDirectoryNode) subtree(key uint64, depth int) *memoryDirectoryNode {
	for level := 0; n != nil && level < depth; level++ {
		shift := uint((memoryDirectoryDepth - level - 1) * memoryDirectoryBits)
		n = n.children[key>>shift&(memoryDirectorySlots-1)]
	}
	return n
}

func (p *memoryDirectoryPath) replaceSubtree(
	src *memoryDirectoryNode, key uint64, depth, level int, replacement *memoryDirectoryNode,
) *memoryDirectoryNode {
	if level == depth {
		replacement.acquire()
		return replacement
	}
	n := p.take(src)
	shift := uint((memoryDirectoryDepth - level - 1) * memoryDirectoryBits)
	slot := key >> shift & (memoryDirectorySlots - 1)
	old := n.children[slot]
	n.children[slot] = p.replaceSubtree(old, key, depth, level+1, replacement)
	old.release()
	n.refresh()
	return n
}
