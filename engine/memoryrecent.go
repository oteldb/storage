package engine

func (n *memoryDirectoryNode) walkEntries(key uint64, depth int, cutoff int64, visit func(uint64, *memoryRun)) {
	if n == nil || n.maxTS < cutoff {
		return
	}
	if n.run != nil {
		visit(key, n.run)
	}
	for i, child := range n.children {
		if child == nil {
			continue
		}
		shift := uint((memoryDirectoryDepth - depth - 1) * memoryDirectoryBits)
		child.walkEntries(key|uint64(i)<<shift, depth+1, cutoff, visit)
	}
}

// Whole runs are retained while any row is recent; readers clip their borrowed ranges.
func (e *Engine) keepRecentMemory(root *memoryDirectoryNode, cutoff int64) *memoryDirectoryNode {
	if root == nil || root.maxTS < cutoff {
		return nil
	}
	if root.minTS >= cutoff {
		root.acquire()
		return root
	}
	var kept *memoryDirectoryNode
	root.walkEntries(0, 0, cutoff, func(key uint64, run *memoryRun) {
		path := e.newMemoryPath()
		next := path.insert(kept, key, run)
		path.close()
		kept.release()
		kept = next
	})
	return kept
}

func (e *Engine) trimMemoryRecent(cutoff int64) {
	if !e.memoryPublicationEnabled() {
		return
	}
	e.viewMu.Lock()
	old := e.readView
	old.acquire()
	e.viewMu.Unlock()
	defer old.release()
	kept := e.keepRecentMemory(old.recent, cutoff)
	next := &memoryReadView{}
	e.viewMu.Lock()
	current := e.readView
	next.inherit(current)
	next.recent.release()
	next.recent = kept
	next.recentMin = max(old.recentMin, cutoff)
	e.readView = next
	e.viewMu.Unlock()
	current.release()
}

func (e *Engine) memoryRecentForFlush(view *memoryReadView) (*memoryDirectoryNode, int64) {
	if !e.recentEnabled() || view.frozen == nil {
		view.recent.acquire()
		return view.recent, view.recentMin
	}
	cutoff := e.head.maxNewest() - e.cfg.RecentWindow
	minimum := max(view.recentMin, cutoff)
	if view.recent == nil {
		minimum = max(view.frozen.minTS, cutoff)
		if view.parts != nil {
			for i := range view.parts.entries {
				entry := &view.parts.entries[i]
				if entry.maxTime < maxInt64 {
					minimum = max(minimum, entry.maxTime+1)
				}
			}
		}
	}
	path := e.newMemoryPath()
	joined := path.concat(view.recent, view.frozen)
	path.close()
	defer joined.release()
	return e.keepRecentMemory(joined, minimum), minimum
}

func (p *enginePlan) recentCovers(e *Engine, start int64) bool {
	if p.readView != nil {
		return p.readView.recent != nil && start >= p.readView.recentMin
	}
	return e.recentEnabled() && start >= e.recentMin
}
