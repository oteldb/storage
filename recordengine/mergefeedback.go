package recordengine

import (
	"cmp"
	"slices"
	"sync"

	"github.com/oteldb/storage/backend"
	"github.com/oteldb/storage/encoding/compress"
	"github.com/oteldb/storage/internal/timebucket"
)

type mergeProfile struct {
	memory, window int64
	granule, frame int
	algorithm      compress.Algorithm
	level          compress.Level
	streaming      bool
}

func (e *Engine) mergeProfile() mergeProfile {
	return mergeProfile{memory: e.mergeMemoryBudgetBytes(), window: e.mergeReadWindow,
		granule: e.mergeGranule, frame: e.mergeFrameBytes, algorithm: e.cfg.MergeCompression,
		level: e.cfg.MergeCompressionLevel, streaming: backend.StreamsWrites(e.cfg.Backend)}
}

type mergeRunKey struct {
	parts [maxTierParts]*part
	n     int
}

func mergeKey(parts []*part) mergeRunKey {
	var key mergeRunKey
	key.n = len(parts)
	copy(key.parts[:], parts)
	slices.SortFunc(key.parts[:key.n], func(a, b *part) int { return cmp.Compare(a.prefix, b.prefix) })
	return key
}

type mergeObservation struct {
	profile     mergeProfile
	capBytes    int64
	outputs     int
	diskLimited bool
}

type mergeFeedback struct {
	mu   sync.RWMutex
	runs map[mergeRunKey]mergeObservation
}

func (f *mergeFeedback) empty() bool {
	f.mu.RLock()
	defer f.mu.RUnlock()
	return len(f.runs) == 0
}

func (f *mergeFeedback) expected(parts []*part, profile mergeProfile, capBytes int64) int {
	f.mu.RLock()
	o, ok := f.runs[mergeKey(parts)]
	f.mu.RUnlock()
	if !ok || o.profile != profile {
		return 1
	}
	if o.diskLimited && (capBytes == 0 || (o.capBytes > 0 && capBytes > o.capBytes)) {
		return 1
	}
	return o.outputs
}

func (f *mergeFeedback) record(parts []*part, profile mergeProfile, capBytes int64, outputs []*part) {
	o := mergeObservation{profile: profile, capBytes: capBytes, outputs: len(outputs)}
	for _, p := range outputs {
		o.diskLimited = o.diskLimited || p.mergeDiskSealed
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.runs == nil {
		f.runs = make(map[mergeRunKey]mergeObservation)
	}
	f.runs[mergeKey(parts)] = o
}

// Only mutations prune their current part set; an inspection snapshot may already be stale.
func (f *mergeFeedback) prune(parts []*part) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if len(f.runs) == 0 {
		return
	}
	live := make(map[*part]struct{}, len(parts))
	for _, p := range parts {
		live[p] = struct{}{}
	}
	for key := range f.runs {
		keep := true
		for i := 0; keep && i < key.n; i++ {
			_, keep = live[key.parts[i]]
		}
		if !keep {
			delete(f.runs, key)
		}
	}
}

func mergeSized(parts []*part) bool {
	for _, p := range parts {
		columns := p.reader.Manifest().Columns
		for i := range columns {
			if !columns[i].Const && !columns[i].HasSizing {
				return false
			}
		}
	}
	return true
}

func (e *Engine) usefulMerge(src, out []*part) bool {
	if len(out) < len(src) || partRows(out) < partRows(src) {
		return true
	}
	if timebucket.SplitsFrom(src, partSpan, minInt64) {
		return true
	}
	if len(out) <= len(src) && !mergeSized(src) && mergeSized(out) {
		return true
	}
	pending := false
	for _, p := range src {
		pending = pending || e.compressionForces(p)
	}
	if !pending {
		return false
	}
	return !slices.ContainsFunc(out, e.compressionForces)
}
