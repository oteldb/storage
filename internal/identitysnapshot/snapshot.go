// Package identitysnapshot captures append-only identity references in bounded lock sections.
package identitysnapshot

import (
	"iter"
	"maps"
	"sync"

	"github.com/oteldb/storage/index/series"
	"github.com/oteldb/storage/signal"
)

const batchSize = 256

// Capture resolves ids against one registry generation, calling emit outside lock.
// The caller prevents registry replacement for the entire call; additions may continue.
// ids must name identities registered before capture. current runs under lock.
func Capture(
	lock sync.Locker, current func() *series.Index, ids iter.Seq[signal.SeriesID],
	emit func(signal.SeriesID, series.View, uint32),
) {
	lock.Lock()
	index := current()
	view := index.View()
	lock.Unlock()
	captureReferences(lock, index, view, ids, emit)
}

// CaptureMap resolves an immutable selected set, without guaranteeing iteration order.
// The caller prevents registry replacement; selected IDs predate capture. emit runs outside lock.
func CaptureMap[V any](
	lock sync.Locker, current func() *series.Index, selected map[signal.SeriesID]V,
	emit func(signal.SeriesID, series.View, uint32),
) {
	lock.Lock()
	index := current()
	view := index.View()
	lock.Unlock()

	if len(selected) < view.Len()/4+min(view.Len()%4, 1) {
		captureReferences(lock, index, view, maps.Keys(selected), emit)
		return
	}
	// Dense flushes scan at most four times their selected cardinality, avoiding writer-preferred
	// lock reacquisition per chunk. Sparse selections retain bounded lookups into the registry.
	for i := range view.Len() {
		id := view.ID(i)
		if _, ok := selected[id]; ok {
			emit(id, view, uint32(i))
		}
	}
}

func captureReferences(
	lock sync.Locker, index *series.Index, view series.View, ids iter.Seq[signal.SeriesID],
	emit func(signal.SeriesID, series.View, uint32),
) {
	var batch [batchSize]signal.SeriesID
	var refs [batchSize]uint32
	var found [batchSize]bool
	n := 0
	flush := func() {
		lock.Lock()
		for i, id := range batch[:n] {
			refs[i], found[i] = index.Ref(id)
		}
		lock.Unlock()
		for i, id := range batch[:n] {
			if found[i] && int(refs[i]) < view.Len() {
				emit(id, view, refs[i])
			}
		}
		n = 0
	}
	for id := range ids {
		batch[n] = id
		n++
		if n == batchSize {
			flush()
		}
	}
	if n > 0 {
		flush()
	}
}
