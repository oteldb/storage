// Package identitysnapshot captures append-only identity references in bounded lock sections.
package identitysnapshot

import (
	"iter"
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
