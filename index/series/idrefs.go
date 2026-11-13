package series

import (
	"github.com/oteldb/storage/internal/memsize"
	"github.com/oteldb/storage/signal"
)

type idRefs struct {
	primary    map[uint64]uint32
	collisions map[signal.SeriesID]uint32
}

func (m *idRefs) get(id signal.SeriesID, rows []row) (uint32, bool) {
	ref, ok := m.primary[id.Lo]
	if !ok {
		return 0, false
	}
	if rows[ref].id.Hi == id.Hi {
		return ref, true
	}
	ref, ok = m.collisions[id]
	return ref, ok
}

func (m *idRefs) put(id signal.SeriesID, ref uint32, rows []row) {
	if m.primary == nil {
		m.primary = make(map[uint64]uint32)
	}
	prior, ok := m.primary[id.Lo]
	if !ok || rows[prior].id.Hi == id.Hi {
		m.primary[id.Lo] = ref
		return
	}
	if m.collisions == nil {
		m.collisions = make(map[signal.SeriesID]uint32)
	}
	m.collisions[id] = ref
}

func (m *idRefs) sizeBytes() int64 {
	// Map slots pad uint32 values to the key's eight-byte alignment.
	return int64(len(m.primary))*memsize.MapEntry[uint64, uint64]() + int64(len(m.collisions))*memsize.MapEntry[signal.SeriesID, uint64]()
}
