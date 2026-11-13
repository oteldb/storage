package series

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/signal"
)

type uint64RowRefs struct {
	primary    map[uint64]uint32
	collisions map[signal.SeriesID]uint32
}

func newUint64RowRefs() *uint64RowRefs {
	return &uint64RowRefs{primary: make(map[uint64]uint32)}
}

func (m *uint64RowRefs) put(id signal.SeriesID, ref uint32, rows []row) {
	prior, ok := m.primary[id.Lo]
	if !ok || rows[prior].id == id {
		m.primary[id.Lo] = ref
		return
	}
	if m.collisions == nil {
		m.collisions = make(map[signal.SeriesID]uint32)
	}
	m.collisions[id] = ref
}

func (m *uint64RowRefs) get(id signal.SeriesID, rows []row) (uint32, bool) {
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

type uint64HighRef struct {
	high uint64
	ref  uint32
}

type uint64InlineRefs struct {
	primary    map[uint64]uint64HighRef
	collisions map[signal.SeriesID]uint32
}

func newUint64InlineRefs() *uint64InlineRefs {
	return &uint64InlineRefs{primary: make(map[uint64]uint64HighRef)}
}

func (m *uint64InlineRefs) put(id signal.SeriesID, ref uint32) {
	prior, ok := m.primary[id.Lo]
	if !ok || prior.high == id.Hi {
		m.primary[id.Lo] = uint64HighRef{high: id.Hi, ref: ref}
		return
	}
	if m.collisions == nil {
		m.collisions = make(map[signal.SeriesID]uint32)
	}
	m.collisions[id] = ref
}

func (m *uint64InlineRefs) get(id signal.SeriesID) (uint32, bool) {
	prior, ok := m.primary[id.Lo]
	if !ok {
		return 0, false
	}
	if prior.high == id.Hi {
		return prior.ref, true
	}
	ref, ok := m.collisions[id]
	return ref, ok
}

func TestUint64LookupCollisions(t *testing.T) {
	t.Parallel()
	rows := []row{{id: signal.SeriesID{}}, {id: signal.SeriesID{Hi: 1}}, {id: signal.SeriesID{Hi: 2}}, {id: signal.SeriesID{Hi: 3, Lo: 7}}, {id: signal.SeriesID{Hi: 4, Lo: 7}}}
	compact := newUint64RowRefs()
	inline := newUint64InlineRefs()
	for ref := range rows {
		compact.put(rows[ref].id, uint32(ref), rows)
		inline.put(rows[ref].id, uint32(ref))
	}
	for repeat := range 2 {
		for ref := range rows {
			id := rows[ref].id
			got, ok := compact.get(id, rows)
			require.True(t, ok)
			require.Equal(t, uint32(ref), got)
			got, ok = inline.get(id)
			require.True(t, ok)
			require.Equal(t, uint32(ref), got)
			if repeat == 0 {
				compact.put(id, uint32(ref), rows)
				inline.put(id, uint32(ref))
			}
		}
	}
	for _, id := range []signal.SeriesID{{Hi: 99}, {Hi: 99, Lo: 7}, {Lo: 99}} {
		_, ok := compact.get(id, rows)
		require.False(t, ok)
		_, ok = inline.get(id)
		require.False(t, ok)
	}
	require.Len(t, compact.primary, 2)
	require.Len(t, compact.collisions, 3)
	require.Len(t, inline.primary, 2)
	require.Len(t, inline.collisions, 3)
}

func FuzzUint64Lookup(f *testing.F) {
	f.Add([]byte{0, 1, 2, 3, 4, 255})
	f.Add([]byte("many high halves with the same low half"))
	f.Fuzz(func(t *testing.T, data []byte) {
		if len(data) > 1024 {
			return
		}
		compact := newUint64RowRefs()
		inline := newUint64InlineRefs()
		want := make(map[signal.SeriesID]uint32)
		rows := make([]row, 0, len(data))
		for i, value := range data {
			id := signal.SeriesID{Hi: uint64(i), Lo: uint64(value % 4)}
			ref := uint32(len(rows))
			rows = append(rows, row{id: id})
			compact.put(id, ref, rows)
			inline.put(id, ref)
			want[id] = ref
		}
		for id, ref := range want {
			a, aok := compact.get(id, rows)
			b, bok := inline.get(id)
			if !aok || !bok || a != ref || b != ref {
				t.Fatal("identity collision changed reference")
			}
		}
	})
}
