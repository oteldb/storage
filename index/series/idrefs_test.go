package series

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/signal"
)

func TestIDRefsCollisions(t *testing.T) {
	t.Parallel()
	rows := []row{
		{id: signal.SeriesID{}},
		{id: signal.SeriesID{Hi: 1}},
		{id: signal.SeriesID{Hi: 2}},
		{id: signal.SeriesID{Hi: 3, Lo: 7}},
		{id: signal.SeriesID{Hi: 4, Lo: 7}},
	}
	compact := &idRefs{}

	for ref := range rows {
		compact.put(rows[ref].id, uint32(ref), rows)
	}
	for repeat := range 2 {
		for ref := range rows {
			id := rows[ref].id
			got, ok := compact.get(id, rows)
			require.True(t, ok)
			require.Equal(t, uint32(ref), got)
			if repeat == 0 {
				compact.put(id, uint32(ref), rows)
			}
		}
	}
	for _, id := range []signal.SeriesID{{Hi: 99}, {Hi: 99, Lo: 7}, {Lo: 99}} {
		_, ok := compact.get(id, rows)
		require.False(t, ok)
	}
	require.Len(t, compact.primary, 2)
	require.Len(t, compact.collisions, 3)
}

func FuzzIDRefs(f *testing.F) {
	f.Add([]byte{0, 1, 2, 3, 4, 255})
	f.Add([]byte("forced collisions across high halves"))
	f.Fuzz(func(t *testing.T, data []byte) {
		if len(data) > 1024 {
			return
		}
		var refs idRefs
		var rows []row
		want := make(map[signal.SeriesID]uint32)
		for i, value := range data {
			id := signal.SeriesID{Hi: uint64(i), Lo: uint64(value % 4)}
			if _, ok := refs.get(id, rows); ok {
				t.Fatal("unknown ID resolved")
			}
			ref := uint32(len(rows))
			refs.put(id, ref, rows)
			rows = append(rows, row{id: id})
			want[id] = ref
		}
		for id, ref := range want {
			got, ok := refs.get(id, rows)
			if !ok || got != ref {
				t.Fatal("wrong reference")
			}
		}
		for lo := range 4 {
			if _, ok := refs.get(signal.SeriesID{Hi: 2048, Lo: uint64(lo)}, rows); ok {
				t.Fatal("false collision hit")
			}
		}
	})
}
