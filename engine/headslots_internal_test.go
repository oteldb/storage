package engine

import (
	"math"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/signal"
)

func TestHeadSlots(t *testing.T) {
	t.Parallel()
	var slots headSlots
	for i := uint32(0); i <= 9*headSlotSize+1; i++ {
		require.Equal(t, i, slots.alloc())
	}
	for _, ref := range []uint32{0, headSlotSize - 1, headSlotSize, 9*headSlotSize + 1} {
		for _, ts := range []int64{0, -1, math.MinInt64, math.MaxInt64} {
			slots.set(ref, ts)
			got, seen := slots.get(ref)
			require.True(t, seen)
			require.Equal(t, ts, got)
		}
		slots.release(ref)
		_, seen := slots.get(ref)
		require.False(t, seen)
		require.Equal(t, ref, slots.alloc())
	}
	require.Zero(t, slots.seen)
	for _, p := range slots.pages {
		require.Nil(t, p)
	}
}

func TestHeadSlotsDetachReattachAndPrune(t *testing.T) {
	t.Parallel()
	h := newHead()
	dead, live, fresh := benchSeries(0), benchSeries(1), benchSeries(2)
	for _, s := range []signal.Series{dead, live} {
		h.registerSeries(s)
	}
	deadRef, _ := h.series.Ref(dead.Hash())
	deadSlot := h.stateRefs[deadRef]
	h.slots.set(deadSlot, 100)
	buf := h.bufFor(live.Hash())
	buf.appendSample(0, 1, 1)
	h.noteTS(live.Hash(), 0)
	h.grow(SampleBytes)
	require.Positive(t, h.buffers.sizeBytes())
	frozen, bytes, since := h.detach()
	require.Zero(t, h.buffers.sizeBytes())
	state := h.seriesState(live.Hash())
	require.Nil(t, state.buffer)
	require.True(t, state.seen)
	require.Zero(t, state.newest)
	liveSlot := h.stateRefs[state.ref]
	newer := h.bufFor(live.Hash())
	newer.appendSample(1, 2, 3)
	h.noteTS(live.Hash(), 1)
	h.grow(SampleBytes)
	h.reattach(frozen, bytes, since)
	state = h.seriesState(live.Hash())
	require.Same(t, h.samples[live.Hash()], state.buffer)
	require.Equal(t, []int64{0, 1}, state.buffer.ts)
	require.Equal(t, []float64{1, 3}, state.buffer.sf)
	require.EqualValues(t, 1, state.newest)

	snap := h.series.View()
	keep := map[signal.SeriesID]struct{}{live.Hash(): {}}
	rebuilt := rebuildIdentity(snap, keep, h.stateRefs)
	// Admission during the off-lock rebuild must remain visible after the swap.
	h.noteTS(live.Hash(), 10)
	h.swapIdentity(rebuilt, snap, h.series.View(), deadPositions(snap, keep))
	state = h.seriesState(live.Hash())
	require.Zero(t, state.ref)
	require.Equal(t, liveSlot, h.stateRefs[state.ref])
	require.EqualValues(t, 10, state.newest)
	require.Same(t, buf, state.buffer)
	require.False(t, h.seriesState(dead.Hash()).known)
	h.registerSeries(fresh)
	state = h.seriesState(fresh.Hash())
	require.Equal(t, deadSlot, h.stateRefs[state.ref])
	require.Nil(t, state.buffer)
	require.False(t, state.seen)
	id, accepted, logSeries := h.append(fresh, -100, 4, int64(time.Second))
	require.Equal(t, fresh.Hash(), id)
	require.True(t, logSeries)
	require.True(t, accepted, "reusing a slot cannot inherit the removed series' watermark")
}

func FuzzHeadSlotsLifetimes(f *testing.F) {
	for _, seed := range [][]byte{{0, 1, 16, 18, 3, 4}, {4, 1, 20, 2, 3, 8}, {0, 4, 8, 12, 1, 18, 3, 0, 4}} {
		f.Add(seed)
	}
	f.Fuzz(func(t *testing.T, operations []byte) {
		h := newHead()
		identities := make([]signal.Series, 8)
		for i := range identities {
			identities[i] = benchSeries(i)
		}
		watermarks := make(map[signal.SeriesID]int64)
		var frozen map[signal.SeriesID]*sampleBuf
		var frozenBytes int64
		var since time.Time
		for i, op := range operations[:min(len(operations), 256)] {
			s := identities[int(op>>2)%len(identities)]
			id := s.Hash()
			switch op & 3 {
			case 0:
				ts := int64(int8(op))
				newest, seen := watermarks[id]
				want := !seen || ts >= newest-16
				out, effective, logSeries, identity := h.appendByID(id, ts, float64(i), 1, 16, AppendLimits{}, func() signal.Series { return s })
				if effective != id || logSeries && identity.Hash() != id {
					t.Fatalf("step %d changed series identity", i)
				}
				if (out == admitted) != want {
					t.Fatalf("step %d admission=%v want=%v", i, out, want)
				}
				if want && (!seen || ts > newest) {
					watermarks[id] = ts
				}
			case 1:
				if frozen == nil {
					frozen, frozenBytes, since = h.detach()
				}
			case 2:
				if frozen != nil {
					if op&16 == 0 {
						h.reattach(frozen, frozenBytes, since)
					} else {
						h.detachedBytes, h.detachedKnown = 0, false
					}
					frozen = nil
				}
			case 3:
				if frozen == nil {
					live := make(map[signal.SeriesID]struct{}, len(h.samples))
					for id := range h.samples {
						live[id] = struct{}{}
					}
					snap := h.series.View()
					rebuilt := rebuildIdentity(snap, live, h.stateRefs)
					h.swapIdentity(rebuilt, snap, snap, deadPositions(snap, live))
					for id := range watermarks {
						if !h.series.Has(id) {
							delete(watermarks, id)
						}
					}
				}
			}
			for j := range identities {
				id := identities[j].Hash()
				state := h.seriesState(id)
				ref, known := h.series.Ref(id)
				want, seen := watermarks[id]
				if state.known != known || state.ref != ref || state.buffer != h.samples[id] || state.seen != seen || state.newest != want {
					t.Fatalf("step %d: inconsistent dense state for %v", i, id)
				}
			}
		}
	})
}
