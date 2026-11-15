package partid

import (
	"bytes"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestSequenceOrder(t *testing.T) {
	t.Parallel()
	before := New()
	a, b := NewSequence(), NewSequence()
	bid, ok := b.Next()
	require.True(t, ok)
	for range 100 {
		aid, ok := a.Next()
		require.True(t, ok)
		require.Negative(t, bytes.Compare(before[:], aid[:]))
		require.Negative(t, bytes.Compare(aid[:], bid[:]))
		before = aid
	}
	after := New()
	require.Negative(t, bytes.Compare(bid[:], after[:]))
}

func TestSequenceConcurrent(t *testing.T) {
	t.Parallel()
	s := NewSequence()
	ids := make(chan ID, 1024)
	var wg sync.WaitGroup
	for range 8 {
		wg.Go(func() {
			for range 128 {
				id, ok := s.Next()
				if !ok {
					panic("sequence exhausted")
				}
				ids <- id
			}
		})
	}
	wg.Wait()
	close(ids)
	seen := map[ID]bool{}
	for id := range ids {
		require.False(t, seen[id])
		seen[id] = true
	}
}

func TestSequenceExhaustion(t *testing.T) {
	t.Parallel()
	for _, n := range []byte{0, 1, 2} {
		t.Run(string(rune('0'+n)), func(t *testing.T) {
			t.Parallel()
			s := &Sequence{}
			for i := 6; i < len(s.next); i++ {
				s.next[i] = 255
			}
			s.next[len(s.next)-1] -= n
			for range int(n) + 1 {
				id, ok := s.Next()
				require.True(t, ok)
				require.Zero(t, id[5])
			}
			_, ok := s.Next()
			require.False(t, ok)
			_, ok = s.Next()
			require.False(t, ok)
		})
	}
}
