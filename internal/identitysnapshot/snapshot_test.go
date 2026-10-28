package identitysnapshot

import (
	"slices"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/index/series"
	"github.com/oteldb/storage/signal"
)

func TestCaptureAllowsRegistrationBetweenBatches(t *testing.T) {
	t.Parallel()
	var mu sync.RWMutex
	index := series.New()
	identity := func(i int) signal.Series {
		return signal.Series{Attributes: signal.NewAttributes(signal.KeyValue{
			Key: []byte("id"), Value: signal.StringValue([]byte(strconv.Itoa(i))),
		})}
	}
	ids := make([]signal.SeriesID, 3*batchSize+1)
	for i := range ids {
		ids[i] = index.Add(identity(i))
	}
	var got []signal.SeriesID
	Capture(mu.RLocker(), func() *series.Index { return index }, slices.Values(ids),
		func(id signal.SeriesID, view series.View, ref uint32) {
			assert.Equal(t, len(ids), view.Len())
			assert.Equal(t, id, view.At(int(ref), nil).Hash())
			// emit is outside the reader lock; additions cannot change captured identities.
			mu.Lock()
			index.Add(identity(len(ids) + len(got)))
			mu.Unlock()
			got = append(got, id)
		})
	assert.Equal(t, ids, got)
}

type countedLock struct {
	sync.Locker

	calls int
}

func (l *countedLock) Lock() {
	l.Locker.Lock()
	l.calls++
}

func captureIdentity(i int) signal.Series {
	return signal.Series{Attributes: signal.NewAttributes(signal.KeyValue{
		Key: []byte("id"), Value: signal.IntValue(int64(i)),
	})}
}

func TestCaptureMapDenseSparseEquivalenceDuringRegistration(t *testing.T) {
	t.Parallel()
	const total = 2*batchSize + 1
	for _, selectedCount := range []int{0, 1, batchSize / 2, batchSize/2 + 1, batchSize, batchSize + 1, total} {
		t.Run(strconv.Itoa(selectedCount), func(t *testing.T) {
			t.Parallel()
			index := series.New()
			want := make(map[signal.SeriesID]uint32, selectedCount)
			selected := make(map[signal.SeriesID]bool, selectedCount)
			for i := range total {
				id := index.Add(captureIdentity(i))
				if i < selectedCount {
					selected[id], want[id] = true, uint32(i)
				}
			}
			var mu sync.RWMutex
			lock := &countedLock{Locker: mu.RLocker()}
			got := make(map[signal.SeriesID]uint32, selectedCount)
			CaptureMap(lock, func() *series.Index { return index }, selected,
				func(id signal.SeriesID, view series.View, ref uint32) {
					assert.Equal(t, total, view.Len())
					assert.Equal(t, id, view.At(int(ref), nil).Hash())
					got[id] = ref
					mu.Lock()
					index.Add(captureIdentity(total + len(got)))
					mu.Unlock()
				})
			assert.Equal(t, want, got)
			calls := 1
			if selectedCount < total/4+min(total%4, 1) {
				calls += (selectedCount + batchSize - 1) / batchSize
			}
			assert.Equal(t, calls, lock.calls)
		})
	}
}

func TestDenseCaptureContinuesWhileWriterHoldsLock(t *testing.T) {
	t.Parallel()
	index := series.New()
	selected := make(map[signal.SeriesID]bool)
	for i := range 2 * batchSize {
		selected[index.Add(captureIdentity(i))] = true
	}
	var mu sync.RWMutex
	ready, resume := make(chan struct{}), make(chan struct{})
	done := make(chan int, 1)
	go func() {
		n := 0
		CaptureMap(mu.RLocker(), func() *series.Index { return index }, selected,
			func(signal.SeriesID, series.View, uint32) {
				if n == 0 {
					close(ready)
					<-resume
				}
				n++
			})
		done <- n
	}()
	<-ready
	mu.Lock()
	defer mu.Unlock()
	index.Add(captureIdentity(99999))
	close(resume)
	select {
	case n := <-done:
		assert.Equal(t, len(selected), n)
	case <-time.After(10 * time.Second):
		t.Fatal("dense capture reacquired the mutable registry lock")
	}
}

func TestCapturePreservesRequestedOrder(t *testing.T) {
	t.Parallel()
	index := series.New()
	ids := make([]signal.SeriesID, 2*batchSize+1)
	for i := range ids {
		ids[i] = index.Add(captureIdentity(i))
	}
	slices.Reverse(ids)
	var mu sync.RWMutex
	var got []signal.SeriesID
	Capture(mu.RLocker(), func() *series.Index { return index }, slices.Values(ids),
		func(id signal.SeriesID, view series.View, ref uint32) {
			require.Equal(t, id, view.ID(int(ref)))
			got = append(got, id)
		})
	assert.Equal(t, ids, got)
}
