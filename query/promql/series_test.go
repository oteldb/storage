package promql

import (
	"context"
	"errors"
	"sync"
	"testing"

	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/tsdb/chunkenc"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/query/fetch"
	"github.com/oteldb/storage/signal"
)

var (
	errSampleFetch       = errors.New("sample fetch is forbidden")
	errEnumerationWindow = errors.New("unexpected enumeration window")
)

type metadataFetcher struct {
	identities []signal.Series
	err        error
}

func (f metadataFetcher) Fetch(context.Context, fetch.Request) (fetch.Iterator, error) {
	return nil, errSampleFetch
}
func (f metadataFetcher) Series(_ context.Context, r fetch.Request) ([]signal.Series, error) {
	if r.Start != 1e6 || r.End != 10e6 {
		return nil, errEnumerationWindow
	}
	return f.identities, f.err
}

func TestSelectSeriesMetadata(t *testing.T) {
	t.Parallel()
	a, b := series("m", "a").Series, series("m", "b").Series
	hidden := series("m", "a").Series.Clone()
	hidden.Attributes = signal.NewAttributes(append(hidden.Attributes, signal.KeyValue{Key: []byte("__unit__"), Value: signal.StringValue([]byte("By"))})...)
	for _, tt := range []struct {
		name     string
		sorted   bool
		matchers []*labels.Matcher
		want     []string
	}{
		{"unsorted", false, nil, []string{"b", "a"}},
		{"sorted", true, nil, []string{"a", "b"}},
		{"full recheck", true, []*labels.Matcher{neq(t, "route", "b")}, []string{"a"}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			q, err := NewQueryable(metadataFetcher{identities: []signal.Series{b, a, a, hidden}}, "default").Querier(1, 10)
			require.NoError(t, err)
			t.Cleanup(func() { require.NoError(t, q.Close()) })
			ss := q.Select(t.Context(), tt.sorted, &storage.SelectHints{Func: "series", Start: 1, End: 10, Limit: 1}, tt.matchers...)
			var got []string
			for ss.Next() {
				s := ss.At()
				got = append(got, s.Labels().Get("route"))
				assert.Empty(t, s.Labels().Get("__unit__"))
				assert.Equal(t, chunkenc.ValNone, s.Iterator(nil).Next(), "metadata series carry no samples")
			}
			require.NoError(t, ss.Err())
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestSelectSeriesMetadataError(t *testing.T) {
	t.Parallel()
	want := assert.AnError
	q, err := NewQueryable(metadataFetcher{err: want}, "default").Querier(1, 10)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, q.Close()) })
	ss := q.Select(t.Context(), false, &storage.SelectHints{Func: "series"})
	assert.False(t, ss.Next())
	assert.ErrorIs(t, ss.Err(), want)
}

func TestSelectSeriesMetadataFallback(t *testing.T) {
	t.Parallel()
	f := &fakeFetcher{batches: []*fetch.Batch{series("m", "a", [2]int64{1, 2})}}
	q, err := NewQueryable(f, "default").Querier(1, 10)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, q.Close()) })
	ss := q.Select(t.Context(), false, &storage.SelectHints{Func: "series"})
	require.True(t, ss.Next())
	assert.Equal(t, "a", ss.At().Labels().Get("route"))
	assert.Equal(t, chunkenc.ValNone, ss.At().Iterator(nil).Next())
	assert.False(t, ss.Next())
	require.NoError(t, ss.Err())
	assert.Equal(t, int64(10e6), f.last.End)
}

func TestSelectSeriesMetadataConcurrent(t *testing.T) {
	t.Parallel()
	q, err := NewQueryable(metadataFetcher{identities: []signal.Series{series("m", "a").Series, series("m", "b").Series}}, "default").Querier(1, 10)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, q.Close()) })
	var wg sync.WaitGroup
	for range 8 {
		wg.Go(func() {
			ss := q.Select(t.Context(), true, &storage.SelectHints{Func: "series"})
			count := 0
			for ss.Next() {
				count++
			}
			require.NoError(t, ss.Err())
			assert.Equal(t, 2, count)
		})
	}
	wg.Wait()
}
