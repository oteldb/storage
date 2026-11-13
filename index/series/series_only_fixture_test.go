package series

import (
	"bytes"
	"strconv"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/signal"
)

const seriesOnlyBatchSize = 4096

type seriesOnlyFixture struct {
	seed      []signal.Series
	instances []signal.Value
	slots     []int
	batch     []signal.Series
	attrs     signal.Attributes
	stride    int
}

func newSeriesOnlyFixture(tb testing.TB, hosts int) *seriesOnlyFixture {
	tb.Helper()
	f := &seriesOnlyFixture{
		seed:      exporterSeed(tb),
		instances: make([]signal.Value, hosts),
		batch:     make([]signal.Series, seriesOnlyBatchSize),
	}
	f.slots = make([]int, len(f.seed))
	for i := range f.seed {
		f.stride = max(f.stride, len(f.seed[i].Attributes))
		f.slots[i] = -1
		for j := range f.seed[i].Attributes {
			if bytes.Equal(f.seed[i].Attributes[j].Key, []byte("instance")) {
				f.slots[i] = j
			}
		}
		require.NotEqual(tb, -1, f.slots[i])
	}
	f.attrs = make(signal.Attributes, f.stride*seriesOnlyBatchSize)
	for i := range f.instances {
		f.instances[i] = signal.StringValue([]byte("node-" + strconv.Itoa(i) + ":9100"))
	}
	return f
}

func (f *seriesOnlyFixture) count() int { return len(f.seed) * len(f.instances) }

// prepareSeriesBatch reuses input storage; registration must own any retained attributes.
func (f *seriesOnlyFixture) prepareSeriesBatch(offset int) []signal.Series {
	n := min(seriesOnlyBatchSize, f.count()-offset)
	for i := range n {
		index := offset + i
		seed, host := index%len(f.seed), index/len(f.seed)
		s := f.seed[seed]
		attrs := f.attrs[i*f.stride : i*f.stride+len(s.Attributes)]
		copy(attrs, s.Attributes)
		attrs[f.slots[seed]].Value = f.instances[host]
		s.Attributes = attrs
		f.batch[i] = s
	}
	return f.batch[:n]
}

func TestSeriesOnlyFixtureReuse(t *testing.T) { //nolint:paralleltest // AllocsPerRun requires exclusive runtime accounting.
	f := newSeriesOnlyFixture(t, 8)
	batch := f.prepareSeriesBatch(0)
	ix := New()
	want := batch[0].AppendHashInput(nil)
	id := ix.Add(batch[0])
	require.Zero(t, testing.AllocsPerRun(10, func() { f.prepareSeriesBatch(seriesOnlyBatchSize) }))
	ref, ok := ix.Ref(id)
	require.True(t, ok)
	require.Equal(t, want, ix.View().AppendCanonical(nil, int(ref)))
	require.Equal(t, 8*1658, f.count())
}
