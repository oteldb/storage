package series

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/signal"
)

func attrs(pairs ...string) signal.Attributes {
	kvs := make([]signal.KeyValue, 0, len(pairs)/2)
	for i := 0; i+1 < len(pairs); i += 2 {
		kvs = append(kvs, signal.KeyValue{Key: []byte(pairs[i]), Value: signal.StringValue([]byte(pairs[i+1]))})
	}

	return signal.NewAttributes(kvs...)
}

func TestAddIsIdempotent(t *testing.T) {
	t.Parallel()

	ix := New()
	id1 := ix.Add(attrs("job", "api", "inst", "1"))
	id2 := ix.Add(attrs("inst", "1", "job", "api")) // same set, different order

	assert.Equal(t, id1, id2, "equal attribute sets ⇒ same id")
	assert.Equal(t, 1, ix.Len(), "re-adding does not store a second copy")
}

func TestAddDistinctSeries(t *testing.T) {
	t.Parallel()

	ix := New()
	a := ix.Add(attrs("job", "api"))
	b := ix.Add(attrs("job", "web"))

	assert.NotEqual(t, a, b)
	assert.Equal(t, 2, ix.Len())
}

func TestGetReconstructs(t *testing.T) {
	t.Parallel()

	ix := New()
	want := attrs("job", "api", "inst", "1")
	id := ix.Add(want)

	got, ok := ix.Get(id)
	require.True(t, ok)
	assert.True(t, got.Equal(want))

	_, ok = ix.Get(signal.SeriesID{Lo: 123})
	assert.False(t, ok)
	assert.True(t, ix.Has(id))
	assert.False(t, ix.Has(signal.SeriesID{Lo: 123}))
}

func TestAddRetainsACopy(t *testing.T) {
	t.Parallel()

	ix := New()
	key := []byte("job")
	val := []byte("api")
	id := ix.Add(signal.NewAttributes(signal.KeyValue{Key: key, Value: signal.StringValue(val)}))

	key[0] = 'X' // mutate caller buffers after Add
	val[0] = 'Y'

	got, _ := ix.Get(id)
	want := attrs("job", "api")
	assert.True(t, got.Equal(want), "the index must retain a deep copy")
}

func TestForEach(t *testing.T) {
	t.Parallel()

	ix := New()
	ix.Add(attrs("job", "api"))
	ix.Add(attrs("job", "web"))

	seen := map[signal.SeriesID]bool{}
	ix.ForEach(func(id signal.SeriesID, _ signal.Attributes) { seen[id] = true })
	assert.Len(t, seen, 2)
}
