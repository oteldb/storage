package log

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/recordengine"
	"github.com/oteldb/storage/signal"
)

func TestBuilderShape(t *testing.T) {
	t.Parallel()

	var ld Logs
	rl := ld.AddResource()
	rl.Resource = signal.Resource{SchemaURL: []byte("schema")}
	sl := rl.AddScope()
	sl.Scope = signal.Scope{Name: []byte("lib")}
	r := sl.AddRecord()
	r.Timestamp = 7
	r.SeverityNumber = 9
	r.Body = []byte("hello")

	require.Len(t, ld.Resources, 1)
	require.Len(t, ld.Resources[0].Scopes, 1)
	require.Len(t, ld.Resources[0].Scopes[0].Records, 1)
	assert.Equal(t, []byte("schema"), ld.Resources[0].Resource.SchemaURL)
	assert.Equal(t, []byte("lib"), ld.Resources[0].Scopes[0].Scope.Name)
	assert.Equal(t, int64(7), ld.Resources[0].Scopes[0].Records[0].Timestamp)
	assert.Equal(t, []byte("hello"), ld.Resources[0].Scopes[0].Records[0].Body)
}

func TestResetRetainsCapacity(t *testing.T) {
	t.Parallel()

	var ld Logs
	rl := ld.AddResource()
	sl := rl.AddScope()
	sl.AddRecord()
	sl.AddRecord()

	ld.Reset()
	assert.Empty(t, ld.Resources)
	assert.GreaterOrEqual(t, cap(ld.Resources), 1, "Reset retains the resource backing array")

	sl2 := ld.AddResource().AddScope()
	assert.Empty(t, sl2.Records)
	assert.GreaterOrEqual(t, cap(sl2.Records), 2, "the record backing array is recycled")
}

// mkLogs builds a two-stream batch: stream A (svc=api) with 2 records, stream B (svc=web) with 1.
func mkLogs() Logs {
	var ld Logs

	rl := ld.AddResource()
	rl.Resource = signal.Resource{Attributes: signal.NewAttributes(
		signal.KeyValue{Key: []byte("service.name"), Value: signal.StringValue([]byte("api"))},
	)}
	sl := rl.AddScope()
	sl.Scope = signal.Scope{Name: []byte("lib")}
	r := sl.AddRecord()
	r.Timestamp, r.SeverityNumber, r.Body = 100, 9, []byte("first")
	r = sl.AddRecord()
	r.Timestamp, r.SeverityNumber, r.Body = 200, 17, []byte("second")

	rl2 := ld.AddResource()
	rl2.Resource = signal.Resource{Attributes: signal.NewAttributes(
		signal.KeyValue{Key: []byte("service.name"), Value: signal.StringValue([]byte("web"))},
	)}
	sl2 := rl2.AddScope()
	sl2.Scope = signal.Scope{Name: []byte("lib")}
	r = sl2.AddRecord()
	r.Timestamp, r.Body = 150, []byte("web log")

	return ld
}

func TestProjectEmitsOneBatchPerStream(t *testing.T) {
	t.Parallel()

	var (
		streams  []signal.SeriesID
		lens     []int
		bodies   [][]byte
		accepted int
	)

	accepted = Project(mkLogs(), func(b *recordengine.Batch) {
		streams = append(streams, b.Stream)
		lens = append(lens, b.Len())
		bodies = append(bodies, b.Bytes[bBody]...)
	})

	require.Len(t, streams, 2, "one batch per (resource, scope) stream")
	assert.Equal(t, 3, accepted, "total records emitted")
	assert.Equal(t, []int{2, 1}, lens)
	assert.NotEqual(t, streams[0], streams[1], "distinct streams hash distinctly")
	assert.Equal(t, [][]byte{[]byte("first"), []byte("second"), []byte("web log")}, bodies)
}

func TestProjectStreamIDMatchesIdentity(t *testing.T) {
	t.Parallel()

	Project(mkLogs(), func(b *recordengine.Batch) {
		assert.Equal(t, b.Identity().Hash(), b.Stream, "Stream equals the identity hash")
	})
}

func TestProjectFillsSchemaColumns(t *testing.T) {
	t.Parallel()

	var ld Logs
	rl := ld.AddResource()
	rl.Resource = signal.Resource{Attributes: signal.NewAttributes(
		signal.KeyValue{Key: []byte("service.name"), Value: signal.StringValue([]byte("api"))},
	)}
	sl := rl.AddScope()
	r := sl.AddRecord()
	r.Timestamp, r.ObservedTimestamp, r.SeverityNumber = 100, 101, 9
	r.SeverityText, r.Body, r.TraceID = []byte("INFO"), []byte("hello"), []byte("tid")
	r.Attributes = signal.NewAttributes(signal.KeyValue{Key: []byte("k"), Value: signal.StringValue([]byte("v"))})

	Project(ld, func(b *recordengine.Batch) {
		require.Equal(t, []int64{100}, b.Ts)
		assert.Equal(t, int64(101), b.Ints[iObserved][0])
		assert.Equal(t, int64(9), b.Ints[iSeverity][0])
		assert.Equal(t, []byte("INFO"), b.Bytes[bSeverityText][0])
		assert.Equal(t, []byte("hello"), b.Bytes[bBody][0])
		assert.Equal(t, []byte("tid"), b.Bytes[bTraceID][0])

		decoded, _, err := signal.DecodeAttributes(b.Bytes[bAttrs][0])
		require.NoError(t, err)
		v, ok := decoded.Get([]byte("k"))
		require.True(t, ok)
		assert.Equal(t, []byte("v"), v.Str())
	})
}

func TestGetPutLogsRecycles(t *testing.T) {
	t.Parallel()

	l := GetLogs()
	l.AddResource().AddScope().AddRecord().Body = []byte("x")
	PutLogs(l)

	l2 := GetLogs()
	assert.Empty(t, l2.Resources, "a pooled batch comes back reset")
	PutLogs(l2)
}

func BenchmarkProject(b *testing.B) {
	ld := mkLogs()

	b.ReportAllocs()

	for b.Loop() {
		Project(ld, func(*recordengine.Batch) {})
	}
}

func TestProjectSkipsEmptyScopes(t *testing.T) {
	t.Parallel()

	var ld Logs
	rl := ld.AddResource()
	rl.AddScope() // no records
	sl := rl.AddScope()
	sl.AddRecord().Body = []byte("x")

	n := 0
	accepted := Project(ld, func(*recordengine.Batch) { n++ })
	assert.Equal(t, 1, n, "empty scope groups emit nothing")
	assert.Equal(t, 1, accepted)
}
