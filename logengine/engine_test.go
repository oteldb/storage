package logengine_test

import (
	"bytes"
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/backend"
	"github.com/oteldb/storage/logengine"
	"github.com/oteldb/storage/query/fetch"
	"github.com/oteldb/storage/signal"
	"github.com/oteldb/storage/signal/log"
	"github.com/oteldb/storage/wal"
)

// stream builds a one-resource, one-scope Logs batch for service svc with the given records
// (ts, severity, body), and returns it.
func stream(svc string, recs ...recSpec) log.Logs {
	var ld log.Logs
	rl := ld.AddResource()
	rl.Resource = signal.Resource{Attributes: signal.NewAttributes(
		signal.KeyValue{Key: []byte("service.name"), Value: signal.StringValue([]byte(svc))},
	)}
	sl := rl.AddScope()
	sl.Scope = signal.Scope{Name: []byte("lib")}

	for _, r := range recs {
		rec := sl.AddRecord()
		rec.Timestamp = r.ts
		rec.SeverityNumber = r.sev
		rec.Body = []byte(r.body)
	}

	return ld
}

type recSpec struct {
	ts   int64
	sev  int32
	body string
}

func ingest(t *testing.T, e *logengine.Engine, ld log.Logs) int {
	t.Helper()

	total := 0
	log.Project(ld, func(b *log.Batch) {
		n, err := e.AppendBatch(b)
		require.NoError(t, err)
		total += n
	})

	return total
}

func eqMatcher(name, value string) fetch.Matcher {
	want := []byte(value)

	return fetch.Matcher{Name: []byte(name), Match: func(v signal.Value) bool { return bytes.Equal(v.Str(), want) }}
}

func fetchAll(t *testing.T, e *logengine.Engine, r fetch.Request) []*fetch.Batch {
	t.Helper()
	it, err := e.Fetch(context.Background(), r)
	require.NoError(t, err)
	got, err := fetch.Drain(context.Background(), it)
	require.NoError(t, err)

	return got
}

// bodies extracts the body column of a batch as strings.
func bodies(b *fetch.Batch) []string {
	col, _ := b.Column("body")
	out := make([]string, len(col.Bytes))
	for i, v := range col.Bytes {
		out[i] = string(v)
	}

	return out
}

func TestAppendAndFetch(t *testing.T) {
	t.Parallel()

	e := logengine.New(logengine.Config{})
	ingest(t, e, stream("api", recSpec{100, 9, "first"}, recSpec{200, 17, "second"}))
	ingest(t, e, stream("web", recSpec{150, 9, "web log"}))

	got := fetchAll(t, e, fetch.Request{Start: 0, End: 1000, Matchers: []fetch.Matcher{eqMatcher("service.name", "api")}})
	require.Len(t, got, 1)
	assert.Equal(t, []int64{100, 200}, got[0].Timestamps, "records sorted by ts")
	assert.Equal(t, []string{"first", "second"}, bodies(got[0]))

	sev, ok := got[0].Column("severity")
	require.True(t, ok)
	assert.Equal(t, []int64{9, 17}, sev.Int64)

	// No matchers ⇒ every stream.
	assert.Len(t, fetchAll(t, e, fetch.Request{Start: 0, End: 1000}), 2)
	// Unknown label ⇒ nothing.
	assert.Empty(t, fetchAll(t, e, fetch.Request{Start: 0, End: 1000, Matchers: []fetch.Matcher{eqMatcher("missing", "x")}}))
	assert.Equal(t, 2, e.StreamCount())
}

func TestFetchWindow(t *testing.T) {
	t.Parallel()

	e := logengine.New(logengine.Config{})
	// Out-of-arrival-order timestamps; fetch sorts and windows them.
	ingest(t, e, stream("api", recSpec{300, 1, "c"}, recSpec{100, 1, "a"}, recSpec{200, 1, "b"}))

	got := fetchAll(t, e, fetch.Request{Start: 150, End: 300, Matchers: []fetch.Matcher{eqMatcher("service.name", "api")}})
	require.Len(t, got, 1)
	assert.Equal(t, []int64{200, 300}, got[0].Timestamps, "windowed and sorted")
	assert.Equal(t, []string{"b", "c"}, bodies(got[0]))
}

func TestFlushThenFetchFromPart(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	be := backend.Memory()
	e := logengine.New(logengine.Config{Backend: be, Prefix: "t/logs"})

	ingest(t, e, stream("api", recSpec{100, 9, "first"}, recSpec{200, 17, "second"}))
	require.NoError(t, e.Flush(ctx))
	assert.Equal(t, 1, e.PartCount())
	assert.Equal(t, 0, e.HeadRecordCount(), "flush drained the head")

	got := fetchAll(t, e, fetch.Request{Start: 0, End: 1000, Matchers: []fetch.Matcher{eqMatcher("service.name", "api")}})
	require.Len(t, got, 1)
	assert.Equal(t, []int64{100, 200}, got[0].Timestamps)
	assert.Equal(t, []string{"first", "second"}, bodies(got[0]))
}

func TestFetchMergesHeadAndPart(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	e := logengine.New(logengine.Config{Backend: backend.Memory(), Prefix: "t/logs"})

	ingest(t, e, stream("api", recSpec{100, 1, "p1"}))
	require.NoError(t, e.Flush(ctx))
	ingest(t, e, stream("api", recSpec{200, 1, "h1"})) // stays in head

	got := fetchAll(t, e, fetch.Request{Start: 0, End: 1000, Matchers: []fetch.Matcher{eqMatcher("service.name", "api")}})
	require.Len(t, got, 1)
	assert.Equal(t, []int64{100, 200}, got[0].Timestamps, "head ∪ part, time-ordered")
	assert.Equal(t, []string{"p1", "h1"}, bodies(got[0]))
}

func TestMergeCompactsParts(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	e := logengine.New(logengine.Config{Backend: backend.Memory(), Prefix: "t/logs"})

	ingest(t, e, stream("api", recSpec{100, 1, "a"}))
	require.NoError(t, e.Flush(ctx))
	ingest(t, e, stream("api", recSpec{200, 1, "b"}))
	require.NoError(t, e.Flush(ctx))
	require.Equal(t, 2, e.PartCount())

	require.NoError(t, e.Merge(ctx, 0))
	assert.Equal(t, 1, e.PartCount(), "two parts compacted into one")

	got := fetchAll(t, e, fetch.Request{Start: 0, End: 1000, Matchers: []fetch.Matcher{eqMatcher("service.name", "api")}})
	require.Len(t, got, 1)
	assert.Equal(t, []int64{100, 200}, got[0].Timestamps)
	assert.Equal(t, []string{"a", "b"}, bodies(got[0]))
}

func TestMergeRetentionDrops(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	e := logengine.New(logengine.Config{Backend: backend.Memory(), Prefix: "t/logs"})

	ingest(t, e, stream("api", recSpec{100, 1, "old"}))
	require.NoError(t, e.Flush(ctx))
	ingest(t, e, stream("api", recSpec{500, 1, "new"}))
	require.NoError(t, e.Flush(ctx))

	require.NoError(t, e.Merge(ctx, 300)) // retain ts ≥ 300

	got := fetchAll(t, e, fetch.Request{Start: 0, End: 1000, Matchers: []fetch.Matcher{eqMatcher("service.name", "api")}})
	require.Len(t, got, 1)
	assert.Equal(t, []int64{500}, got[0].Timestamps, "the old record was dropped by retention")
}

func TestOutOfOrderRejected(t *testing.T) {
	t.Parallel()

	e := logengine.New(logengine.Config{OOOWindow: 50})
	n := ingest(t, e, stream("api", recSpec{100, 1, "a"}, recSpec{80, 1, "b"}, recSpec{40, 1, "c"}))
	assert.Equal(t, 2, n, "40 is older than newest(100)-50; rejected")

	got := fetchAll(t, e, fetch.Request{Start: 0, End: 1000, Matchers: []fetch.Matcher{eqMatcher("service.name", "api")}})
	require.Len(t, got, 1)
	assert.Equal(t, []int64{80, 100}, got[0].Timestamps)
}

func TestAllColumnsRoundTripThroughPart(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	e := logengine.New(logengine.Config{Backend: backend.Memory(), Prefix: "t/logs"})

	var ld log.Logs
	rl := ld.AddResource()
	rl.Resource = signal.Resource{Attributes: signal.NewAttributes(
		signal.KeyValue{Key: []byte("service.name"), Value: signal.StringValue([]byte("api"))},
	)}
	sl := rl.AddScope()
	sl.Scope = signal.Scope{Name: []byte("lib"), Version: []byte("v1")}

	r := sl.AddRecord()
	r.Timestamp, r.ObservedTimestamp, r.SeverityNumber = 100, 101, 9
	r.SeverityText, r.Body = []byte("INFO"), []byte("first")
	r.TraceID, r.SpanID = []byte("0123456789abcdef"), []byte("01234567")
	r.Flags, r.Dropped = 1, 2
	r.Attributes = signal.NewAttributes(signal.KeyValue{Key: []byte("k"), Value: signal.StringValue([]byte("v1"))})

	r = sl.AddRecord()
	r.Timestamp, r.ObservedTimestamp, r.SeverityNumber = 200, 202, 17
	r.SeverityText, r.Body = []byte("ERROR"), []byte("second")
	r.TraceID, r.SpanID = []byte("fedcba9876543210"), []byte("76543210")
	r.Flags, r.Dropped = 3, 4
	r.Attributes = signal.NewAttributes(signal.KeyValue{Key: []byte("k"), Value: signal.StringValue([]byte("v2"))})

	ingest(t, e, ld)
	require.NoError(t, e.Flush(ctx)) // force the columnar part path (non-const columns)

	got := fetchAll(t, e, fetch.Request{Start: 0, End: 1000, Matchers: []fetch.Matcher{eqMatcher("service.name", "api")}})
	require.Len(t, got, 1)
	b := got[0]

	assert.Equal(t, []int64{100, 200}, b.Timestamps)
	assertI64Col(t, b, "observed", []int64{101, 202})
	assertI64Col(t, b, "severity", []int64{9, 17})
	assertI64Col(t, b, "flags", []int64{1, 3})
	assertI64Col(t, b, "dropped", []int64{2, 4})
	assertBytesCol(t, b, "severity_text", []string{"INFO", "ERROR"})
	assertBytesCol(t, b, "trace_id", []string{"0123456789abcdef", "fedcba9876543210"})
	assertBytesCol(t, b, "span_id", []string{"01234567", "76543210"})
	assert.Equal(t, []string{"first", "second"}, bodies(b))

	// The attrs column carries the reversible attribute encoding; decode the first row back.
	attrs, ok := b.Column("attrs")
	require.True(t, ok)
	decoded, _, err := signal.DecodeAttributes(attrs.Bytes[0])
	require.NoError(t, err)
	v, ok := decoded.Get([]byte("k"))
	require.True(t, ok)
	assert.Equal(t, []byte("v1"), v.Str())
}

func assertI64Col(t *testing.T, b *fetch.Batch, name string, want []int64) {
	t.Helper()
	col, ok := b.Column(name)
	require.Truef(t, ok, "column %q present", name)
	assert.Equalf(t, want, col.Int64, "column %q", name)
}

func assertBytesCol(t *testing.T, b *fetch.Batch, name string, want []string) {
	t.Helper()
	col, ok := b.Column(name)
	require.Truef(t, ok, "column %q present", name)
	got := make([]string, len(col.Bytes))
	for i, v := range col.Bytes {
		got[i] = string(v)
	}

	assert.Equalf(t, want, got, "column %q", name)
}

func TestWALReplayReconstructs(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	sw, err := wal.Create(dir, 64) // tiny ⇒ rotation
	require.NoError(t, err)

	src := logengine.New(logengine.Config{WAL: sw})
	ingest(t, src, stream("api", recSpec{100, 9, "first"}, recSpec{200, 17, "second"}))
	ingest(t, src, stream("web", recSpec{150, 1, "web"}))
	require.NoError(t, sw.Close())

	// A fresh engine replays the WAL and answers the same query.
	restored := logengine.New(logengine.Config{})
	require.NoError(t, restored.Replay(dir))
	assert.Equal(t, 2, restored.StreamCount())

	got := fetchAll(t, restored, fetch.Request{Start: 0, End: 1000, Matchers: []fetch.Matcher{eqMatcher("service.name", "api")}})
	require.Len(t, got, 1)
	assert.Equal(t, []int64{100, 200}, got[0].Timestamps)
	assert.Equal(t, []string{"first", "second"}, bodies(got[0]))
}

func TestLoadPartsStatelessRead(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	be := backend.Memory()

	writer := logengine.New(logengine.Config{Backend: be, Prefix: "t/logs"})
	ingest(t, writer, stream("api", recSpec{100, 9, "first"}, recSpec{200, 17, "second"}))
	require.NoError(t, writer.Flush(ctx))

	// A fresh engine over the same backend reconstructs parts + the stream index with no
	// in-memory state carried over (the object-store-native read path).
	reader := logengine.New(logengine.Config{Backend: be, Prefix: "t/logs"})
	require.NoError(t, reader.LoadParts(ctx))
	assert.Equal(t, 1, reader.PartCount())

	got := fetchAll(t, reader, fetch.Request{Start: 0, End: 1000, Matchers: []fetch.Matcher{eqMatcher("service.name", "api")}})
	require.Len(t, got, 1, "matchers resolve via the persisted stream index")
	assert.Equal(t, []int64{100, 200}, got[0].Timestamps)
	assert.Equal(t, []string{"first", "second"}, bodies(got[0]))
}

func TestRefreshReplicaTrimsHead(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	be := backend.Memory()

	primary := logengine.New(logengine.Config{Backend: be, Prefix: "t/logs"})
	replica := logengine.New(logengine.Config{Backend: be, Prefix: "t/logs"})

	// Both hold the same record in their heads; the primary flushes it to the shared store.
	ingest(t, primary, stream("api", recSpec{100, 9, "x"}))
	ingest(t, replica, stream("api", recSpec{100, 9, "x"}))
	require.Equal(t, 1, replica.HeadRecordCount())

	require.NoError(t, primary.Flush(ctx))

	// The replica pulls the flushed part and trims its head to the unflushed window.
	require.NoError(t, replica.RefreshReplica(ctx))
	assert.Equal(t, 1, replica.PartCount(), "replica loaded the part")
	assert.Equal(t, 0, replica.HeadRecordCount(), "replica head trimmed — flushed records dropped")

	// It still serves the full series, now from the part.
	got := fetchAll(t, replica, fetch.Request{Start: 0, End: 1000, Matchers: []fetch.Matcher{eqMatcher("service.name", "api")}})
	require.Len(t, got, 1)
	assert.Equal(t, []int64{100}, got[0].Timestamps)
}

func TestCloseFlushes(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	e := logengine.New(logengine.Config{Backend: backend.Memory(), Prefix: "t/logs"})
	ingest(t, e, stream("api", recSpec{100, 1, "x"}))
	require.NoError(t, e.Close(ctx))
	assert.Equal(t, 1, e.PartCount(), "Close flushed the head")
}

func TestFlushAndMergeNoOps(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	e := logengine.New(logengine.Config{Backend: backend.Memory(), Prefix: "t/logs"})

	require.NoError(t, e.Flush(ctx)) // empty head ⇒ no part
	assert.Equal(t, 0, e.PartCount())

	ingest(t, e, stream("api", recSpec{100, 1, "x"}))
	require.NoError(t, e.Flush(ctx))
	require.NoError(t, e.Merge(ctx, 0)) // single part, no retention ⇒ no-op
	assert.Equal(t, 1, e.PartCount())
}

func TestMergeRetentionDropsAll(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	e := logengine.New(logengine.Config{Backend: backend.Memory(), Prefix: "t/logs"})

	ingest(t, e, stream("api", recSpec{100, 1, "a"}))
	require.NoError(t, e.Flush(ctx))
	ingest(t, e, stream("api", recSpec{200, 1, "b"}))
	require.NoError(t, e.Flush(ctx))

	require.NoError(t, e.Merge(ctx, 10_000)) // retain ts ≥ 10000 ⇒ everything dropped
	assert.Equal(t, 0, e.PartCount(), "retention dropped every record, no parts remain")
	assert.Empty(t, fetchAll(t, e, fetch.Request{Start: 0, End: 1 << 60}))
}

func TestFetchTimePrunesParts(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	e := logengine.New(logengine.Config{Backend: backend.Memory(), Prefix: "t/logs"})

	ingest(t, e, stream("api", recSpec{100, 1, "a"}))
	require.NoError(t, e.Flush(ctx))
	ingest(t, e, stream("api", recSpec{900, 1, "b"}))
	require.NoError(t, e.Flush(ctx))

	// A window covering only the second part: the first part is pruned by its time bounds.
	got := fetchAll(t, e, fetch.Request{Start: 500, End: 1000, Matchers: []fetch.Matcher{eqMatcher("service.name", "api")}})
	require.Len(t, got, 1)
	assert.Equal(t, []int64{900}, got[0].Timestamps)
}

func TestResetClears(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	e := logengine.New(logengine.Config{Backend: backend.Memory(), Prefix: "t/logs"})
	ingest(t, e, stream("api", recSpec{100, 1, "x"}))
	require.NoError(t, e.Flush(ctx))

	require.NoError(t, e.Reset(ctx))
	assert.Equal(t, 0, e.PartCount())
	assert.Equal(t, 0, e.StreamCount())
	assert.Empty(t, fetchAll(t, e, fetch.Request{Start: 0, End: 1000}))
}
