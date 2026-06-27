package recordengine_test

import (
	"bytes"
	"context"
	"fmt"
	"math/rand/v2"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/backend"
	"github.com/oteldb/storage/encoding/chunk"
	"github.com/oteldb/storage/query/fetch"
	"github.com/oteldb/storage/recordengine"
	"github.com/oteldb/storage/signal"
)

// testSchema exercises every column kind and bloom mode: an int column, a full-text byte column,
// an equality byte column (the trace-by-id shape), and a serialized-attributes column.
var testSchema = recordengine.NewSchema(
	recordengine.Column{Name: "sev", Kind: recordengine.KindInt64, Codec: chunk.CodecT64},
	recordengine.Column{Name: "body", Kind: recordengine.KindBytes, Codec: chunk.CodecDict, Bloom: recordengine.BloomFullText},
	recordengine.Column{Name: "id", Kind: recordengine.KindBytes, Codec: chunk.CodecDict, Bloom: recordengine.BloomEquality},
	recordengine.Column{Name: "attrs", Kind: recordengine.KindBytes, Codec: chunk.CodecDict, Bloom: recordengine.BloomAttrs},
)

const (
	iSev               = 0
	bBody, bID, bAttrs = 0, 1, 2
)

type rrec struct {
	ts   int64
	sev  int64
	body string
	id   string
	attr [2]string // key,value (empty key ⇒ no attribute)
}

func mkBatch(svc string, recs ...rrec) *recordengine.Batch {
	res := signal.Resource{Attributes: signal.NewAttributes(
		signal.KeyValue{Key: []byte("service.name"), Value: signal.StringValue([]byte(svc))},
	)}
	series := signal.Series{Resource: res}

	b := &recordengine.Batch{
		Stream:   series.Hash(),
		Identity: func() signal.Series { return series },
		Ints:     make([][]int64, 1),
		Bytes:    make([][][]byte, 3),
	}

	for _, r := range recs {
		var attrs []byte
		if r.attr[0] != "" {
			attrs = signal.NewAttributes(signal.KeyValue{Key: []byte(r.attr[0]), Value: signal.StringValue([]byte(r.attr[1]))}).AppendHashInput(nil)
		}

		b.Ts = append(b.Ts, r.ts)
		b.Ints[iSev] = append(b.Ints[iSev], r.sev)
		b.Bytes[bBody] = append(b.Bytes[bBody], []byte(r.body))
		b.Bytes[bID] = append(b.Bytes[bID], []byte(r.id))
		b.Bytes[bAttrs] = append(b.Bytes[bAttrs], attrs)
	}

	return b
}

func newEngine(t *testing.T, be backend.Backend) *recordengine.Engine {
	t.Helper()

	return recordengine.New(recordengine.Config{Schema: testSchema, Backend: be, Prefix: "t/recs"})
}

func ingest(t *testing.T, e *recordengine.Engine, b *recordengine.Batch) {
	t.Helper()
	_, err := e.AppendBatch(b, recordengine.AppendLimits{})
	require.NoError(t, err)
}

func svcMatcher(svc string) fetch.Matcher {
	want := []byte(svc)

	return fetch.Matcher{Name: []byte("service.name"), Match: func(v signal.Value) bool { return bytes.Equal(v.Str(), want) }}
}

func fetchAll(t *testing.T, e *recordengine.Engine, r fetch.Request) []*fetch.Batch {
	t.Helper()
	it, err := e.Fetch(context.Background(), r)
	require.NoError(t, err)
	got, err := fetch.Drain(context.Background(), it)
	require.NoError(t, err)

	return got
}

func bodies(b *fetch.Batch) []string {
	col, _ := b.Column("body")
	out := make([]string, len(col.Bytes))
	for i, v := range col.Bytes {
		out[i] = string(v)
	}

	return out
}

func req(svc string, conds ...fetch.Condition) fetch.Request {
	return fetch.Request{
		Signal: signal.Log, Start: 0, End: 1 << 60,
		Matchers: []fetch.Matcher{svcMatcher(svc)}, Conditions: conds, AllConditions: len(conds) > 0,
	}
}

func TestAppendAndFetch(t *testing.T) {
	t.Parallel()

	e := newEngine(t, nil)
	ingest(t, e, mkBatch("api", rrec{ts: 100, sev: 9, body: "first"}, rrec{ts: 200, sev: 17, body: "second"}))
	ingest(t, e, mkBatch("web", rrec{ts: 150, body: "web"}))

	got := fetchAll(t, e, req("api"))
	require.Len(t, got, 1)
	assert.Equal(t, []int64{100, 200}, got[0].Timestamps)
	assert.Equal(t, []string{"first", "second"}, bodies(got[0]))

	sev, ok := got[0].Column("sev")
	require.True(t, ok)
	assert.Equal(t, []int64{9, 17}, sev.Int64)

	assert.Len(t, fetchAll(t, e, fetch.Request{Start: 0, End: 1 << 60}), 2, "no matchers ⇒ all streams")
	assert.Equal(t, 2, e.StreamCount())
}

func TestFetchWindowSorts(t *testing.T) {
	t.Parallel()

	e := newEngine(t, nil)
	ingest(t, e, mkBatch("api", rrec{ts: 300, body: "c"}, rrec{ts: 100, body: "a"}, rrec{ts: 200, body: "b"}))

	got := fetchAll(t, e, fetch.Request{Start: 150, End: 300, Matchers: []fetch.Matcher{svcMatcher("api")}})
	require.Len(t, got, 1)
	assert.Equal(t, []int64{200, 300}, got[0].Timestamps)
	assert.Equal(t, []string{"b", "c"}, bodies(got[0]))
}

func TestFlushMergeRetention(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	e := newEngine(t, backend.Memory())

	ingest(t, e, mkBatch("api", rrec{ts: 100, body: "p1"}))
	require.NoError(t, e.Flush(ctx))
	ingest(t, e, mkBatch("api", rrec{ts: 200, body: "p2"}))
	require.NoError(t, e.Flush(ctx))
	ingest(t, e, mkBatch("api", rrec{ts: 300, body: "head"}))
	require.Equal(t, 2, e.PartCount())

	got := fetchAll(t, e, req("api"))
	require.Len(t, got, 1)
	assert.Equal(t, []string{"p1", "p2", "head"}, bodies(got[0]), "head ∪ parts, time-ordered")

	require.NoError(t, e.Merge(ctx, 0))
	assert.Equal(t, 1, e.PartCount(), "two parts compacted (head was unflushed)")

	require.NoError(t, e.Flush(ctx)) // flush the head record too
	require.NoError(t, e.Merge(ctx, 250))
	got = fetchAll(t, e, req("api"))
	require.Len(t, got, 1)
	assert.Equal(t, []string{"head"}, bodies(got[0]), "retention dropped ts<250")
}

func TestOutOfOrderRejected(t *testing.T) {
	t.Parallel()

	e := recordengine.New(recordengine.Config{Schema: testSchema, OOOWindow: 50})
	res, err := e.AppendBatch(mkBatch("api", rrec{ts: 100, body: "a"}, rrec{ts: 80, body: "b"}, rrec{ts: 40, body: "c"}), recordengine.AppendLimits{})
	require.NoError(t, err)
	assert.Equal(t, 2, res.Accepted, "40 < newest(100)-50 ⇒ rejected")

	got := fetchAll(t, e, req("api"))
	require.Len(t, got, 1)
	assert.Equal(t, []int64{80, 100}, got[0].Timestamps)
}

func TestStatelessLoadParts(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	be := backend.Memory()

	w := newEngine(t, be)
	ingest(t, w, mkBatch("api", rrec{ts: 100, body: "x"}, rrec{ts: 200, body: "y"}))
	require.NoError(t, w.Flush(ctx))

	r := newEngine(t, be)
	require.NoError(t, r.LoadParts(ctx))
	assert.Equal(t, 1, r.PartCount())

	got := fetchAll(t, r, req("api"))
	require.Len(t, got, 1, "matchers resolve via the persisted stream index")
	assert.Equal(t, []string{"x", "y"}, bodies(got[0]))
}

func TestRefreshReplicaTrims(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	be := backend.Memory()

	primary, replica := newEngine(t, be), newEngine(t, be)
	ingest(t, primary, mkBatch("api", rrec{ts: 100, body: "x"}))
	ingest(t, replica, mkBatch("api", rrec{ts: 100, body: "x"}))
	require.Equal(t, 1, replica.HeadRecordCount())

	require.NoError(t, primary.Flush(ctx))
	require.NoError(t, replica.RefreshReplica(ctx))
	assert.Equal(t, 1, replica.PartCount())
	assert.Equal(t, 0, replica.HeadRecordCount(), "head trimmed to the unflushed window")

	got := fetchAll(t, replica, req("api"))
	require.Len(t, got, 1)
	assert.Equal(t, []int64{100}, got[0].Timestamps)
}

func TestResetClears(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	e := newEngine(t, backend.Memory())
	ingest(t, e, mkBatch("api", rrec{ts: 100, body: "x"}))
	require.NoError(t, e.Flush(ctx))

	require.NoError(t, e.Reset(ctx))
	assert.Equal(t, 0, e.PartCount())
	assert.Equal(t, 0, e.StreamCount())
	assert.Empty(t, fetchAll(t, e, fetch.Request{Start: 0, End: 1 << 60}))
}

// TestFetchSupersetOfBruteForce is the contract property over random data + queries.
func TestFetchSupersetOfBruteForce(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	rng := rand.New(rand.NewPCG(1, 2))
	e := newEngine(t, backend.Memory())

	services := []string{"api", "web", "db"}

	type record struct {
		svc  string
		ts   int64
		body string
	}

	all := make([]record, 0, 600)

	for round := range 5 {
		batches := map[string]*recordengine.Batch{}
		for range 40 {
			svc := services[rng.IntN(len(services))]
			ts := int64(rng.IntN(1000))
			body := fmt.Sprintf("m%d", rng.IntN(50))
			all = append(all, record{svc, ts, body})

			if batches[svc] == nil {
				batches[svc] = mkBatch(svc)
			}

			b := batches[svc]
			b.Ts = append(b.Ts, ts)
			b.Ints[iSev] = append(b.Ints[iSev], 0)
			b.Bytes[bBody] = append(b.Bytes[bBody], []byte(body))
			b.Bytes[bID] = append(b.Bytes[bID], nil)
			b.Bytes[bAttrs] = append(b.Bytes[bAttrs], nil)
		}

		for _, b := range batches {
			ingest(t, e, b)
		}

		if round%2 == 1 {
			require.NoError(t, e.Flush(ctx))
		}
	}

	for range 20 {
		svc := services[rng.IntN(len(services))]
		lo := int64(rng.IntN(1000))
		hi := lo + int64(rng.IntN(400))

		got := map[string]int{}
		for _, b := range fetchAll(t, e, fetch.Request{Start: lo, End: hi, Matchers: []fetch.Matcher{svcMatcher(svc)}}) {
			for _, body := range bodies(b) {
				got[body]++
			}
		}

		want := map[string]int{}
		for _, rec := range all {
			if rec.svc == svc && rec.ts >= lo && rec.ts <= hi {
				want[rec.body]++
			}
		}

		require.Equalf(t, want, got, "svc %q window [%d,%d]", svc, lo, hi)
	}
}

// snapshot maps each returned stream to a stable, comparable string of its rows (ts:sev:body), so a
// recycled fetch can be checked row-for-row against a plain one.
func snapshot(batches []*fetch.Batch) map[signal.SeriesID][]string {
	out := make(map[signal.SeriesID][]string, len(batches))
	for _, b := range batches {
		sev, _ := b.Column("sev")
		rows := make([]string, len(b.Timestamps))

		for i := range b.Timestamps {
			rows[i] = fmt.Sprintf("%d:%d:%s", b.Timestamps[i], sev.Int64[i], bodies(b)[i])
		}

		out[b.ID] = rows
	}

	return out
}

// TestFetchRecycleMatchesPlain verifies the opt-in Recycle path: recycled fetches return byte-for-
// byte the same rows as a plain fetch, and reusing the pooled accumulators/int buffers across many
// release→fetch rounds never corrupts a later result. Projection alternates per round so the
// accumulator re-arm (prepare) is exercised across differing column selections.
func TestFetchRecycleMatchesPlain(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	e := newEngine(t, backend.Memory())

	// Several flushed parts plus a live head, across multiple services (⇒ multiple streams pooled).
	for round := range 4 {
		for _, svc := range []string{"api", "web", "db"} {
			recs := make([]rrec, 0, 8)
			for i := range 8 {
				recs = append(recs, rrec{
					ts:   int64(round*100 + i),
					sev:  int64(round*10 + i),
					body: fmt.Sprintf("%s-r%d-%d", svc, round, i),
				})
			}

			ingest(t, e, mkBatch(svc, recs...))
		}

		if round < 3 { // leave the last round in the head
			require.NoError(t, e.Flush(ctx))
		}
	}

	want := snapshot(fetchAll(t, e, fetch.Request{Start: 0, End: 1 << 60}))
	require.NotEmpty(t, want)

	projections := [][]string{nil, {"ts", "sev", "body"}, {"body"}, {"ts", "sev", "body"}}

	for round := range 6 {
		r := fetch.Request{Start: 0, End: 1 << 60, Recycle: true, Projection: projections[round%len(projections)]}

		it, err := e.Fetch(ctx, r)
		require.NoError(t, err)

		var batches []*fetch.Batch

		for {
			b, err := it.Next(ctx)
			if err != nil {
				break
			}

			batches = append(batches, b)
		}

		require.NoError(t, it.Close())

		// Compare to the plain snapshot before releasing (after Release the batch must not be read).
		// When body is the only projection, compare just that column.
		got := snapshotProjected(batches, r.Projection)
		require.Equalf(t, projectWant(want, r.Projection), got, "recycle round %d projection %v", round, r.Projection)

		for _, b := range batches {
			b.Release()
		}
	}
}

// snapshotProjected is [snapshot] honoring a body-only projection (no sev/ts columns present).
func snapshotProjected(batches []*fetch.Batch, projection []string) map[signal.SeriesID][]string {
	if len(projection) == 1 && projection[0] == "body" {
		out := make(map[signal.SeriesID][]string, len(batches))
		for _, b := range batches {
			out[b.ID] = bodies(b)
		}

		return out
	}

	return snapshot(batches)
}

// projectWant reduces a full snapshot to the body-only column when the request projects only body.
func projectWant(full map[signal.SeriesID][]string, projection []string) map[signal.SeriesID][]string {
	if len(projection) != 1 || projection[0] != "body" {
		return full
	}

	out := make(map[signal.SeriesID][]string, len(full))
	for id, rows := range full {
		bodyOnly := make([]string, len(rows))
		for i, row := range rows {
			parts := bytes.SplitN([]byte(row), []byte(":"), 3)
			bodyOnly[i] = string(parts[2])
		}

		out[id] = bodyOnly
	}

	return out
}
