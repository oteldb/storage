package storage

import (
	"context"
	"net/http/httptest"
	"slices"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/cluster"
	"github.com/oteldb/storage/query/fetch"
	"github.com/oteldb/storage/signal"
	"github.com/oteldb/storage/signal/log"
)

func peerOptionsStore(tb testing.TB, rows int) (*Storage, fetch.Fetcher) {
	tb.Helper()
	s, err := InMemory()
	require.NoError(tb, err)
	tb.Cleanup(func() { _ = s.Close(context.Background()) })
	var ld log.Logs
	for _, svc := range []string{"a", "b"} {
		rl := ld.AddResource()
		rl.Resource.Attributes = signal.NewAttributes(signal.KeyValue{Key: []byte("service.name"), Value: signal.StringValue([]byte(svc))})
		sl := rl.AddScope()
		for i := range rows {
			r := sl.AddRecord()
			r.Timestamp, r.SeverityNumber = int64(i+1), 9
			r.Body = []byte("message")
			r.Attributes = signal.NewAttributes(signal.KeyValue{Key: []byte("extra"), Value: signal.StringValue([]byte(strings.Repeat("x", 1024)))})
		}
	}
	_, err = s.WriteLogs(context.Background(), ld)
	require.NoError(tb, err)
	eng, err := s.logEngineFor("default")
	require.NoError(tb, err)
	require.NoError(tb, eng.Flush(context.Background()))
	srv := httptest.NewServer(cluster.NewReadHandler(s.serveFetch))
	tb.Cleanup(srv.Close)
	return s, cluster.NewRemoteFetcher(signal.Log, strings.TrimPrefix(srv.URL, "http://"), srv.Client())
}

func TestPeerFetchOptions(t *testing.T) {
	t.Parallel()
	s, remote := peerOptionsStore(t, 100)
	for _, tt := range []struct {
		name string
		r    fetch.Request
		rows int
		cols int
	}{
		{"projection", fetch.Request{Projection: []string{"body"}}, 200, 1},
		{"newest boundary ties", fetch.Request{Projection: []string{"body"}, Limit: 3, Reverse: true}, 4, 1},
		{"oldest boundary ties", fetch.Request{Projection: []string{"body"}, Limit: 3}, 4, 1},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			r := tt.r
			r.Tenant, r.Signal, r.Start, r.End = "default", signal.Log, 1, 100
			it, err := remote.Fetch(t.Context(), r)
			require.NoError(t, err)
			got, err := fetch.Drain(t.Context(), it)
			require.NoError(t, err)
			it, err = s.LogFetcher("default").Fetch(t.Context(), r)
			require.NoError(t, err)
			want, err := fetch.Drain(t.Context(), it)
			require.NoError(t, err)
			var gotTS, wantTS []int64
			for _, b := range got {
				gotTS = append(gotTS, b.Timestamps...)
				assert.Len(t, b.Columns, tt.cols)
			}
			for _, b := range want {
				wantTS = append(wantTS, b.Timestamps...)
			}
			slices.Sort(gotTS)
			slices.Sort(wantTS)
			assert.Equal(t, wantTS, gotTS)
			assert.Len(t, gotTS, tt.rows)
		})
	}
}

func TestPeerFetchKeepsResidualInputs(t *testing.T) {
	t.Parallel()
	_, remote := peerOptionsStore(t, 10)
	r := fetch.Request{Tenant: "default", Start: 1, End: 10, Projection: []string{"severity"}, Limit: 1, Reverse: true,
		AllConditions: true,
		Conditions:    []fetch.Condition{{Column: "extra", Match: func(v signal.Value) bool { return len(v.Str()) == 1024 }}},
		SecondPass:    func(b *fetch.Batch) bool { _, ok := b.Column("body"); return ok },
	}
	it, err := fetch.Filter(remote).Fetch(t.Context(), r)
	require.NoError(t, err)
	got, err := fetch.Drain(t.Context(), it)
	require.NoError(t, err)
	rows := 0
	for _, b := range got {
		rows += len(b.Timestamps)
		_, ok := b.Column("attrs")
		assert.True(t, ok)
	}
	assert.Equal(t, 20, rows, "residual predicates run before the consumer's final limit")
}

func BenchmarkPeerFetchOptions(b *testing.B) {
	const rows = 4096
	_, remote := peerOptionsStore(b, rows)
	cases := []struct {
		name string
		r    fetch.Request
	}{
		{"projection", fetch.Request{Projection: []string{"body"}}},
		{"top_n", fetch.Request{Limit: 100, Reverse: true}},
	}
	for i := range cases {
		tt := &cases[i]
		b.Run(tt.name, func(b *testing.B) {
			r := tt.r
			r.Tenant, r.Start, r.End = "default", 1, rows
			b.ReportAllocs()
			for b.Loop() {
				it, err := remote.Fetch(b.Context(), r)
				if err != nil {
					b.Fatal(err)
				}
				if _, err := fetch.Drain(b.Context(), it); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
