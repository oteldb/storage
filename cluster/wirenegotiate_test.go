package cluster_test

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/cluster"
	"github.com/oteldb/storage/query/fetch"
	"github.com/oteldb/storage/signal"
)

// recordingTransport observes what the fan-out actually negotiated, which is otherwise invisible
// from the fetcher's return value.
type recordingTransport struct {
	accept   string
	encoding string
}

func (t *recordingTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	t.accept = req.Header.Get("Accept-Encoding")

	resp, err := http.DefaultTransport.RoundTrip(req)
	if err == nil {
		t.encoding = resp.Header.Get("Content-Encoding")
	}

	return resp, err
}

// stripAcceptEncoding makes a handler behave like a node that predates content negotiation.
func stripAcceptEncoding(h http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		r.Header.Del("Accept-Encoding")
		h.ServeHTTP(w, r)
	})
}

func TestRemoteFetcherNegotiatesZSTD(t *testing.T) {
	t.Parallel()

	want := benchLogBatches(4, 200)

	logFn := func(context.Context, string, int64, int64, []fetch.Matcher) ([]*fetch.Batch, error) {
		return benchLogBatches(4, 200), nil
	}
	noFn := func(context.Context, string, int64, int64, []fetch.Matcher) ([]*fetch.Batch, error) { return nil, nil }
	handler := cluster.ReadHandler(noFn, logFn, noFn, noFn)

	for _, tc := range []struct {
		name         string
		handler      http.Handler
		wantEncoding string
	}{
		{"new peer", handler, "zstd"},
		{"old peer", stripAcceptEncoding(handler), ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			mux := http.NewServeMux()
			mux.Handle(cluster.ReadPath, tc.handler)
			srv := httptest.NewServer(mux)
			t.Cleanup(srv.Close)

			rt := &recordingTransport{}
			rf := cluster.NewRemoteFetcher(signal.Log, strings.TrimPrefix(srv.URL, "http://"),
				&http.Client{Transport: rt})

			it, err := rf.Fetch(context.Background(), fetch.Request{Tenant: "acme", Start: 0, End: 1 << 62})
			require.NoError(t, err)

			got, err := fetch.Drain(context.Background(), it)
			require.NoError(t, err)

			assert.Equal(t, "zstd", rt.accept, "a new node always advertises zstd")
			assert.Equal(t, tc.wantEncoding, rt.encoding)

			require.Len(t, got, len(want))

			for i := range want {
				assert.True(t, want[i].Series.Equal(got[i].Series))
				assert.Equal(t, want[i].Timestamps, got[i].Timestamps)
				require.Len(t, got[i].Columns, len(want[i].Columns))
				assert.Equal(t, want[i].Columns[0].Bytes, got[i].Columns[0].Bytes)
			}
		})
	}
}
