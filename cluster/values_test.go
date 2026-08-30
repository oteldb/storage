package cluster_test

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/cluster"
	"github.com/oteldb/storage/signal"
)

func TestValuesRequestRoundTrip(t *testing.T) {
	t.Parallel()

	for _, in := range []cluster.ValuesRequest{
		{Signal: signal.Trace, Tenant: "acme", Column: "name", Start: -5, End: 900, Limit: 100},
		{Signal: signal.Log, Tenant: "", AttrKey: []byte("http.method")},
		{Signal: signal.Profile, Tenant: "t", Column: "\x00\xff", Limit: -1},
	} {
		out, err := cluster.DecodeValuesRequest(cluster.EncodeValuesRequest(in))
		require.NoError(t, err)
		assert.Equal(t, in, out)
	}
}

func TestValueListRoundTrip(t *testing.T) {
	t.Parallel()

	in := [][]byte{[]byte("alpha"), {}, []byte("\x00\xff\x01")}

	out, err := cluster.DecodeValueList(cluster.EncodeValueList(in))
	require.NoError(t, err)
	require.Len(t, out, 3)
	assert.Equal(t, []byte("alpha"), out[0])
	assert.Empty(t, out[1])
	assert.Equal(t, []byte("\x00\xff\x01"), out[2])
}

// The handler must hand the decoded request through unchanged and frame the answer back.
func TestValuesHandlerRoundTrip(t *testing.T) {
	t.Parallel()

	var got cluster.ValuesRequest

	srv := httptest.NewServer(cluster.ValuesHandler(func(_ context.Context, r cluster.ValuesRequest) ([][]byte, error) {
		got = r

		return [][]byte{[]byte("GET"), []byte("POST")}, nil
	}))
	t.Cleanup(srv.Close)

	want := cluster.ValuesRequest{
		Signal: signal.Log, Tenant: "acme", AttrKey: []byte("http.method"), Start: 10, End: 20, Limit: 5,
	}

	out, err := cluster.FetchValues(context.Background(), srv.Client(), srv.Listener.Addr().String(), want)
	require.NoError(t, err)
	assert.Equal(t, want, got)
	assert.Equal(t, [][]byte{[]byte("GET"), []byte("POST")}, out)
}

func TestValuesHandlerRejectsGET(t *testing.T) {
	t.Parallel()

	srv := httptest.NewServer(cluster.ValuesHandler(func(context.Context, cluster.ValuesRequest) ([][]byte, error) {
		t.Error("handler must not run for a GET")

		return nil, nil
	}))
	t.Cleanup(srv.Close)

	req, err := http.NewRequestWithContext(t.Context(), http.MethodGet, srv.URL+cluster.ValuesPath, http.NoBody)
	require.NoError(t, err)

	resp, err := srv.Client().Do(req)
	require.NoError(t, err)
	require.NoError(t, resp.Body.Close())
	assert.Equal(t, http.StatusBadRequest, resp.StatusCode)
}

// FuzzDecodeValues: arbitrary bytes to the values request/response decoders must error or decode,
// never panic.
func FuzzDecodeValues(f *testing.F) {
	f.Add(cluster.EncodeValuesRequest(cluster.ValuesRequest{Signal: signal.Log, Tenant: "t", Column: "body"}))
	f.Add(cluster.EncodeValueList([][]byte{[]byte("v")}))
	f.Add([]byte{0x02, 0xff})

	f.Fuzz(func(_ *testing.T, data []byte) {
		_, _ = cluster.DecodeValuesRequest(data)
		_, _ = cluster.DecodeValueList(data)
	})
}
