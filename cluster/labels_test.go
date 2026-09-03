package cluster

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/go-faster/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/query/fetch"
	"github.com/oteldb/storage/signal"
)

func TestLabelsRequestRoundTrip(t *testing.T) {
	t.Parallel()

	eq := []fetch.EqualMatcher{
		{Name: "__name__", Value: "http.requests"},
		{Name: "service.name", Value: "api"},
	}

	for _, tt := range []struct {
		name string
		req  LabelsRequest
	}{
		{
			// An empty name is the discriminator for label *names*.
			name: "names",
			req: LabelsRequest{
				Signal: signal.Metric, Tenant: "acme", Start: -5, End: 1 << 40, Equal: eq,
			},
		},
		{
			// A non-empty name asks for that name's *values*.
			name: "values",
			req: LabelsRequest{
				Signal: signal.Metric, Tenant: "acme", Start: 100, End: 200,
				Name: []byte("service.name"), Equal: eq,
			},
		},
		{
			name: "no matchers",
			req:  LabelsRequest{Signal: signal.Log, Tenant: "", Name: []byte("host")},
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got, err := DecodeLabelsRequest(EncodeLabelsRequest(tt.req))
			require.NoError(t, err)
			assert.Equal(t, tt.req.Signal, got.Signal)
			assert.Equal(t, tt.req.Tenant, got.Tenant)
			assert.Equal(t, tt.req.Start, got.Start)
			assert.Equal(t, tt.req.End, got.End)
			assert.Equal(t, tt.req.Name, got.Name)
			assert.Len(t, got.Equal, len(tt.req.Equal))

			// The rebuilt matchers keep their pushable spec, so a peer resolves them against
			// the identity index rather than dropping them.
			ms := got.Matchers()
			require.Len(t, ms, len(tt.req.Equal))

			for i := range ms {
				require.NotNil(t, ms[i].Spec)
				assert.Equal(t, tt.req.Equal[i], *ms[i].Spec)
			}
		})
	}
}

func TestDecodeLabelsRequestMalformed(t *testing.T) {
	t.Parallel()

	good := EncodeLabelsRequest(LabelsRequest{
		Signal: signal.Metric, Tenant: "acme", Start: 1, End: 2, Name: []byte("host"),
	})

	for _, tt := range []struct {
		name string
		data []byte
	}{
		{"empty", nil},
		{"name length overruns", []byte{0x7f}},
		{"truncated after name", good[:len(good)-1]},
		{"no fetch frame", EncodeStringList(nil)[:1]},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			_, err := DecodeLabelsRequest(tt.data)
			require.Error(t, err)
		})
	}
}

func TestStringListRoundTrip(t *testing.T) {
	t.Parallel()

	for _, values := range [][]string{
		nil,
		{},
		{""},
		{"a"},
		{"", "b", "http.requests", strings.Repeat("x", 300)},
	} {
		got, err := DecodeStringList(EncodeStringList(values))
		require.NoError(t, err)
		assert.Len(t, got, len(values))

		for i := range values {
			assert.Equal(t, values[i], got[i])
		}
	}
}

func TestDecodeStringListMalformed(t *testing.T) {
	t.Parallel()

	good := EncodeStringList([]string{"a", "bb"})

	for _, tt := range []struct {
		name string
		data []byte
	}{
		{"empty", nil},
		{"count overruns", []byte{0xff, 0x01}},
		{"truncated entry", good[:len(good)-1]},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			_, err := DecodeStringList(tt.data)
			require.Error(t, err)
		})
	}
}

// TestLabelsHandlerUnsupported pins how a signal without a label index answers: the sentinel
// survives the round trip, so the caller keeps the path it was on instead of failing the query.
func TestLabelsHandlerUnsupported(t *testing.T) {
	t.Parallel()

	srv := httptest.NewServer(LabelsHandler(func(context.Context, LabelsRequest) ([]string, error) {
		return nil, fetch.ErrLabelsUnsupported
	}))
	t.Cleanup(srv.Close)

	_, err := FetchLabels(t.Context(), srv.Client(), strings.TrimPrefix(srv.URL, "http://"),
		LabelsRequest{Signal: signal.Log, Tenant: "acme"})
	require.Error(t, err)
	assert.True(t, errors.Is(err, fetch.ErrLabelsUnsupported), "got %v", err)
}

// TestLabelsMissingEndpointUnsupported covers a peer too old to know [LabelsPath]: its mux answers
// 404, which must also leave the caller on its current path rather than failing the query.
func TestLabelsMissingEndpointUnsupported(t *testing.T) {
	t.Parallel()

	srv := httptest.NewServer(http.NewServeMux())
	t.Cleanup(srv.Close)

	_, err := FetchLabels(t.Context(), srv.Client(), strings.TrimPrefix(srv.URL, "http://"),
		LabelsRequest{Signal: signal.Metric, Tenant: "acme"})
	require.Error(t, err)
	assert.True(t, errors.Is(err, fetch.ErrLabelsUnsupported), "got %v", err)
}

// TestLabelsHandlerServesRequest walks the whole RPC: the handler decodes what the client encoded
// and the sorted string list comes back intact.
func TestLabelsHandlerServesRequest(t *testing.T) {
	t.Parallel()

	var seen LabelsRequest

	srv := httptest.NewServer(LabelsHandler(func(_ context.Context, r LabelsRequest) ([]string, error) {
		seen = r

		return []string{"api", "web"}, nil
	}))
	t.Cleanup(srv.Close)

	req := LabelsRequest{
		Signal: signal.Metric, Tenant: "acme", Start: 1, End: 2, Name: []byte("service.name"),
		Equal: []fetch.EqualMatcher{{Name: "__name__", Value: "http.requests"}},
	}

	got, err := FetchLabels(t.Context(), srv.Client(), strings.TrimPrefix(srv.URL, "http://"), req)
	require.NoError(t, err)
	assert.Equal(t, []string{"api", "web"}, got)
	assert.Equal(t, req, seen)
}
