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
	"github.com/oteldb/storage/signal"
)

func TestFetchSideCarriesWindow(t *testing.T) {
	t.Parallel()

	var gotTenant string

	var gotStart, gotEnd int64

	mux := http.NewServeMux()
	mux.Handle(cluster.SidePath, cluster.SideHandler(
		func(_ context.Context, tenant string, start, end int64) (map[string][]byte, error) {
			gotTenant, gotStart, gotEnd = tenant, start, end

			return map[string][]byte{"stacks": []byte("s")}, nil
		}))

	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)

	tables, err := cluster.FetchSide(t.Context(), nil, strings.TrimPrefix(srv.URL, "http://"),
		signal.Profile, "acme", -7, 1_700_000_000)
	require.NoError(t, err)
	assert.Equal(t, map[string][]byte{"stacks": []byte("s")}, tables)
	assert.Equal(t, "acme", gotTenant)
	assert.Equal(t, int64(-7), gotStart)
	assert.Equal(t, int64(1_700_000_000), gotEnd)
}
