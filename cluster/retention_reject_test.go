package cluster

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/signal"
)

func TestRetentionRejectWireCompatibility(t *testing.T) {
	t.Parallel()
	addr := servePrimaryWrite(t, func(context.Context, signal.Signal, string, []byte) (Reject, error) {
		return Reject{OOO: 2, Cardinality: 3, InFlight: 4, Retention: 5}, nil
	})
	payload := EncodeWrite(signal.Log, "default", nil)
	got, err := SendPrimaryWrite(t.Context(), nil, addr, payload)
	require.NoError(t, err)
	require.Equal(t, Reject{OOO: 2, Cardinality: 3, InFlight: 4, Retention: 5}, got)
	req, err := http.NewRequestWithContext(t.Context(), http.MethodPost, "http://"+addr+PrimaryWritePath, strings.NewReader(string(payload)))
	require.NoError(t, err)
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	var ooo, cardinality, inflight int
	_, err = fmt.Sscanf(string(body), "%d %d %d", &ooo, &cardinality, &inflight)
	require.NoError(t, err)
	require.Equal(t, got.Total(), ooo+cardinality+inflight)
}

func TestRetentionRejectHeaderValidation(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name, header string
		valid        bool
	}{
		{"legacy server", "", true}, {"zero", "0", true}, {"count", "2", true},
		{"negative", "-1", false}, {"too large", "4", false}, {"malformed", "x", false}, {"overflow", "999999999999999999999999999999999", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				if tc.header != "" {
					w.Header().Set(retentionRejectHeader, tc.header)
				}
				_, _ = io.WriteString(w, "3 1 2")
			}))
			defer server.Close()
			got, err := SendPrimaryWrite(t.Context(), nil, strings.TrimPrefix(server.URL, "http://"), EncodeWrite(signal.Log, "default", nil))
			if !tc.valid {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			require.Equal(t, 6, got.Total())
			if tc.header == "" {
				require.Zero(t, got.Retention)
				require.Equal(t, 3, got.OOO)
			}
		})
	}
}

func FuzzPrimaryRejectCounts(f *testing.F) {
	for _, header := range []string{"", "0", "2", "-1", "4", "x", "999999999999999999999999999999999"} {
		f.Add([]byte("3 1 2"), header)
	}
	f.Add([]byte("9223372036854775807 1 1"), "")
	f.Fuzz(func(t *testing.T, body []byte, header string) {
		if len(body) > 8192 || len(header) > 8192 {
			t.Skip()
		}
		got, err := decodePrimaryReject(body, header)
		if err != nil {
			return
		}
		require.GreaterOrEqual(t, got.OOO, 0)
		require.GreaterOrEqual(t, got.Retention, 0)
		require.GreaterOrEqual(t, got.Cardinality, 0)
		require.GreaterOrEqual(t, got.InFlight, 0)
		require.GreaterOrEqual(t, got.Total(), 0)
	})
}
