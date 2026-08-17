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

	"github.com/oteldb/storage/signal"
)

// servePrimaryWrite mounts fn at [PrimaryWritePath] and returns the host:port to send to.
func servePrimaryWrite(t *testing.T, fn PrimaryWriteFunc) string {
	t.Helper()

	mux := http.NewServeMux()
	mux.Handle(PrimaryWritePath, PrimaryWriteHandler(fn))

	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)

	return strings.TrimPrefix(srv.URL, "http://")
}

func TestPrimaryWriteRoundTrip(t *testing.T) {
	t.Parallel()

	var (
		gotSig   signal.Signal
		gotShard string
		gotWAL   []byte
	)

	addr := servePrimaryWrite(t, func(_ context.Context, sig signal.Signal, shardKey string, wal []byte) (Reject, error) {
		gotSig, gotShard, gotWAL = sig, shardKey, wal

		return Reject{OOO: 3, Cardinality: 5, InFlight: 7}, nil
	})

	wal := []byte{0x01, 0x02, 0x03}
	rej, err := SendPrimaryWrite(t.Context(), nil, addr, EncodeWrite(signal.Log, "acme/_s2", wal))
	require.NoError(t, err)

	assert.Equal(t, Reject{OOO: 3, Cardinality: 5, InFlight: 7}, rej)
	assert.Equal(t, 15, rej.Total())

	assert.Equal(t, signal.Log, gotSig)
	assert.Equal(t, "acme/_s2", gotShard)
	assert.Equal(t, wal, gotWAL)
}

func TestPrimaryWriteReportsApplyFailure(t *testing.T) {
	t.Parallel()

	addr := servePrimaryWrite(t, func(context.Context, signal.Signal, string, []byte) (Reject, error) {
		return Reject{}, errors.New("head is sealed")
	})

	_, err := SendPrimaryWrite(t.Context(), nil, addr, EncodeWrite(signal.Metric, "acme", nil))
	require.Error(t, err)
	assert.Contains(t, err.Error(), "head is sealed")
}

func TestPrimaryWriteRejectsMalformedPayload(t *testing.T) {
	t.Parallel()

	called := false
	addr := servePrimaryWrite(t, func(context.Context, signal.Signal, string, []byte) (Reject, error) {
		called = true

		return Reject{}, nil
	})

	// A truncated frame must be refused before it reaches the engine.
	_, err := SendPrimaryWrite(t.Context(), nil, addr, []byte{byte(signal.Metric), 0xff})
	require.Error(t, err)
	assert.False(t, called, "a malformed frame never reaches the apply function")
}
