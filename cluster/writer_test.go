package cluster_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/cluster"
	"github.com/oteldb/storage/signal"
)

func TestEncodeDecodeWriteRoundTrip(t *testing.T) {
	t.Parallel()

	payload := cluster.EncodeWrite(signal.Log, "tenant-7", []byte{1, 2, 3, 4})
	sig, tenant, walBytes, err := cluster.DecodeWrite(payload)
	require.NoError(t, err)
	assert.Equal(t, signal.Log, sig)
	assert.Equal(t, "tenant-7", tenant)
	assert.Equal(t, []byte{1, 2, 3, 4}, walBytes)

	_, _, _, err = cluster.DecodeWrite(nil) //nolint:dogsled // only the error matters
	require.Error(t, err)
	_, _, _, err = cluster.DecodeWrite([]byte{byte(signal.Metric), 0xff}) //nolint:dogsled // only the error matters
	require.Error(t, err)
}
