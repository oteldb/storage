package cluster_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/cluster"
)

func TestEncodeDecodeWriteRoundTrip(t *testing.T) {
	t.Parallel()

	payload := cluster.EncodeWrite("tenant-7", []byte{1, 2, 3, 4})
	tenant, walBytes, err := cluster.DecodeWrite(payload)
	require.NoError(t, err)
	assert.Equal(t, "tenant-7", tenant)
	assert.Equal(t, []byte{1, 2, 3, 4}, walBytes)

	_, _, err = cluster.DecodeWrite([]byte{0xff}) // truncated length
	require.Error(t, err)
}
