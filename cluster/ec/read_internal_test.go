package ec

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestReconstructDataSkipsParity pins the read path's cost: a complete data set is not decoded
// at all, and a missing data shard is rebuilt without rebuilding the parity the read never uses.
func TestReconstructDataSkipsParity(t *testing.T) {
	t.Parallel()

	s := Scheme{Data: 3, Parity: 2}
	data := bytes.Repeat([]byte("0123456789"), 10)

	full, err := Encode(s, data)
	require.NoError(t, err)

	shards := [][]byte{full[0], full[1], full[2], nil, nil}
	require.NoError(t, reconstructData(s, shards))
	assert.Nil(t, shards[3], "complete data needs no parity")
	assert.Nil(t, shards[4])

	shards = [][]byte{full[0], nil, full[2], full[3], nil}
	require.NoError(t, reconstructData(s, shards))
	assert.Equal(t, full[1], shards[1])
	assert.Nil(t, shards[4], "unused parity stays missing")

	got, err := Join(s, shards, int64(len(data)))
	require.NoError(t, err)
	assert.Equal(t, data, got)

	require.NoError(t, reconstructData(s, make([][]byte, s.Shards())), "the zero-byte object")
	require.Error(t, reconstructData(s, [][]byte{full[0], nil, nil, nil, nil}), "below Data shards")
}
