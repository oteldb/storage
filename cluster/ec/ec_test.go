package ec_test

import (
	"math/rand/v2"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/cluster/ec"
)

func TestEncodeReconstructJoinIdentity(t *testing.T) {
	t.Parallel()

	rng := rand.New(rand.NewPCG(1, 2))

	for _, s := range []ec.Scheme{{Data: 4, Parity: 2}, {Data: 2, Parity: 1}, {Data: 6, Parity: 3}, {Data: 1, Parity: 1}} {
		for _, size := range []int{0, 1, 7, 1024, 65536, 65537} {
			data := make([]byte, size)
			for i := range data {
				data[i] = byte(rng.Uint32())
			}

			shards, err := ec.Encode(s, data)
			require.NoError(t, err)
			require.Len(t, shards, s.Shards())

			// Drop up to Parity random shards; the object must survive.
			lost := rng.IntN(s.Parity + 1)
			for _, i := range rng.Perm(s.Shards())[:lost] {
				shards[i] = nil
			}

			require.NoError(t, ec.Reconstruct(s, shards))

			got, err := ec.Join(s, shards, int64(size))
			require.NoError(t, err)
			assert.Equalf(t, data, got, "scheme %+v size %d lost %d: identity", s, size, lost)
		}
	}
}

func TestReconstructFailsBeyondParity(t *testing.T) {
	t.Parallel()

	s := ec.Scheme{Data: 4, Parity: 2}
	data := make([]byte, 4096)
	for i := range data {
		data[i] = byte(i)
	}

	shards, err := ec.Encode(s, data)
	require.NoError(t, err)

	// Lose Parity+1 shards: reconstruction must fail loudly, never silently corrupt.
	for i := range s.Parity + 1 {
		shards[i] = nil
	}

	require.Error(t, ec.Reconstruct(s, shards), "losing more than Parity shards is unrecoverable")
}

func TestSchemeValidate(t *testing.T) {
	t.Parallel()

	require.NoError(t, ec.Scheme{Data: 4, Parity: 2}.Validate())
	require.Error(t, ec.Scheme{Data: 0, Parity: 2}.Validate())
	require.Error(t, ec.Scheme{Data: 4, Parity: 0}.Validate())
	require.Error(t, ec.Scheme{Data: 200, Parity: 100}.Validate(), "more than 256 total shards")

	_, err := ec.Encode(ec.Scheme{Data: 0, Parity: 1}, []byte("x"))
	require.Error(t, err)
}

func TestJoinRequiresDataShards(t *testing.T) {
	t.Parallel()

	s := ec.Scheme{Data: 2, Parity: 1}
	shards, err := ec.Encode(s, []byte("hello world"))
	require.NoError(t, err)

	shards[0] = nil
	_, err = ec.Join(s, shards, 11)
	require.Error(t, err, "Join demands a complete data set; Reconstruct first")

	require.NoError(t, ec.Reconstruct(s, shards))
	got, err := ec.Join(s, shards, 11)
	require.NoError(t, err)
	assert.Equal(t, []byte("hello world"), got)
}

func TestMetaRoundTrip(t *testing.T) {
	t.Parallel()

	m := &ec.Meta{
		Scheme: ec.Scheme{Data: 4, Parity: 2},
		Objects: []ec.ObjectMeta{
			{Name: "c/0", Size: 123456, Checksums: []uint64{1, 2, 3, 4, 5, 6}},
			{Name: "manifest", Size: 512, Checksums: []uint64{7, 8, 9, 10, 11, 12}},
			{Name: "marks", Size: 0, Checksums: []uint64{0, 0, 0, 0, 0, 0}},
		},
	}

	enc := m.AppendBinary(nil)
	got, err := ec.DecodeMeta(enc)
	require.NoError(t, err)
	assert.Equal(t, m, got)
}

func TestDecodeMetaRejectsCorruption(t *testing.T) {
	t.Parallel()

	m := &ec.Meta{
		Scheme:  ec.Scheme{Data: 2, Parity: 1},
		Objects: []ec.ObjectMeta{{Name: "c/0", Size: 10, Checksums: []uint64{1, 2, 3}}},
	}
	enc := m.AppendBinary(nil)

	// Any single-byte flip must be rejected (whole-payload checksum).
	for i := range enc {
		bad := append([]byte(nil), enc...)
		bad[i] ^= 0xff
		_, err := ec.DecodeMeta(bad)
		require.Errorf(t, err, "flipped byte %d detected", i)
	}

	// Truncations too.
	for i := range enc {
		_, err := ec.DecodeMeta(enc[:i])
		require.Errorf(t, err, "truncation at %d detected", i)
	}

	_, err := ec.DecodeMeta(nil)
	require.Error(t, err)
}

func TestChecksumShardDetectsShardCorruption(t *testing.T) {
	t.Parallel()

	s := ec.Scheme{Data: 2, Parity: 1}
	shards, err := ec.Encode(s, []byte("some object payload"))
	require.NoError(t, err)

	sum := ec.ChecksumShard(shards[1])
	shards[1][0] ^= 0xff
	assert.NotEqual(t, sum, ec.ChecksumShard(shards[1]), "a corrupt shard fails its checksum")
}

// TestGoldenMetaEncoding pins the on-disk Meta framing: version 1, uvarint scheme {2,1}, one
// object ("c/0", size 10, three checksums LE), xxh3 trailer. A change here is a format break.
func TestGoldenMetaEncoding(t *testing.T) {
	t.Parallel()

	m := &ec.Meta{
		Scheme:  ec.Scheme{Data: 2, Parity: 1},
		Objects: []ec.ObjectMeta{{Name: "c/0", Size: 10, Checksums: []uint64{1, 2, 3}}},
	}

	want := []byte{
		0x1,      // version
		0x2, 0x1, // scheme {2,1}
		0x1,                // one object
		0x3, 'c', '/', '0', // name
		0xa,                      // size 10
		0x3,                      // three checksums
		0x1, 0, 0, 0, 0, 0, 0, 0, // checksum 1 (LE)
		0x2, 0, 0, 0, 0, 0, 0, 0, // checksum 2
		0x3, 0, 0, 0, 0, 0, 0, 0, // checksum 3
		0x39, 0x46, 0xec, 0x43, 0x10, 0xfa, 0x5f, 0x82, // xxh3 trailer
	}
	assert.Equal(t, want, m.AppendBinary(nil))
}
