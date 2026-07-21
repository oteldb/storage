package ec_test

import (
	"context"
	"math/rand/v2"
	"testing"

	"github.com/go-faster/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/backend"
	"github.com/oteldb/storage/cluster/ec"
)

const partPrefix = "default/metrics/0000000007"

// buildECPart encodes objects into an EC part spread over scheme.Shards() per-node memory
// backends (shard slot i on node i) with the sidecar on every node, returning the nodes and
// the original bytes.
func buildECPart(t *testing.T, s ec.Scheme, objects map[string][]byte) []backend.Backend {
	t.Helper()
	ctx := context.Background()

	nodes := make([]backend.Backend, s.Shards())
	for i := range nodes {
		nodes[i] = backend.Memory()
	}

	meta := &ec.Meta{Scheme: s}

	for name, data := range objects {
		if !ec.ShouldShard(int64(len(data))) {
			// Sub-floor objects stay full-copy everywhere.
			for _, n := range nodes {
				require.NoError(t, n.Write(ctx, partPrefix+"/"+name, data))
			}

			continue
		}

		shards, om, err := ec.EncodeObject(s, name, data)
		require.NoError(t, err)
		meta.Objects = append(meta.Objects, om)

		for i, sh := range shards {
			require.NoError(t, nodes[i].Write(ctx, ec.ShardKey(partPrefix, i, name), sh))
		}
	}

	raw := meta.AppendBinary(nil)
	for _, n := range nodes {
		require.NoError(t, n.Write(ctx, ec.MetaKey(partPrefix), raw))
	}

	return nodes
}

// fetchFrom returns a PeerFetch reading directly from the per-node backends, optionally
// failing a set of slots (down nodes).
func fetchFrom(nodes []backend.Backend, down map[int]bool) ec.PeerFetch {
	return func(ctx context.Context, slot int, key string) ([]byte, error) {
		if down[slot] {
			return nil, errors.New("node down")
		}

		return nodes[slot].Read(ctx, key)
	}
}

func testObjects(rng *rand.Rand) map[string][]byte {
	big := make([]byte, 128<<10)
	for i := range big {
		big[i] = byte(rng.Uint32())
	}

	return map[string][]byte{
		"c/0":      big,
		"marks":    []byte("tiny-marks-object"), // sub-floor: full-copy
		"manifest": append([]byte("manifest:"), big[:8<<10]...),
	}
}

func TestReaderReconstructs(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	s := ec.Scheme{Data: 4, Parity: 2}
	objects := testObjects(rand.New(rand.NewPCG(3, 4)))
	nodes := buildECPart(t, s, objects)

	// Read every object from every node's point of view: local slot + peer gather.
	for slot := range s.Shards() {
		r := &ec.Reader{Local: nodes[slot], Slot: slot, Fetch: fetchFrom(nodes, nil)}

		for name, want := range objects {
			got, err := r.Read(ctx, partPrefix+"/"+name)
			require.NoErrorf(t, err, "node %d reads %q", slot, name)
			assert.Equalf(t, want, got, "node %d: %q identity", slot, name)
		}
	}
}

func TestReaderSurvivesParityLosses(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	s := ec.Scheme{Data: 4, Parity: 2}
	objects := testObjects(rand.New(rand.NewPCG(5, 6)))
	nodes := buildECPart(t, s, objects)

	// Two nodes down (the tolerance of {4,2}); a surviving node still reads everything.
	r := &ec.Reader{Local: nodes[2], Slot: 2, Fetch: fetchFrom(nodes, map[int]bool{0: true, 5: true})}

	for name, want := range objects {
		got, err := r.Read(ctx, partPrefix+"/"+name)
		require.NoErrorf(t, err, "read %q with 2 nodes down", name)
		assert.Equal(t, want, got)
	}

	// Three nodes down exceeds parity: sharded objects fail loudly, full-copy ones survive.
	r = &ec.Reader{Local: nodes[2], Slot: 2, Fetch: fetchFrom(nodes, map[int]bool{0: true, 1: true, 5: true})}
	_, err := r.Read(ctx, partPrefix+"/c/0")
	require.Error(t, err, "beyond-parity loss is unrecoverable")
	got, err := r.Read(ctx, partPrefix+"/marks")
	require.NoError(t, err, "sub-floor objects are full-copy local")
	assert.Equal(t, objects["marks"], got)
}

func TestReaderRejectsCorruptShards(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	s := ec.Scheme{Data: 2, Parity: 1}
	objects := testObjects(rand.New(rand.NewPCG(7, 8)))
	nodes := buildECPart(t, s, objects)

	// Corrupt node 0's shard of c/0 on disk: the checksum gate must skip it and use parity.
	key := ec.ShardKey(partPrefix, 0, "c/0")
	sh, err := nodes[0].Read(ctx, key)
	require.NoError(t, err)
	sh[0] ^= 0xff
	require.NoError(t, nodes[0].Write(ctx, key, sh))

	r := &ec.Reader{Local: nodes[1], Slot: 1, Fetch: fetchFrom(nodes, nil)}
	got, err := r.Read(ctx, partPrefix+"/c/0")
	require.NoError(t, err, "corrupt shard skipped, parity fills in")
	assert.Equal(t, objects["c/0"], got)
}

func TestReaderPassthroughAndNotExist(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	local := backend.Memory()
	require.NoError(t, local.Write(ctx, "default/metrics/bucket-index.bin", []byte("ix")))
	require.NoError(t, local.Write(ctx, "default/metrics/0000000001/manifest", []byte("full-copy")))

	r := &ec.Reader{Local: local, Slot: -1}

	// Full copies (part or non-part keys) pass straight through.
	got, err := r.Read(ctx, "default/metrics/bucket-index.bin")
	require.NoError(t, err)
	assert.Equal(t, []byte("ix"), got)

	got, err = r.Read(ctx, "default/metrics/0000000001/manifest")
	require.NoError(t, err)
	assert.Equal(t, []byte("full-copy"), got)

	// Absent everywhere: ErrNotExist, both for non-part keys and part keys without a sidecar.
	_, err = r.Read(ctx, "default/metrics/streams.bin")
	require.ErrorIs(t, err, backend.ErrNotExist)
	_, err = r.Read(ctx, "default/metrics/0000000002/c/0")
	require.ErrorIs(t, err, backend.ErrNotExist)
}

func TestSplitKey(t *testing.T) {
	t.Parallel()

	for key, want := range map[string][2]string{
		"default/metrics/0000000007/c/0":      {"default/metrics/0000000007", "c/0"},
		"default/metrics/0000000007/manifest": {"default/metrics/0000000007", "manifest"},
		"acme/_s3/logs/0000000001/bloom-x":    {"acme/_s3/logs/0000000001", "bloom-x"},
	} {
		prefix, object, ok := ec.SplitKey(key)
		require.Truef(t, ok, "key %q splits", key)
		assert.Equal(t, want[0], prefix)
		assert.Equal(t, want[1], object)
	}

	for _, key := range []string{
		"default/metrics/bucket-index.bin", // no part segment
		"default/metrics/series.bin",
		"default/metrics/0000000007", // a part prefix with no object
		"123456789/x",                // nine digits, not a seq segment
	} {
		_, _, ok := ec.SplitKey(key)
		assert.Falsef(t, ok, "key %q rejected", key)
	}
}
