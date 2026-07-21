package ec_test

import (
	"context"
	"math/rand/v2"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/backend"
	"github.com/oteldb/storage/cluster/ec"
)

// writeFullPart writes a full-copy part (objects at their plain keys) into be.
func writeFullPart(t *testing.T, be backend.Backend, objects map[string][]byte) {
	t.Helper()
	ctx := context.Background()

	for name, data := range objects {
		require.NoError(t, be.Write(ctx, partPrefix+"/"+name, data))
	}
}

func convObjects(rng *rand.Rand) map[string][]byte {
	big := make([]byte, 96<<10)
	for i := range big {
		big[i] = byte(rng.Uint32())
	}

	mid := make([]byte, 16<<10)
	for i := range mid {
		mid[i] = byte(rng.Uint32() * 7)
	}

	return map[string][]byte{
		"c/0":      big,
		"c/1":      mid,
		"manifest": append([]byte("manifest:"), mid[:5<<10]...),
		"marks":    []byte("tiny-marks"), // sub-floor: stays full-copy
	}
}

func TestConvertThenRead(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	s := ec.Scheme{Data: 4, Parity: 2}
	objects := convObjects(rand.New(rand.NewPCG(11, 12)))
	be := backend.Memory()
	writeFullPart(t, be, objects)

	meta, err := ec.Convert(ctx, be, partPrefix, s)
	require.NoError(t, err)

	// The sidecar lists exactly the sharded (>= floor) objects; sub-floor is absent.
	names := map[string]bool{}
	for _, o := range meta.Objects {
		names[o.Name] = true
	}
	assert.Equal(t, map[string]bool{"c/0": true, "c/1": true, "manifest": true}, names)

	// Full copies of sharded objects are gone; the sub-floor object and shards remain.
	for _, name := range []string{"c/0", "c/1", "manifest"} {
		_, err := be.Read(ctx, partPrefix+"/"+name)
		require.ErrorIsf(t, err, backend.ErrNotExist, "full copy of %q deleted", name)
	}
	_, err = be.Read(ctx, partPrefix+"/marks")
	require.NoError(t, err, "sub-floor full copy kept")

	// Every object reads back identically through the reconstructing reader (all shards local).
	r := &ec.Reader{Local: be, Slot: 0}
	for name, want := range objects {
		got, err := r.Read(ctx, partPrefix+"/"+name)
		require.NoErrorf(t, err, "read %q after convert", name)
		assert.Equalf(t, want, got, "%q identity", name)
	}
}

func TestConvertIdempotent(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	s := ec.Scheme{Data: 2, Parity: 1}
	objects := convObjects(rand.New(rand.NewPCG(13, 14)))
	be := backend.Memory()
	writeFullPart(t, be, objects)

	m1, err := ec.Convert(ctx, be, partPrefix, s)
	require.NoError(t, err)

	// A second Convert (full copies already gone) must not fail trying to re-read them.
	m2, err := ec.Convert(ctx, be, partPrefix, s)
	require.NoError(t, err)
	assert.Equal(t, m1, m2, "re-convert returns the recorded sidecar unchanged")

	r := &ec.Reader{Local: be, Slot: 0}
	got, err := r.Read(ctx, partPrefix+"/c/0")
	require.NoError(t, err)
	assert.Equal(t, objects["c/0"], got)
}

func TestConvertSweepsLeftoverFullCopies(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	s := ec.Scheme{Data: 2, Parity: 1}
	objects := convObjects(rand.New(rand.NewPCG(15, 16)))
	be := backend.Memory()
	writeFullPart(t, be, objects)

	meta, err := ec.Convert(ctx, be, partPrefix, s)
	require.NoError(t, err)

	// Simulate a crash mid-delete: a full copy reappears after the sidecar was written.
	require.NoError(t, be.Write(ctx, partPrefix+"/c/0", objects["c/0"]))

	_, err = ec.Convert(ctx, be, partPrefix, s)
	require.NoError(t, err)
	_, err = be.Read(ctx, partPrefix+"/c/0")
	require.ErrorIs(t, err, backend.ErrNotExist, "the leftover full copy was swept")

	assert.NotNil(t, meta)
}

func TestConvertedDetection(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	be := backend.Memory()
	writeFullPart(t, be, convObjects(rand.New(rand.NewPCG(17, 18))))

	_, ok, err := ec.Converted(ctx, be, partPrefix)
	require.NoError(t, err)
	assert.False(t, ok, "a full-copy part is not converted")

	_, err = ec.Convert(ctx, be, partPrefix, ec.Scheme{Data: 2, Parity: 1})
	require.NoError(t, err)

	meta, ok, err := ec.Converted(ctx, be, partPrefix)
	require.NoError(t, err)
	require.True(t, ok)
	assert.Equal(t, ec.Scheme{Data: 2, Parity: 1}, meta.Scheme)
}

func TestConvertCrashBeforeSidecarLeavesFullCopy(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	// Emulate the pre-commit state: shards written, no sidecar yet. A reader must still serve
	// the surviving full copies, and a fresh Convert must complete cleanly.
	s := ec.Scheme{Data: 2, Parity: 1}
	objects := convObjects(rand.New(rand.NewPCG(19, 20)))
	be := backend.Memory()
	writeFullPart(t, be, objects)

	shards, _, err := ec.EncodeObject(s, "c/0", objects["c/0"])
	require.NoError(t, err)
	for i, sh := range shards {
		require.NoError(t, be.Write(ctx, ec.ShardKey(partPrefix, i, "c/0"), sh))
	}

	// No sidecar: Reader serves the full copy.
	r := &ec.Reader{Local: be, Slot: 0}
	got, err := r.Read(ctx, partPrefix+"/c/0")
	require.NoError(t, err)
	assert.Equal(t, objects["c/0"], got)

	// Recovery re-runs Convert to completion.
	_, err = ec.Convert(ctx, be, partPrefix, s)
	require.NoError(t, err)
	got, err = r.Read(ctx, partPrefix+"/c/0")
	require.NoError(t, err)
	assert.Equal(t, objects["c/0"], got)
}
