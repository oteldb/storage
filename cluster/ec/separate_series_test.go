package ec_test

import (
	"bytes"
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/backend"
	"github.com/oteldb/storage/backend/faultbackend"
	"github.com/oteldb/storage/block"
	"github.com/oteldb/storage/cluster/ec"
	"github.com/oteldb/storage/encoding/chunk"
)

func ecObjectSnapshot(t *testing.T, be backend.Backend) map[string][]byte {
	t.Helper()
	keys, err := be.List(t.Context(), "")
	require.NoError(t, err)
	out := make(map[string][]byte, len(keys))
	for _, key := range keys {
		raw, err := be.Read(t.Context(), key)
		require.NoError(t, err)
		out[key] = bytes.Clone(raw)
	}
	return out
}

func TestConvertRejectsSeparateSeriesBeforeMutation(t *testing.T) {
	t.Parallel()
	for _, kind := range []string{"numeric", "metadata"} {
		t.Run(kind, func(t *testing.T) {
			t.Parallel()
			for _, sidecar := range []bool{false, true} {
				name := "fresh"
				if sidecar {
					name = "existing sidecar"
				}
				t.Run(name, func(t *testing.T) {
					t.Parallel()
					be := backend.Memory()
					scheme := ec.Scheme{Data: 2, Parity: 1}
					if kind == "metadata" {
						w := block.NewPartWriter()
						require.NoError(t, w.AddColumn(block.Column{Name: "series", Kind: block.KindInt128,
							Int128: []chunk.U128{{Hi: 1, Lo: 2}}}))
						require.NoError(t, block.WritePart(t.Context(), be, partPrefix, w))
					} else {
						require.NoError(t, be.Write(t.Context(), partPrefix+"/series-ref", []byte("SREF")))
					}
					body := bytes.Repeat([]byte{1, 2, 3, 4}, ec.FullCopyFloor)
					require.NoError(t, be.Write(t.Context(), partPrefix+"/series-index", body))
					if sidecar {
						_, object, err := ec.EncodeObject(scheme, "series-index", body)
						require.NoError(t, err)
						meta := ec.Meta{Scheme: scheme, Objects: []ec.ObjectMeta{object}}
						require.NoError(t, be.Write(t.Context(), ec.MetaKey(partPrefix), meta.AppendBinary(nil)))
					}
					before := ecObjectSnapshot(t, be)
					guarded := faultbackend.Wrap(be)
					guarded.Add(faultbackend.Rule{Kind: faultbackend.Write, Err: assert.AnError})
					guarded.Add(faultbackend.Rule{Kind: faultbackend.Delete, Err: assert.AnError})
					_, err := ec.Convert(context.Background(), guarded, partPrefix, scheme)
					require.ErrorIs(t, err, ec.ErrSeparateSeries)
					require.Equal(t, before, ecObjectSnapshot(t, be))
				})
			}
		})
	}
}
