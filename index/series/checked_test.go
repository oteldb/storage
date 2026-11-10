package series

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/signal"
)

func TestIndexAddChecked(t *testing.T) {
	t.Parallel()
	for _, original := range []signal.Series{{}, encodingSeries(), mk("api", "mode", "idle")} {
		ix := New()
		id := original.Hash()
		wrong := id
		wrong.Lo ^= 1
		n, ok := ix.AddChecked(wrong, original)
		require.False(t, ok)
		require.Equal(t, len(original.AppendHashInput(nil)), n)
		require.Zero(t, ix.Len())
		require.Empty(t, ix.byID)
		require.Empty(t, ix.rows)
		require.Empty(t, ix.resources)
		require.Empty(t, ix.scopes)
		require.Empty(t, ix.contexts)
		require.Empty(t, ix.keysets)
		require.Empty(t, ix.values)
		require.Empty(t, ix.sym.Snapshot())
		n, ok = ix.AddChecked(id, original)
		require.True(t, ok)
		require.Equal(t, len(original.AppendHashInput(nil)), n)
		require.Equal(t, 1, ix.Len())
		require.Equal(t, id, ix.ID(0))
		require.Equal(t, original.AppendHashInput(nil), ix.View().AppendCanonical(nil, 0))
		other := mk("different", "metric", "different")
		_, ok = ix.AddChecked(id, other)
		require.False(t, ok, "an existing expected ID does not bypass hash validation")
		require.Equal(t, 1, ix.Len())
		require.Equal(t, original.AppendHashInput(nil), ix.View().AppendCanonical(nil, 0))
		before := ix.SizeBytes()
		_, ok = ix.AddChecked(id, original)
		require.True(t, ok)
		require.Equal(t, 1, ix.Len())
		require.Equal(t, before, ix.SizeBytes())
		_, ok = ix.AddChecked(wrong, original)
		require.False(t, ok)
		require.Equal(t, 1, ix.Len())
		require.Equal(t, original.AppendHashInput(nil), ix.View().AppendCanonical(nil, 0))
	}
}

func TestIndexAddCheckedOwnsValues(t *testing.T) {
	t.Parallel()
	s := encodingSeries()
	want := s.AppendHashInput(nil)
	ix := New()
	_, ok := ix.AddChecked(s.Hash(), s)
	require.True(t, ok)
	clear(s.Resource.SchemaURL)
	clear(s.Scope.Name)
	clear(s.Scope.Version)
	clear(s.Scope.SchemaURL)
	for _, kv := range s.Attributes {
		clear(kv.Key)
		if kv.Value.Kind() == signal.KindStr {
			clear(kv.Value.Str())
		}
	}
	require.Equal(t, want, ix.View().AppendCanonical(nil, 0))
}

func FuzzIndexAddChecked(f *testing.F) {
	f.Add(signal.Series{}.AppendHashInput(nil))
	f.Add(encodingSeries().AppendHashInput(nil))
	f.Fuzz(func(t *testing.T, raw []byte) {
		if len(raw) > 1<<16 {
			return
		}
		s, n, err := signal.DecodeSeries(raw)
		if err != nil || n != len(raw) {
			return
		}
		expected, checked := New(), New()
		id := expected.Add(s)
		size, ok := checked.AddChecked(id, s)
		if !ok || size != len(s.AppendHashInput(nil)) || !bytes.Equal(expected.View().AppendCanonical(nil, 0), checked.View().AppendCanonical(nil, 0)) {
			t.Fatal("checked add changed canonical identity")
		}
		wrong := id
		wrong.Hi ^= 1
		if _, ok := checked.AddChecked(wrong, s); ok || checked.Len() != 1 {
			t.Fatal("mismatched identity was accepted")
		}
	})
}
