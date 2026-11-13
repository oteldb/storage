package series

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/signal"
)

func TestEmptyContextInterning(t *testing.T) {
	t.Parallel()
	for _, shared := range []bool{false, true} {
		name := "owned"
		var pool *Contexts
		if shared {
			name = "shared"
			pool = NewContexts(1 << 20)
		}
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			for _, tc := range []struct {
				name     string
				resource signal.Resource
				scope    signal.Scope
			}{
				{name: "empty"},
				{name: "empty-slices", resource: signal.Resource{Attributes: signal.Attributes{}}, scope: signal.Scope{Attributes: signal.Attributes{}}},
				{name: "resource-schema", resource: signal.Resource{SchemaURL: []byte("r")}},
				{name: "resource-attrs", resource: signal.Resource{Attributes: signal.NewAttributes(signal.KeyValue{Key: []byte("k"), Value: sv("v")})}},
				{name: "scope-name", scope: signal.Scope{Name: []byte("name")}},
				{name: "scope-version", scope: signal.Scope{Version: []byte("v")}},
				{name: "scope-schema", scope: signal.Scope{SchemaURL: []byte("s")}},
				{name: "scope-attrs", scope: signal.Scope{Attributes: signal.NewAttributes(signal.KeyValue{Key: []byte("k"), Value: sv("v")})}},
			} {
				t.Run(tc.name, func(t *testing.T) {
					t.Parallel()
					ix := NewWithContexts(pool)
					// Empty references need not be zero after another context is registered.
					ix.Add(signal.Series{Resource: signal.Resource{SchemaURL: []byte("first")}, Scope: signal.Scope{Name: []byte("first")}})
					for _, s := range []signal.Series{{Resource: tc.resource, Scope: tc.scope}, {}, {Resource: tc.resource, Scope: tc.scope}, {}} {
						id := ix.Add(s)
						require.Equal(t, s.Hash(), id)
						got, ok := ix.Get(id)
						require.True(t, ok)
						require.True(t, s.Equal(got))
						ref, ok := ix.Ref(id)
						require.True(t, ok)
						require.Equal(t, s.AppendHashInput(nil), ix.View().AppendCanonical(nil, int(ref)))
					}
					// Distinct point attributes must traverse context interning again.
					s := signal.Series{Resource: tc.resource, Scope: tc.scope, Attributes: signal.NewAttributes(signal.KeyValue{Key: []byte("point"), Value: sv("other")})}
					id := ix.Add(s)
					got, ok := ix.Get(id)
					require.True(t, ok)
					require.True(t, s.Equal(got))
					s = signal.Series{Attributes: s.Attributes}
					id = ix.Add(s)
					got, ok = ix.Get(id)
					require.True(t, ok)
					require.True(t, s.Equal(got))
				})
			}
		})
	}
}

func BenchmarkContextInterning(b *testing.B) {
	for _, mode := range []string{"empty", "populated"} {
		r := signal.Resource{}
		s := signal.Scope{}
		if mode == "populated" {
			r.SchemaURL = []byte("schema")
			r.Attributes = signal.NewAttributes(signal.KeyValue{Key: []byte("service.name"), Value: sv("api")})
			s.Name = []byte("instrumentation")
			s.Version = []byte("1")
		}
		b.Run(mode, func(b *testing.B) {
			b.Run("resource", func(b *testing.B) {
				ix := New()
				want := ix.internResource(r)
				b.ReportAllocs()
				for b.Loop() {
					if ix.internResource(r) != want {
						b.Fatal("resource changed")
					}
				}
			})
			b.Run("scope", func(b *testing.B) {
				ix := New()
				want := ix.internScope(s)
				b.ReportAllocs()
				for b.Loop() {
					if ix.internScope(s) != want {
						b.Fatal("scope changed")
					}
				}
			})
		})
	}
}
