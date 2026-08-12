package identity_test

import (
	"testing"

	"github.com/oteldb/storage/index/identity"
	"github.com/oteldb/storage/index/series"
	"github.com/oteldb/storage/signal"
)

// FuzzDecode asserts the decoder never panics on arbitrary bytes — it parses durable objects that a
// truncated write or bit-rot can corrupt, so every bound must be checked. Valid objects are seeded
// from the round-trip tests so the fuzzer starts inside the format.
func FuzzDecode(f *testing.F) {
	f.Add(identity.Encode(nil, nil))
	f.Add(identity.Encode(nil, []series.Entry{mkEntry(0)}))

	entries := make([]series.Entry, 8)
	for i := range entries {
		entries[i] = mkEntry(i)
	}

	f.Add(identity.Encode(nil, entries))

	f.Fuzz(func(_ *testing.T, data []byte) {
		_ = identity.Decode(data, func(signal.SeriesID, signal.Series) error { return nil })
		_, _ = identity.Count(data)
	})
}

// FuzzRoundTrip asserts encode∘decode is the identity on arbitrary label content: whatever bytes an
// attribute carries, the decoded identity must equal the encoded one and still hash to its id (the
// id is content-addressed, so a lossy encoding would silently orphan a part's rows).
func FuzzRoundTrip(f *testing.F) {
	f.Add("service.name", "api", "__name__", "up", int64(1))
	f.Add("", "", "", "", int64(0))
	f.Add("k", "\x00\xff", "n", "v", int64(-1))

	f.Fuzz(func(t *testing.T, rk, rv, ak, av string, num int64) {
		s := signal.Series{
			Resource: signal.Resource{
				SchemaURL:  []byte(rk),
				Attributes: signal.NewAttributes(signal.KeyValue{Key: []byte(rk), Value: signal.StringValue([]byte(rv))}),
			},
			Scope: signal.Scope{Name: []byte(ak), Version: []byte(av)},
			Attributes: signal.NewAttributes(
				signal.KeyValue{Key: []byte(ak), Value: signal.StringValue([]byte(av))},
				signal.KeyValue{Key: []byte(ak + "_n"), Value: signal.IntValue(num)},
			),
		}
		ent := series.Entry{ID: s.Hash(), Series: s}

		var got []series.Entry

		if err := identity.Decode(identity.Encode(nil, []series.Entry{ent}),
			func(id signal.SeriesID, d signal.Series) error {
				got = append(got, series.Entry{ID: id, Series: d.Clone()})

				return nil
			}); err != nil {
			t.Fatalf("decode: %v", err)
		}

		if len(got) != 1 {
			t.Fatalf("got %d identities, want 1", len(got))
		}

		if got[0].ID != ent.ID {
			t.Fatalf("id %v != %v", got[0].ID, ent.ID)
		}

		if !got[0].Series.Equal(ent.Series) {
			t.Fatal("identity did not round-trip")
		}

		if got[0].Series.Hash() != ent.ID {
			t.Fatal("decoded identity does not hash to its id")
		}
	})
}
