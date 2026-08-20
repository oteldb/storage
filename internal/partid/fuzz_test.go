package partid_test

import (
	"testing"

	"github.com/oteldb/storage/internal/partid"
)

func FuzzParse(f *testing.F) {
	f.Add("00000000000000000000000000")
	f.Add("7ZZZZZZZZZZZZZZZZZZZZZZZZZ")
	f.Add(partid.New().String())
	f.Add("0000000000")
	f.Add("")

	f.Fuzz(func(t *testing.T, s string) {
		id, err := partid.Parse(s)
		if err != nil {
			return
		}

		if got := id.String(); got != s {
			t.Fatalf("re-encode mismatch: %q != %q", got, s)
		}
	})
}

func FuzzRoundTrip(f *testing.F) {
	f.Add(make([]byte, partid.Len))
	f.Add([]byte{
		0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF,
		0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF,
	})

	f.Fuzz(func(t *testing.T, b []byte) {
		if len(b) != partid.Len {
			return
		}

		var id partid.ID
		copy(id[:], b)

		got, err := partid.Parse(id.String())
		if err != nil {
			t.Fatalf("parse %q: %v", id.String(), err)
		}

		if got != id {
			t.Fatalf("round trip: %x != %x", got, id)
		}
	})
}
