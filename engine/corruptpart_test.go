package engine_test

import (
	"context"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/backend"
	"github.com/oteldb/storage/backend/faultbackend"
	"github.com/oteldb/storage/engine"
	"github.com/oteldb/storage/query/fetch"
)

// TestCorruptColumnIsNotSilentlyEmpty is the reproducer for #389: a part's column objects carry no
// at-rest checksum (the manifest and marks do), so a column that comes back from the store altered
// is decoded as if it were sound.
//
// The store is made to return one flipped byte in the value column — a bit rot, a truncated write
// that reported success, a misdirected read. The read path reports no error and yields no rows: two
// hundred samples become an empty result, and the caller is told nothing. A hole a query cannot
// distinguish from "there was no data" is worse than a failed query, which is why the assertion
// here is that the read must *fail*, not that it must succeed.
func TestCorruptColumnIsNotSilentlyEmpty(t *testing.T) {
	t.Skip("reproducer for #389: column objects are unchecksummed, so corruption reads as absence")
	t.Parallel()

	ctx := context.Background()
	raw := backend.Memory()
	be := faultbackend.Wrap(raw)

	e := engine.New(engine.Config{Backend: be, Prefix: sharedPrefix})

	s := mkSeries("job", "api")
	for i := range 200 {
		mustAppend(t, e, s, int64(100+i), float64(i))
	}

	require.NoError(t, e.Flush(ctx))

	req := fetch.Request{Start: 0, End: 100000, Matchers: []fetch.Matcher{eqMatcher("job", "api")}}

	sound := newSharedEngine(t, be)
	require.Len(t, fetchAll(t, sound, req), 1, "the part is readable before the store is made to lie")

	be.Add(faultbackend.Rule{
		Kind:  faultbackend.Read,
		Match: func(op faultbackend.Op) bool { return strings.Contains(op.Key, "/c/") },
		Replace: func(_ faultbackend.Op, data []byte) []byte {
			if len(data) < 2 {
				return data
			}

			out := append([]byte(nil), data...)
			out[len(out)/2] ^= 0xff

			return out
		},
	})

	r := engine.New(engine.Config{Backend: be, Prefix: sharedPrefix})
	require.NoError(t, r.LoadParts(ctx))

	it, err := r.Fetch(ctx, req)
	if err != nil {
		return // Detected at plan time, which is all this asks for.
	}

	got, err := fetch.Drain(ctx, it)
	if err != nil {
		return // Detected during decode.
	}

	require.Len(t, got, 1, "a corrupt column must be reported, not read as an absence of rows")
	require.Len(t, got[0].Values, 200)
}
