package engine_test

import (
	"context"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/backend"
	"github.com/oteldb/storage/backend/faultbackend"
	"github.com/oteldb/storage/block"
	"github.com/oteldb/storage/engine"
	"github.com/oteldb/storage/query/fetch"
)

// TestCorruptColumnIsNotSilentlyEmpty covers the reason column objects carry checksums: a column
// that comes back from the store altered must not decode as if it were sound.
//
// The store is made to return one flipped byte in the value column — a bit rot, a truncated write
// that reported success, a misdirected read. Before the checksums the read path reported no error
// and yielded no rows: two hundred samples became an empty result and the caller was told nothing,
// a hole no query can distinguish from "there was no data". So the assertion is that the read must
// *fail*, and fail as [block.ErrCorrupt].
func TestCorruptColumnIsNotSilentlyEmpty(t *testing.T) {
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
		require.ErrorIs(t, err, block.ErrCorrupt) // Detected at plan time.

		return
	}

	_, err = fetch.Drain(ctx, it)
	require.Error(t, err, "a corrupt column must be reported, not read as an absence of rows")
	require.ErrorIs(t, err, block.ErrCorrupt)
}
