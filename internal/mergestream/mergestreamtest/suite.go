package mergestreamtest

import (
	"context"
	"slices"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Row is one row of a merge's input or output. Stream is the index of the stream (metric series or
// record stream) it belongs to; the engine owns the identity behind it, so the suite never needs to
// forge a [signal.SeriesID] an engine would reject.
type Row struct {
	Stream int
	Ts     int64
	Val    int64
}

// Store is one engine's merge path, adapted so the suite can drive it.
type Store interface {
	// Ingest writes rows as one new source part. The suite ingests sources oldest first.
	Ingest(ctx context.Context, rows []Row) error
	// Merge compacts the parts the store holds.
	Merge(ctx context.Context) error
	// Rows returns every row the store holds, in any order.
	Rows(ctx context.Context) ([]Row, error)
	// Parts returns the identifiers of the parts the store exposes.
	Parts() []string
	// Want returns the rows a merge of src must leave behind, computed naively. Engines differ:
	// records concatenate, metrics keep the last source's value for a duplicate timestamp.
	Want(src [][]Row) []Row
}

// Sources is the suite's input: three parts with overlapping and part-unique streams, and
// timestamps repeated across parts so an engine that dedups has something to dedup.
func Sources() [][]Row {
	return [][]Row{
		{{Stream: 0, Ts: 10, Val: 1}, {Stream: 0, Ts: 20, Val: 2}, {Stream: 2, Ts: 10, Val: 3}},
		{{Stream: 0, Ts: 20, Val: 4}, {Stream: 1, Ts: 15, Val: 5}, {Stream: 3, Ts: 30, Val: 6}},
		{{Stream: 1, Ts: 15, Val: 7}, {Stream: 2, Ts: 40, Val: 8}, {Stream: 4, Ts: 50, Val: 9}},
	}
}

// Run exercises the invariants a merge must hold in either engine: the output row multiset is what
// a naive merge would produce, no source object is read twice, and a merge that fails partway
// commits nothing. open builds a store over the suite's [Backend].
func Run(t *testing.T, open func(t *testing.T, be *Backend) Store) {
	t.Helper()

	t.Run("RowMultiset", func(t *testing.T) {
		t.Parallel()

		ctx := context.Background()
		src := Sources()
		s := seed(t, open(t, NewBackend()), src)

		before := len(s.Parts())
		require.NoError(t, s.Merge(ctx))
		assert.Less(t, len(s.Parts()), before, "the merge must actually compact, or the suite proves nothing")

		got, err := s.Rows(ctx)
		require.NoError(t, err)
		assert.Equal(t, sortRows(s.Want(src)), sortRows(got))
	})

	t.Run("SourceReadOnce", func(t *testing.T) {
		t.Parallel()

		ctx := context.Background()
		be := NewBackend()
		s := seed(t, open(t, be), Sources())

		before := be.Reads()
		require.NoError(t, s.Merge(ctx))

		var read int

		for key, n := range be.Reads() {
			if n -= before[key]; n == 0 {
				continue
			}

			read++

			assert.LessOrEqual(t, n, 1, "source object %q was read %d times by one merge", key, n)
		}

		assert.Positive(t, read, "the merge read no source object at all")
	})

	t.Run("FailedMergeCommitsNothing", func(t *testing.T) {
		t.Parallel()

		ctx := context.Background()
		src := Sources()

		be := NewBackend()
		s := seed(t, open(t, be), src)
		be.FailWriteAt(0)
		require.NoError(t, s.Merge(ctx))

		writes := be.Writes()
		require.Positive(t, writes, "a merge that writes nothing cannot fail partway")

		for n := 1; n <= writes; n++ {
			be := NewBackend()
			s := seed(t, open(t, be), src)

			parts := slices.Clone(s.Parts())
			rows, err := s.Rows(ctx)
			require.NoError(t, err)

			be.FailWriteAt(n)
			require.Error(t, s.Merge(ctx), "write %d/%d: merge must surface the failure", n, writes)

			assert.ElementsMatch(t, parts, s.Parts(), "write %d/%d: a failed merge committed a part", n, writes)

			after, err := s.Rows(ctx)
			require.NoError(t, err)
			assert.Equal(t, sortRows(rows), sortRows(after), "write %d/%d: a failed merge lost rows", n, writes)
		}
	})
}

func seed(t *testing.T, s Store, src [][]Row) Store {
	t.Helper()

	ctx := context.Background()
	for _, rows := range src {
		require.NoError(t, s.Ingest(ctx, rows))
	}

	require.Len(t, s.Parts(), len(src))

	return s
}

func sortRows(rows []Row) []Row {
	out := slices.Clone(rows)
	slices.SortFunc(out, func(a, b Row) int {
		if a.Stream != b.Stream {
			return a.Stream - b.Stream
		}

		if a.Ts != b.Ts {
			return int(a.Ts - b.Ts)
		}

		return int(a.Val - b.Val)
	})

	return out
}
