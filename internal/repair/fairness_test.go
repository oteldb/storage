package repair

import (
	"context"
	"strconv"
	"testing"

	"github.com/go-faster/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/backend/bucketindex"
)

func TestAdmitIsOrderIndependent(t *testing.T) {
	t.Parallel()

	members := group("f", 10, 3, bucketindex.Blocks(1))
	successor := bucketindex.Entry{Prefix: "s", Blocks: bucketindex.Blocks(1), Level: 2}
	member := Unit{{Target: Target{Want: wantOf(members[0])}, Entry: members[0]}}
	merged := Unit{{Target: Target{Want: wantOf(successor)}, Entry: successor}}

	for _, units := range [][]Unit{{member, merged}, {merged, member}} {
		var stats bucketindex.RepairStats

		got := Admit(context.Background(), nil, units, func(*Result) error { return nil }, &stats)

		assert.Equal(t, []bucketindex.Entry{successor}, got,
			"the member's rows are inside the successor's ancestry, so it waits for its group")
		assert.Equal(t, bucketindex.RepairStats{Fetched: 1, Failed: 1}, stats)
	}
}

// fetchFunc answers each want with the entry it returns; nil is a transient failure.
type fetchFunc func(w bucketindex.Want) *bucketindex.Entry

func (f fetchFunc) FetchWants(_ context.Context, wants []bucketindex.Want) []bucketindex.FetchResult {
	out := make([]bucketindex.FetchResult, len(wants))

	for i := range wants {
		if ent := f(wants[i]); ent != nil {
			out[i].Entry = *ent
		} else {
			out[i].Err = errors.New("unreachable")
		}
	}

	return out
}

func TestStuckWantsDoNotStarveTheRest(t *testing.T) {
	t.Parallel()

	good := bucketindex.Entry{Prefix: "z", Blocks: bucketindex.Blocks(9)}
	h := &host{disk: map[string]bucketindex.Entry{}}

	for i := range uint64(FetchesPerCycle) {
		h.wants = append(h.wants, want(strconv.FormatUint(i, 10), i+1))
	}

	h.wants = append(h.wants, wantOf(good))

	var (
		s     State
		asked []string
	)

	f := fetchFunc(func(w bucketindex.Want) *bucketindex.Entry {
		asked = append(asked, w.Prefix)
		if w.Prefix == good.Prefix {
			h.disk[good.Prefix] = good

			return &good
		}

		return nil
	})

	drive(t, &s, h, f)
	require.NotContains(t, asked, good.Prefix, "the first pass takes the wants in order")

	drive(t, &s, h, f)
	assert.Equal(t, []string{"z"}, h.prefixes(), "the want never tried goes before those that failed")
	assert.Len(t, h.wants, FetchesPerCycle)

	drive(t, &s, h, f)
	assert.NotContains(t, s.tried, good.Prefix, "a discharged want is forgotten")
}

// TestStuckMemberDoesNotStarveTheRest is a member whose group must be complete, because the ancestor
// is still here, and whose sibling is already a hole: the group can never complete, and committing
// the member alone would duplicate the ancestor's rows. It stays outstanding, but other wants go on.
func TestStuckMemberDoesNotStarveTheRest(t *testing.T) {
	t.Parallel()

	members, wants, p := memberWants(t)
	ancestor := bucketindex.Entry{Prefix: "ancestor", Blocks: bucketindex.Blocks(1)}
	good := bucketindex.Entry{Prefix: "z", Blocks: bucketindex.Blocks(9)}
	hole := members[1]
	hole.Hole = true

	p.ix.Entries = append(p.ix.Entries, good)
	h := &host{
		wants: []bucketindex.Want{wants[0]},
		holes: []bucketindex.Entry{hole},
		parts: []*fakePart{{ent: members[2]}, {ent: ancestor}},
		disk:  map[string]bucketindex.Entry{},
	}
	p.disk = h.disk

	for i := range uint64(FetchesPerCycle) {
		h.wants = append(h.wants, want("a"+strconv.FormatUint(i, 10), 20+i))
	}

	h.wants = append(h.wants, wantOf(good))

	var s State

	for range 3 {
		drive(t, &s, h, p)
	}

	assert.Contains(t, h.prefixes(), "z", "the stuck member does not hold its slot for ever")
	assert.NotContains(t, h.prefixes(), members[0].Prefix, "the member never lands beside its ancestor")
	assert.Contains(t, wantPrefixes(h.wants), members[0].Prefix)
}

func wantPrefixes(ws []bucketindex.Want) []string {
	out := make([]string, len(ws))
	for i := range ws {
		out[i] = ws[i].Prefix
	}

	return out
}
