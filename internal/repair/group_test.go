package repair

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/backend/bucketindex"
)

func wantOf(e bucketindex.Entry) bucketindex.Want {
	return bucketindex.WantOf(e, bucketindex.Generation{})
}

// memberWants is a node holding f02 of a split group whose ancestor, block 1, it no longer holds,
// and owing f00 and f01. A peer holds f00; f01 is gone from every owner.
func memberWants(t *testing.T) (members []bucketindex.Entry, wants []bucketindex.Want, p *peer) {
	t.Helper()

	members = group("f", 10, 3, bucketindex.Blocks(1))
	wants = []bucketindex.Want{wantOf(members[0]), wantOf(members[1])}
	p = &peer{ix: bucketindex.Index{Entries: []bucketindex.Entry{members[0], members[2]}}}

	return members, wants, p
}

func TestMemberWantCommitsAlone(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name string
		held bool
	}{
		{"Fetched", false},
		{"Held", true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			members, wants, p := memberWants(t)
			live := []bucketindex.Entry{members[2]}
			pass := Pass{Fetcher: p}

			if tc.held {
				pass.Hold = func(_ context.Context, prefix string) bool { return prefix == members[0].Prefix }
			}

			evidence := make(map[string]int)

			var lost []bucketindex.Want

			for range HoleConfirmations {
				plan := pass.Run(context.Background(), live, wants, nil)

				require.Len(t, plan.Units, 1, "a member holding rows this node lacks commits without its group")
				assert.Equal(t, []string{"f00"}, prefixes(plan.Units[0]))

				lost = ConfirmLost(evidence, wants, plan.Attempts, plan.Failed)
			}

			assert.Equal(t, []bucketindex.Want{wants[1]}, lost, "only the member no owner holds is lost")
		})
	}
}

func TestMemberAbsenceIsNoEvidenceAgainstASatisfiedTarget(t *testing.T) {
	t.Parallel()

	members, wants, p := memberWants(t)
	// The ancestor is still here, so f00 alone would duplicate its rows: the group is required.
	live := []bucketindex.Entry{members[2], {Prefix: "ancestor", Blocks: bucketindex.Blocks(1)}}
	evidence := make(map[string]int)

	var lost []bucketindex.Want

	for range HoleConfirmations {
		plan := Pass{Fetcher: p}.Run(context.Background(), live, wants, nil)

		assert.Empty(t, plan.Units)
		assert.Equal(t, []string{"f00"}, plan.Failed, "a satisfied target concludes nothing from its group")

		lost = ConfirmLost(evidence, wants, plan.Attempts, plan.Failed)
	}

	assert.Equal(t, []bucketindex.Want{wants[1]}, lost)
}

func TestSupersededResultIsNotPublished(t *testing.T) {
	t.Parallel()

	members := group("f", 10, 3, bucketindex.Blocks(1))
	// Between rounds the peer merged f00 and f01; the output keeps the group's unrealized claim.
	merged := bucketindex.Entry{
		Prefix: "t", Blocks: bucketindex.Interval{Min: 10, Max: 11}, Claim: members[0].Claim, Level: 2,
	}
	p := &peer{
		ix:     bucketindex.Index{Entries: members},
		member: map[uint64]bucketindex.FetchResult{11: {Entry: merged}},
	}

	plan := Pass{Fetcher: p}.Run(context.Background(), nil, []bucketindex.Want{want("lost", 1)}, nil)
	require.Len(t, plan.Units, 1)
	require.ElementsMatch(t, []string{"f00", "t", "f02"}, prefixes(plan.Units[0]))

	var stats bucketindex.RepairStats

	got := Admit(context.Background(), nil, plan.Units, func(*Result) error { return nil }, &stats)

	assert.Equal(t, []bucketindex.Entry{merged, members[2]}, got, "f00's rows are inside t")
	assert.Equal(t, int64(2), stats.Fetched)
}

func TestSharedGroupCountsEachPartOnce(t *testing.T) {
	t.Parallel()

	members := group("f", 10, 3, bucketindex.Interval{Min: 1, Max: 2})
	p := &peer{ix: bucketindex.Index{Entries: members}}

	plan := Pass{Fetcher: p}.Run(context.Background(), nil,
		[]bucketindex.Want{want("a", 1), want("b", 2)}, nil)
	require.Len(t, plan.Units, 2)

	var stats bucketindex.RepairStats

	got := Admit(context.Background(), nil, plan.Units, func(*Result) error { return nil }, &stats)

	assert.Len(t, got, 3)
	assert.Equal(t, bucketindex.RepairStats{Fetched: 3}, stats, "three parts published, three fetched")
}
