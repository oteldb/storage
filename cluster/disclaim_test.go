package cluster

import (
	"net/http/httptest"
	"testing"

	"github.com/go-faster/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// An incomplete answer must fail over exactly like an absent one — every failover site in the tree
// branches on [ErrShardAbsent], and a site that had to learn a second sentinel is a site that can be
// missed. It must not, however, be *only* absence: the two collapse differently.
func TestIncompleteIsADisclaimButNotAnAbsence(t *testing.T) {
	t.Parallel()

	require.ErrorIs(t, ErrShardIncomplete, ErrShardAbsent, "an incomplete owner fails over")
	require.NotErrorIs(t, ErrShardAbsent, ErrShardIncomplete,
		"a node holding nothing is not a node holding something short")
}

// TestIncompleteSentinelCrossesTheWire: a peer's "I hold this shard and it is short" has to survive
// the hop as itself. Flattened to absence, a fan-out where every owner is incomplete would collapse
// to an empty success — the silent short answer the policy exists to prevent.
func TestIncompleteSentinelCrossesTheWire(t *testing.T) {
	t.Parallel()

	require.NotEqual(t, absentStatus, incompleteStatus)

	w := httptest.NewRecorder()
	writeRPCError(w, errors.Wrap(ErrShardIncomplete, "shard 2 wants part p-7"))
	require.Equal(t, incompleteStatus, w.Code)

	remote := statusError("peer:7946", "fetch", w.Code, w.Body.Bytes())
	require.ErrorIs(t, remote, ErrShardIncomplete)
	require.ErrorIs(t, remote, ErrShardAbsent, "still a failover")
	assert.Contains(t, remote.Error(), "peer:7946")
}

func TestDisclaimsCollapse(t *testing.T) {
	t.Parallel()

	for _, tt := range []struct {
		name         string
		errs         []error
		owners       int
		empty, faild bool
	}{
		{"no owner holds it", []error{ErrShardAbsent, ErrShardAbsent}, 2, true, false},
		{"one owner is short", []error{ErrShardAbsent, ErrShardIncomplete}, 2, false, true},
		{"every owner is short", []error{ErrShardIncomplete, ErrShardIncomplete}, 2, false, true},
		{"an owner failed for another reason", []error{ErrShardAbsent, errors.New("timeout")}, 2, false, false},
		{"a short owner and a broken one", []error{ErrShardIncomplete, errors.New("timeout")}, 2, false, false},
		{"nothing disclaimed", nil, 1, false, false},
		{"no owners at all", nil, 0, true, false},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			var d Disclaims
			for _, err := range tt.errs {
				d.Note(err)
			}

			assert.Equal(t, tt.empty, d.Empty(tt.owners), "may answer empty")
			assert.Equal(t, tt.faild, d.Failed(tt.owners), "must fail the read")
			assert.False(t, d.Empty(tt.owners) && d.Failed(tt.owners), "the two are exclusive")
		})
	}
}

// TestDisclaimsCountsWrappedErrors: the sentinels reach a fan-out wrapped by [statusError] and by
// the retry layer, and an unwrapping tally is the difference between counting a disclaim and
// silently treating it as a transport fault.
func TestDisclaimsCountsWrappedErrors(t *testing.T) {
	t.Parallel()

	var d Disclaims

	d.Note(errors.Wrap(ErrShardIncomplete, "peer"))
	d.Note(errors.Wrap(ErrShardAbsent, "peer"))

	assert.Equal(t, int64(1), d.Incomplete())
	assert.Equal(t, int64(1), d.Absent(), "an incomplete answer is not tallied twice")
}
