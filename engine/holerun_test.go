package engine_test

import (
	"fmt"
	"path"
	"testing"

	"github.com/go-faster/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/backend"
	"github.com/oteldb/storage/backend/bucketindex"
)

// TestTransientErrorBreaksAbsenceRun: absences separated by a failed attempt are not consecutive,
// so they do not add up to a hole. The error carries an absent outcome, which it must override.
func TestTransientErrorBreaksAbsenceRun(t *testing.T) {
	t.Parallel()

	be := backend.Memory()

	var fail bool

	f := &fakeFetcher{answer: func(bucketindex.Want) (bucketindex.Entry, bucketindex.WantOutcome, error) {
		if fail {
			return bucketindex.Entry{}, bucketindex.WantAbsent, errors.New("peer unreachable")
		}

		return bucketindex.Entry{}, bucketindex.WantAbsent, nil
	}}
	e := newRepairEngine(t, be, f)

	lost := loseFirstOfTwo(t, e, be)

	mergeTimes(t, e, 1)

	fail = true
	mergeTimes(t, e, 1)
	fail = false

	mergeTimes(t, e, 2)

	assert.Equal(t, []string{lost}, e.WantPrefixes(), "absent, error, absent, absent is not three in a row")
	assert.Empty(t, e.Holes())
	assert.Zero(t, e.LostParts())

	mergeTimes(t, e, 1)

	holes := e.Holes()
	require.Len(t, holes, 1, "three uninterrupted absences after the error do")
	assert.Equal(t, lost, holes[0].Prefix)
}

// TestUnattemptedWantKeepsAbsenceEvidence: only a want the cycle asked for can have its run broken.
// One pushed past the per-cycle fetch cap by wants that fail keeps the absences it had earned.
func TestUnattemptedWantKeepsAbsenceEvidence(t *testing.T) {
	t.Parallel()

	be := backend.Memory()

	blockers := make(map[string]struct{})
	f := &fakeFetcher{answer: func(w bucketindex.Want) (bucketindex.Entry, bucketindex.WantOutcome, error) {
		if _, ok := blockers[w.Prefix]; ok {
			return bucketindex.Entry{}, bucketindex.WantIncomplete, errors.New("peer unreachable")
		}

		return bucketindex.Entry{}, bucketindex.WantAbsent, nil
	}}
	e := newRepairEngine(t, be, f)

	lost := loseFirstOfTwo(t, e, be)

	mergeTimes(t, e, 2)
	require.Empty(t, e.Holes())

	// Wants sorting ahead of it, on blocks nothing covers, fill the cycle's fetch budget and fail.
	for i := range 4 {
		b := fmt.Sprintf("%s/%026d", path.Dir(lost), i)
		require.Less(t, b, lost)

		blockers[b] = struct{}{}
		e.LosePart(b, bucketindex.Interval{Min: uint64(100 + i), Max: uint64(100 + i)})
	}

	before := len(f.asks())
	mergeTimes(t, e, 1)

	capped := f.asks()[before:]
	require.Len(t, capped, 4)
	require.NotContains(t, capped, lost, "the want is past the per-cycle cap")

	// A local part now covers the blockers, so they discharge without a fetch and the want is asked.
	e.SetPartBlocks(e.PartPrefixes()[0], bucketindex.Interval{Min: 100, Max: 103}, 1)
	mergeTimes(t, e, 1)

	holes := e.Holes()
	require.Len(t, holes, 1, "two absences before the capped cycle and one after make three")
	assert.Equal(t, lost, holes[0].Prefix)
	assert.Empty(t, e.WantPrefixes())
}
