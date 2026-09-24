package storage

import (
	"context"
	"strings"
	"testing"

	"github.com/go-faster/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/backend"
	"github.com/oteldb/storage/backend/bucketindex"
	"github.com/oteldb/storage/backend/faultbackend"
)

var errBackendDown = errors.New("injected: backend unavailable")

// failUnder fails every Read and ReadVersioned under prefix with an error that is not
// [backend.ErrNotExist]: a backend that could not answer, as opposed to one that said "not there".
// It replaces any earlier prefix; an empty one heals the backend. The wrapper hides the inner
// backend's optional interfaces, so every sized, viewed or ranged read falls back to Read and is
// failed here too.
func failUnder(be *faultbackend.Backend, prefix string) {
	be.Reset()

	if prefix == "" {
		return
	}

	match := func(op faultbackend.Op) bool { return strings.HasPrefix(op.Key, prefix) }
	be.Add(faultbackend.Rule{Kind: faultbackend.Read, Match: match, Err: errBackendDown})
	be.Add(faultbackend.Rule{Kind: faultbackend.ReadVersioned, Match: match, Err: errBackendDown})
}

func TestSoleOwnerRepairerEvidence(t *testing.T) {
	t.Parallel()

	const (
		prefix = "default/metrics"
		lost   = prefix + "/p1"
	)

	want := bucketindex.Want{Prefix: lost, Blocks: bucketindex.Blocks(1), MinTime: 100, MaxTime: 100}
	hole := want.Entry()
	hole.Hole = true
	successor := bucketindex.Entry{Prefix: prefix + "/p2", Blocks: bucketindex.Blocks(1, 2), Level: 1}

	for _, tc := range []struct {
		name    string
		index   bucketindex.Index
		present bool
		down    string
		outcome bucketindex.WantOutcome
		err     bool
	}{
		{name: "OwedAndAbsent", index: bucketindex.Index{Wanted: []bucketindex.Want{want}}, outcome: bucketindex.WantAbsent},
		{name: "HoleAndAbsent", index: bucketindex.Index{Entries: []bucketindex.Entry{hole}}, outcome: bucketindex.WantAbsent},
		{
			name: "PresentButUnopenable", index: bucketindex.Index{Wanted: []bucketindex.Want{want}},
			present: true, err: true,
		},
		{
			name: "MergedIntoASuccessor", outcome: bucketindex.WantIncomplete,
			index: bucketindex.Index{Wanted: []bucketindex.Want{want}, Entries: []bucketindex.Entry{successor}},
		},
		{
			name: "Tombstoned", outcome: bucketindex.WantIncomplete,
			index: bucketindex.Index{
				Wanted: []bucketindex.Want{want}, Removed: []bucketindex.Removal{{Prefix: lost}},
			},
		},
		{name: "NotOwed", outcome: bucketindex.WantIncomplete},
		{name: "IndexUnreadable", index: bucketindex.Index{Wanted: []bucketindex.Want{want}}, down: prefix + "/" + bucketindex.Object, err: true},
		{name: "ProbeFails", index: bucketindex.Index{Wanted: []bucketindex.Want{want}}, down: lost + "/", err: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			ctx := context.Background()

			be := faultbackend.Wrap(backend.Memory())
			_, err := tc.index.Save(ctx, be, prefix+"/"+bucketindex.Object, backend.VersionAbsent)
			require.NoError(t, err)

			if tc.present {
				require.NoError(t, be.Write(ctx, lost+"/manifest", []byte("unreadable")))
			}

			failUnder(be, tc.down)

			got := soleOwnerRepairer{backend: be, prefix: prefix}.FetchWants(ctx, []bucketindex.Want{want})
			require.Len(t, got, 1)

			if tc.err {
				require.Error(t, got[0].Err, "a failure is never evidence")
				assert.NotErrorIs(t, got[0].Err, backend.ErrNotExist)

				return
			}

			require.NoError(t, got[0].Err)
			assert.Equal(t, tc.outcome, got[0].Outcome)
		})
	}
}

func TestRepairerForSelectsTheEvidenceRule(t *testing.T) {
	t.Parallel()

	assert.IsType(t, soleOwnerRepairer{}, (&Storage{opts: Options{}}).metricRepairerFor("t", "t/metrics"))
	assert.IsType(t, recordPartRepairer{}, (&Storage{opts: Options{}}).recordRepairerFor("t", "t/logs"))
	assert.Nil(t, (&Storage{opts: Options{ReadOnly: true}}).metricRepairerFor("t", "t/metrics"),
		"a read-only store must never commit a hole")
	assert.Nil(t, (&Storage{opts: Options{ReadOnly: true}}).recordRepairerFor("t", "t/logs"))
}
