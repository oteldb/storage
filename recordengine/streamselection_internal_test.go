package recordengine

import (
	"context"
	"fmt"
	"math/rand/v2"
	"slices"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/backend"
	"github.com/oteldb/storage/query/fetch"
	"github.com/oteldb/storage/readbudget"
	"github.com/oteldb/storage/signal"
)

func TestSizeStreamsLivenessMatchesSizing(t *testing.T) {
	t.Parallel()
	rng := rand.New(rand.NewPCG(365, 357))
	for trial := range 100 {
		part := &part{}
		for i := range 32 {
			if rng.IntN(3) == 0 {
				continue
			}
			part.ranges = append(part.ranges, streamRange{id: signal.SeriesID{Lo: uint64(i)}, rowRange: rowRange{start: i * 3, end: i*3 + 3}})
		}
		var ids []signal.SeriesID
		for range rng.IntN(64) {
			ids = append(ids, signal.SeriesID{Lo: uint64(rng.IntN(48))})
		}
		ids = sortedIDsOf(ids)
		plan := &fetchPlan{sortedIDs: ids}
		rows := make([]int, len(ids))
		want := false
		for k, id := range ids {
			for _, sr := range part.ranges {
				if sr.id == id {
					rows[k] += sr.end - sr.start
					want = true
				}
			}
		}
		got := make([]int, len(ids))
		assert.Equal(t, want, plan.sizeStreams(part, got), "trial %d", trial)
		assert.Equal(t, rows, got, "trial %d", trial)
		assert.Equal(t, want, plan.sizeStreams(part, nil), "liveness trial %d", trial)
	}
}

func TestStreamSelectionReusedAcrossParts(t *testing.T) {
	t.Parallel()
	e, ids, all := rangeFixture(t, 9, 24, 4)
	t.Cleanup(func() {
		for _, part := range e.parts {
			assert.Zero(t, part.refs.Load())
		}
	})
	rng := rand.New(rand.NewPCG(365, 1))
	for trial := range 12 {
		var asked []signal.SeriesID
		for _, id := range ids {
			if rng.IntN(3) == 0 {
				asked = append(asked, id)
			}
		}
		asked = append(asked, signal.SeriesID{Lo: 1}, signal.SeriesID{Hi: 1 << 63})
		start := int64(rng.IntN(9000))
		end := start + int64(rng.IntN(4000))
		for _, budgeted := range []bool{false, true} {
			for _, filtered := range []bool{false, true} {
				t.Run(fmt.Sprintf("trial=%d/budget=%t/filtered=%t", trial, budgeted, filtered), func(t *testing.T) {
					t.Parallel()
					ctx := t.Context()
					budget := readbudget.New(16 << 20)
					if budgeted {
						ctx = readbudget.With(ctx, budget)
					}
					req := fetch.Request{Start: start, End: end, AllConditions: filtered}
					if filtered {
						req.Conditions = []fetch.Condition{{Column: "sev", Match: func(v signal.Value) bool { return v.Int()%2 == 0 }}}
					}
					e.mu.RLock()
					plan, err := e.planFetch(ctx, asked, req)
					e.mu.RUnlock()
					defer plan.releaseParts()
					defer plan.releaseBudget()
					require.NoError(t, err)
					// Visit ranges in a different order to catch scratch retained from an adjacent part.
					slices.Reverse(plan.liveParts)
					require.NoError(t, plan.readParts(ctx))
					for _, id := range asked {
						var want []int64
						for _, ts := range all[id] {
							severity := (ts%1000 - int64(slices.Index(ids, id))) / 10
							if ts >= start && ts <= end && (!filtered || severity%2 == 0) {
								want = append(want, ts)
							}
						}
						got := slices.Clone(plan.accs[id].ts)
						slices.Sort(got)
						assert.True(t, slices.Equal(want, got), "stream %v: want %v, got %v", id, want, got)
					}
					assert.LessOrEqual(t, len(plan.matched), len(asked), "only one part's matches may be retained")
					plan.releaseBudget()
					if budgeted {
						assert.Equal(t, int64(16<<20), budget.Remaining())
					}
				})
			}
		}
	}
}

func TestWindowGranulesUsesResolvedRanges(t *testing.T) {
	t.Parallel()
	p, ids := granuleTestPartAt(t, 5, 4000, 10000)
	matched := p.heldStreams(nil, sortedIDsOf(ids[:2]))
	require.Len(t, matched, 2)
	want := p.windowGranules(t.Context(), matched, 0, maxInt64)
	require.NotEmpty(t, want)
	// No lookup in the part's index is needed after resolution, and its row ranges are immutable.
	p.ranges = nil
	slices.Reverse(matched)
	matched = append(matched, matched[0])
	assert.Equal(t, want, p.windowGranules(t.Context(), matched, 0, maxInt64))
}

type selectionCancelBackend struct {
	backend.Backend

	cancel context.CancelFunc
}

func (b *selectionCancelBackend) Read(ctx context.Context, key string) ([]byte, error) {
	if b.cancel != nil && strings.Contains(key, "/c/") {
		b.cancel()
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return b.Backend.Read(ctx, key)
}

func TestStreamSelectionCancellationReleasesPlan(t *testing.T) {
	t.Parallel()
	for _, budgeted := range []bool{false, true} {
		for _, filtered := range []bool{false, true} {
			t.Run(fmt.Sprintf("budget=%t/filtered=%t", budgeted, filtered), func(t *testing.T) {
				t.Parallel()
				be := &selectionCancelBackend{Backend: backend.Memory()}
				e := projectedBudgetEngineOn(t, 64, "part", be)
				ctx, cancel := context.WithCancel(t.Context())
				defer cancel()
				be.cancel = cancel
				budget := readbudget.New(4 << 20)
				if budgeted {
					ctx = readbudget.With(ctx, budget)
				}
				req := fetch.Request{Start: 0, End: 63, AllConditions: filtered}
				if filtered {
					req.Conditions = []fetch.Condition{{Column: "severity", Match: func(signal.Value) bool { return true }}}
				}
				_, err := e.Fetch(ctx, req)
				require.ErrorIs(t, err, context.Canceled)
				assert.Zero(t, e.parts[0].refs.Load())
				if budgeted {
					assert.Equal(t, int64(4<<20), budget.Remaining())
				}
			})
		}
	}
}
