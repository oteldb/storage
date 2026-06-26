package storage

import (
	"context"
	"slices"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/query/fetch"
	"github.com/oteldb/storage/signal"
	"github.com/oteldb/storage/signal/profile"
)

// flame is a minimal flamegraph node — the shape an embedder's adapter folds storage samples into
// (mirroring oteldb profilestorage.FlameNode: Name, Self, Total, Children).
type flame struct {
	name     string
	self     int64
	total    int64
	children map[string]*flame
}

func newFlame() *flame { return &flame{children: map[string]*flame{}} }

// add folds a resolved (leaf-first) stack and its value into the tree, caller→leaf.
func (f *flame) add(frames []profile.Frame, v int64) {
	f.total += v
	cur := f
	for _, fr := range slices.Backward(frames) { // caller (outermost) → leaf
		name := fr.Function
		ch := cur.children[name]
		if ch == nil {
			ch = &flame{name: name, children: map[string]*flame{}}
			cur.children[name] = ch
		}

		ch.total += v
		cur = ch
	}

	cur.self += v // leaf
}

// TestProfilesAdapterFlamegraph proves the public API is sufficient to implement the oteldb
// profilestorage Querier's SelectMergeProfile → FlameTree: select samples by a type+label matcher,
// resolve each row's stack_id to function frames, and merge into a flamegraph — all over the
// storage facade, no oteldb import.
func TestProfilesAdapterFlamegraph(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	s, err := InMemory()
	require.NoError(t, err)
	t.Cleanup(func() { _ = s.Close(ctx) })

	// Two stacks sharing main(): main→work (100) and main→idle (30).
	var pd profile.Profiles
	d := &pd.Dictionary
	mainLoc := d.AddLocation(profile.Location{Lines: []profile.Line{{FunctionIndex: d.AddFunction(profile.Function{NameStrindex: d.InternString([]byte("main"))})}}})
	workLoc := d.AddLocation(profile.Location{Lines: []profile.Line{{FunctionIndex: d.AddFunction(profile.Function{NameStrindex: d.InternString([]byte("work"))})}}})
	idleLoc := d.AddLocation(profile.Location{Lines: []profile.Line{{FunctionIndex: d.AddFunction(profile.Function{NameStrindex: d.InternString([]byte("idle"))})}}})

	rp := pd.AddResource()
	rp.Resource = signal.Resource{Attributes: signal.NewAttributes(
		signal.KeyValue{Key: []byte("service.name"), Value: signal.StringValue([]byte("api"))},
	)}
	pr := rp.AddScope().AddProfile()
	pr.SampleType = profile.ValueType{TypeStrindex: d.InternString([]byte("cpu")), UnitStrindex: d.InternString([]byte("nanoseconds"))}
	pr.TimeNanos = 1000
	w := pr.AddSample()
	w.StackIndex, w.Values = d.AddStack(workLoc, mainLoc), []int64{100}
	i := pr.AddSample()
	i.StackIndex, i.Values = d.AddStack(idleLoc, mainLoc), []int64{30}

	_, err = s.WriteProfiles(ctx, pd)
	require.NoError(t, err)

	// --- adapter: SelectMergeProfile(cpu, {service.name="api"}) ---

	req := fetch.Request{
		Signal: signal.Profile, Start: 0, End: 1 << 60,
		Matchers: []fetch.Matcher{nameMatcherSvc("api"), labelMatcher(profile.LabelSampleType, "cpu")},
		// project just the value + stack id (what the merge needs).
		Projection: []string{profile.ColValue, profile.ColStackID},
	}
	batches, err := fetch.Drain(ctx, must(s.ProfileFetcher("default").Fetch(ctx, req)))
	require.NoError(t, err)
	require.Len(t, batches, 1)

	resolver, err := s.ProfileResolver(ctx, "default")
	require.NoError(t, err)
	require.NotNil(t, resolver)

	root := newFlame()
	for _, b := range batches {
		values, _ := b.Column(profile.ColValue)
		stacks, _ := b.Column(profile.ColStackID)
		require.Len(t, values.Int64, len(stacks.Bytes))

		for row := range stacks.Bytes {
			root.add(resolver.Resolve(stacks.Bytes[row]), values.Int64[row])
		}
	}

	// root(130) → main(130) → {work: self 100, idle: self 30}.
	assert.Equal(t, int64(130), root.total)
	require.Contains(t, root.children, "main")
	main := root.children["main"]
	assert.Equal(t, int64(130), main.total)
	require.Contains(t, main.children, "work")
	require.Contains(t, main.children, "idle")
	assert.Equal(t, int64(100), main.children["work"].self)
	assert.Equal(t, int64(30), main.children["idle"].self)

	// After a flush the symbols live in a part sidecar, not the head; the resolver still resolves
	// (SideSnapshot unions parts) — content ids are stable across the flush.
	require.NoError(t, s.profileEngineFor("default").Flush(ctx))
	flushed, err := s.ProfileResolver(ctx, "default")
	require.NoError(t, err)

	var anyStackID []byte
	for _, b := range batches {
		stacks, _ := b.Column(profile.ColStackID)
		anyStackID = stacks.Bytes[0]

		break
	}
	frames := flushed.Resolve(anyStackID)
	require.NotEmpty(t, frames)
	assert.Equal(t, "main", frames[len(frames)-1].Function)
}
