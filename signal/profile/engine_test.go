package profile

import (
	"context"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/backend"
	"github.com/oteldb/storage/recordengine"
	"github.com/oteldb/storage/signal"
)

// oneSampleBatch builds a Profiles with a single "api" profile whose one sample uses the shared
// "main→work" stack, valued v.
func oneSampleBatch(v int64) Profiles {
	var pd Profiles
	d := &pd.Dictionary
	main := d.AddLocation(Location{Lines: []Line{{FunctionIndex: d.AddFunction(Function{NameStrindex: d.InternString([]byte("main"))})}}})
	work := d.AddLocation(Location{Lines: []Line{{FunctionIndex: d.AddFunction(Function{NameStrindex: d.InternString([]byte("work"))})}}})
	st := d.AddStack(work, main)

	rp := pd.AddResource()
	rp.Resource.Attributes = nil
	pr := rp.AddScope().AddProfile()
	pr.TimeNanos = v
	s := pr.AddSample()
	s.StackIndex = st
	s.Values = []int64{v}

	return pd
}

// TestEngineSymbolStoreDedupAcrossParts drives a real record engine with the profiles SymbolStore:
// two flushed parts each reference the same stack, and after a merge the unioned stacks sidecar holds
// that stack exactly once — the content-addressed dedup the symbol store exists for.
func TestEngineSymbolStoreDedupAcrossParts(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	be := backend.Memory()
	eng := recordengine.New(recordengine.Config{
		Schema:    Schema,
		Backend:   be,
		Prefix:    "p/profiles",
		SideStore: NewSymbolStore(),
	})

	for _, v := range []int64{10, 20} {
		pd := oneSampleBatch(v)
		Project(&pd, func(b *recordengine.Batch) {
			_, err := eng.AppendBatch(b, recordengine.AppendLimits{})
			require.NoError(t, err)
		})
		require.NoError(t, eng.Flush(ctx))
	}

	require.Equal(t, 2, eng.PartCount())
	require.NoError(t, eng.Merge(ctx, 0))
	require.Equal(t, 1, eng.PartCount())

	// After merge the old parts' sidecars are gone; the one remaining stacks sidecar holds the
	// shared stack exactly once.
	keys, err := be.List(ctx, "p/profiles/")
	require.NoError(t, err)

	merged := map[signal.SeriesID][]byte{}
	for _, k := range keys {
		if !strings.HasSuffix(k, "/sym-stacks.bin") {
			continue
		}

		data, err := be.Read(ctx, k)
		require.NoError(t, err)
		require.NoError(t, decodeTable(merged, data))
	}

	require.Len(t, merged, 1, "shared stack stored once after merge-union")
}
