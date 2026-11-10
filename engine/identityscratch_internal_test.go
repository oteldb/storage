package engine

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/internal/memsize"
	"github.com/oteldb/storage/signal"
)

func TestReleaseMergeIdentityScratch(t *testing.T) {
	t.Parallel()

	limit := int(mergeIdentityAttributeBytes / memsize.Of[signal.KeyValue]())
	for _, tc := range []struct {
		name     string
		capacity int
		keep     bool
	}{
		{name: "small", capacity: 1, keep: true},
		{name: "within-limit", capacity: limit, keep: true},
		{name: "over-limit", capacity: limit + 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			source := &diskMergeSource{identity: mergedIdentitySeries()}
			source.attrs = source.identity.Attributes
			backing := make([]signal.Attributes, 0, 3)
			for _, attrs := range []*signal.Attributes{&source.identity.Resource.Attributes, &source.identity.Scope.Attributes, &source.identity.Attributes} {
				*attrs = make(signal.Attributes, 1, tc.capacity)
				(*attrs)[0] = source.attrs[0]
				backing = append(backing, *attrs)
			}
			releaseMergeIdentityScratch([]*diskMergeSource{source})
			require.True(t, source.identity.Equal(signal.Series{}))
			require.Nil(t, source.identity.Resource.SchemaURL)
			require.Nil(t, source.identity.Scope.Name)
			require.Nil(t, source.identity.Scope.Version)
			require.Nil(t, source.identity.Scope.SchemaURL)
			for i, attrs := range []signal.Attributes{source.identity.Resource.Attributes, source.identity.Scope.Attributes, source.identity.Attributes} {
				require.Empty(t, attrs)
				require.Equal(t, signal.Attributes{{}}, backing[i])
				if tc.keep {
					require.Equal(t, tc.capacity, cap(attrs))
				} else {
					require.Nil(t, attrs)
				}
			}
			require.Equal(t, mergedIdentitySeries().Attributes.AppendHashInput(nil), source.attrs.AppendHashInput(nil))
		})
	}
}

func TestMergeIdentityItemReleasesScratchAtEOF(t *testing.T) {
	t.Parallel()

	source := &diskMergeSource{identity: mergedIdentitySeries()}
	item := mergeIdentityItem{identity: source.identity}
	e := &Engine{cfg: Config{DiskSeries: true}}
	hasNext, err := item.next(t.Context(), e, &seriesMerge{}, nil, nil, []*diskMergeSource{source})
	require.NoError(t, err)
	require.False(t, hasNext)
	require.Equal(t, signal.Series{}, item.identity)
	require.True(t, source.identity.Equal(signal.Series{}))
	require.Nil(t, source.identity.Resource.SchemaURL)
	require.Nil(t, source.identity.Scope.Name)
}
