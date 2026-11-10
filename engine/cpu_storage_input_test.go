package engine

import (
	"context"
	"math"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/backend"
)

func TestCPUStorageInput(t *testing.T) {
	t.Parallel()
	for _, points := range []int{1, 3, 64} {
		options := cpuFixtureOptions{hosts: 5, points: points}
		seed := newCPUStorageInput(options, 0)
		next := seed.generation(1)
		require.Len(t, seed.identities, 5*len(cpuFixtureModes))
		require.Len(t, seed.ids, len(seed.identities)*points)
		require.Len(t, seed.ts, len(seed.ids))
		require.Len(t, seed.values, len(seed.ids))
		for i := range seed.identities {
			id := cpuMetricSeries(i).Hash()
			for point := range points {
				j := i*points + point
				require.Equal(t, id, seed.ids[j])
				require.Equal(t, id, next.ids[j])
				require.Equal(t, cpuFixtureBase+int64(point)*cpuFixtureCadence, seed.ts[j])
				require.Equal(t, seed.ts[j]+int64(points)*cpuFixtureCadence, next.ts[j])
				require.Equal(t, math.Float64bits(float64(i+point)/1000), math.Float64bits(seed.values[j]))
				require.Equal(t, math.Float64bits(float64(i+points+point)/1000), math.Float64bits(next.values[j]))
			}
		}
	}
}

func TestCPUStoragePhases(t *testing.T) {
	t.Parallel()
	for _, phase := range []cpuStoragePhase{cpuAppendCold, cpuAppendHeadWarm, cpuAppendAfterFlush, cpuFlush} {
		t.Run(string(phase), func(t *testing.T) {
			t.Parallel()
			options := cpuFixtureOptions{hosts: 2, points: 3, disk: true}
			seed := newCPUStorageInput(options, 0)
			next := seed.generation(1)
			f := &cpuFixture{engine: New(Config{Backend: backend.Memory(), DiskSeries: true}), options: options, root: t.TempDir()}
			t.Cleanup(func() {
				if f.engine != nil {
					require.NoError(t, f.engine.Close(context.WithoutCancel(t.Context())))
				}
			})
			require.NoError(t, phase.prepare(t.Context(), f, seed))
			if phase == cpuAppendCold {
				require.Zero(t, f.engine.Stats().Series)
			} else {
				require.EqualValues(t, len(seed.identities), f.engine.Stats().Series)
			}
			if phase == cpuAppendHeadWarm || phase == cpuFlush {
				require.EqualValues(t, len(seed.ids), f.engine.Stats().HeadSamples)
				require.Equal(t, len(seed.identities), f.engine.head.series.Len())
			}
			if phase == cpuAppendAfterFlush {
				require.Zero(t, f.engine.head.series.Len())
				require.Zero(t, f.engine.Stats().HeadSamples)
				require.Positive(t, f.engine.PartCount())
			}
			input := next
			if phase == cpuAppendCold {
				input = seed
			}
			require.NoError(t, phase.execute(t.Context(), f, input))
			require.EqualValues(t, len(seed.identities), f.engine.Stats().Series)
			f.options.generations = 2
			if phase == cpuAppendCold || phase == cpuFlush {
				f.options.generations = 1
			}
			require.NoError(t, f.fetchHost(t.Context(), 0))
			require.NoError(t, f.fetchHost(t.Context(), 1))
			if phase == cpuFlush {
				require.Zero(t, f.engine.Stats().HeadSamples)
			}
			require.NoError(t, f.remove(t.Context()))
			require.NoDirExists(t, f.root)
		})
	}
}

func TestNewCPUFixtureCleanup(t *testing.T) {
	t.Parallel()
	f := newCPUFixture(t, t.Context(), cpuFixtureOptions{hosts: 1, points: 1, disk: true})
	require.NotNil(t, f.counter)
	require.NotNil(t, f.engine.cfg.WAL)
	require.NoError(t, f.remove(t.Context()))
	require.NoDirExists(t, f.root)
}

func TestCPUStorageInputPartialBatch(t *testing.T) {
	t.Parallel()
	options := cpuFixtureOptions{hosts: 173, points: 3, generations: 1}
	input := newCPUStorageInput(options, 0)
	f := &cpuFixture{engine: New(Config{Backend: backend.Memory(), DiskSeries: true}), options: options}
	t.Cleanup(func() { require.NoError(t, f.engine.Close(context.WithoutCancel(t.Context()))) })
	require.NoError(t, input.appendTo(f.engine))
	require.EqualValues(t, len(input.identities), f.engine.Stats().Series)
	require.EqualValues(t, len(input.ids), f.engine.Stats().HeadSamples)
	require.NoError(t, f.fetchHost(t.Context(), 0))
	require.NoError(t, f.fetchHost(t.Context(), options.hosts-1))
}
