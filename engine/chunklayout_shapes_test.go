package engine_test

import (
	"math"
	"strconv"
	"testing"

	"github.com/stretchr/testify/require"

	backendfile "github.com/oteldb/storage/backend/file"
	"github.com/oteldb/storage/engine"
	"github.com/oteldb/storage/query/fetch"
)

func TestSeriesChunkShapes(t *testing.T) {
	t.Parallel()
	for _, cache := range []int64{0, 1 << 20} {
		for _, constantTs := range []bool{false, true} {
			for _, constantValues := range []bool{false, true} {
				name := strconv.FormatInt(cache, 10) + "/ts" + strconv.FormatBool(constantTs) + "/v" + strconv.FormatBool(constantValues)
				t.Run(name, func(t *testing.T) {
					t.Parallel()
					files, err := backendfile.New(t.TempDir())
					require.NoError(t, err)
					e := engine.New(engine.Config{Backend: files, Prefix: "shapes", MetricSeriesChunks: true, MetricBlockRows: 32, DecodeCacheBytes: cache})
					for s, n := range []int{1, 100, 5, 64} {
						for i := range n {
							ts := int64(i + 1)
							if constantTs {
								ts = 10
							}
							value := float64(s*100 + i)
							if constantValues {
								value = math.Copysign(0, -1)
							}
							mustAppend(t, e, mkSeries("instance", "host-"+strconv.Itoa(s)), ts, value)
						}
					}
					require.NoError(t, e.Flush(t.Context()))
					for repeat := range 2 {
						for s, n := range []int{1, 100, 5, 64} {
							req := fetch.Request{Start: 0, End: 1000, Matchers: []fetch.Matcher{eqMatcher("instance", "host-"+strconv.Itoa(s))}}
							got := fetchAll(t, e, req)
							require.Len(t, got, 1)
							wantN := n
							if constantTs {
								wantN = 1
							}
							require.Len(t, got[0].Values, wantN, "repeat %d", repeat)
							for i, v := range got[0].Values {
								position := i
								if constantTs {
									position = n - 1
								}
								want := float64(s*100 + position)
								if constantValues {
									want = math.Copysign(0, -1)
								}
								require.Equal(t, math.Float64bits(want), math.Float64bits(v))
							}
							count, err := e.Count(t.Context(), req)
							require.NoError(t, err)
							require.Equal(t, 1, count)
						}
					}
				})
			}
		}
	}
}
