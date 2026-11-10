package engine

import (
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	backendfile "github.com/oteldb/storage/backend/file"
	"github.com/oteldb/storage/query/fetch"
	"github.com/oteldb/storage/signal"
)

func TestDiskSeriesMergedPostingsFetchAndReopen(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name                string
		split, legacy, days bool
		retain              int64
	}{
		{name: "broad"}, {name: "multiple days", days: true}, {name: "split", split: true}, {name: "retention", retain: 115}, {name: "mixed legacy", legacy: true}, {name: "split retention legacy", split: true, legacy: true, retain: 115},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			be, err := backendfile.New(t.TempDir())
			require.NoError(t, err)
			cfg := Config{Backend: be, Prefix: "postings-merge", DiskSeries: true, AggregateStats: true, MetricSeriesChunks: true, MetricBlockRows: 16, FlushMemoryBytes: 128 << 20, MergeMemoryBytes: 128 << 20, MergeCeilingBytes: -1}
			e := New(cfg)
			t.Cleanup(func() { require.NoError(t, e.Close(t.Context())) })
			input := make([]signal.Series, 40)
			for i := range input {
				value := signal.StringValue([]byte("1"))
				if i%2 == 1 {
					value = signal.IntValue(1)
				}
				input[i] = signal.Series{Resource: signal.Resource{Attributes: signal.NewAttributes(signal.KeyValue{Key: []byte("shared"), Value: signal.StringValue([]byte("resource"))})}, Scope: signal.Scope{Name: []byte("scope"), Version: []byte("v1"), Attributes: signal.NewAttributes(signal.KeyValue{Key: []byte("shared"), Value: signal.IntValue(7)})}, Attributes: signal.NewAttributes(signal.KeyValue{Key: []byte("host"), Value: signal.StringValue([]byte(strconv.Itoa(i)))}, signal.KeyValue{Key: []byte("value"), Value: value}, signal.KeyValue{Key: []byte("shared"), Value: signal.BoolValue(true)})}
			}
			for generation := range 4 {
				for i, s := range input {
					if i%7 == 0 && generation >= 2 {
						continue
					}
					accepted, err := e.Append(s, postingFixtureStamp(generation, tc.days), float64(i+generation))
					require.NoError(t, err)
					require.True(t, accepted)
				}
				require.NoError(t, e.Flush(t.Context()))
			}
			require.Len(t, e.parts, 4)
			if tc.legacy {
				p := e.parts[0]
				require.NoError(t, be.Delete(t.Context(), seriesIndexKey(p.prefix)))
				p.metadata = nil
				p.metadataChecked = false
			}
			if tc.split {
				e.cfg.MergeCeilingBytes = 64
			}
			require.NoError(t, e.MergeWith(t.Context(), MergeOptions{Force: true, RetainFrom: tc.retain}))
			if tc.days {
				for range 3 {
					require.NoError(t, e.MergeWith(t.Context(), MergeOptions{Force: true}))
				}
			}
			switch {
			case tc.split:
				require.Greater(t, e.PartCount(), 1)
			case tc.days:
				require.Equal(t, 2, e.PartCount())
			default:
				require.Equal(t, 1, e.PartCount())
			}
			checkMergedPostingRows(t, e, input, tc.retain, tc.days)
			require.NoError(t, e.Close(t.Context()))
			reopened := New(cfg)
			t.Cleanup(func() { require.NoError(t, reopened.Close(t.Context())) })
			require.NoError(t, reopened.LoadParts(t.Context()))
			checkMergedPostingRows(t, reopened, input, tc.retain, tc.days)
			legacyReader := New(Config{Backend: be, Prefix: cfg.Prefix})
			t.Cleanup(func() { require.NoError(t, legacyReader.Close(t.Context())) })
			require.NoError(t, legacyReader.LoadParts(t.Context()))
			checkMergedPostingRows(t, legacyReader, input, tc.retain, tc.days)
		})
	}
}

func checkMergedPostingRows(t *testing.T, e *Engine, input []signal.Series, retain int64, days bool) {
	t.Helper()
	for _, kind := range []signal.ValueKind{signal.KindEmpty, signal.KindStr, signal.KindInt} {
		matcher := fetch.Matcher{Name: []byte("value"), Spec: &fetch.EqualMatcher{Value: "1"}, Match: func(v signal.Value) bool { return v.Kind() == kind }}
		it, err := e.Fetch(t.Context(), fetch.Request{Start: 0, End: 10 * int64(24*time.Hour), Matchers: []fetch.Matcher{matcher}})
		require.NoError(t, err)
		rows, err := fetch.Drain(t.Context(), it)
		require.NoError(t, err)
		require.NoError(t, it.Close())
		if kind == signal.KindEmpty {
			require.Empty(t, rows)
			continue
		}
		expectedCount := 0
		for i := range input {
			if (i%2 == 0) == (kind == signal.KindStr) && (retain <= 110 || i%7 != 0) {
				expectedCount++
			}
		}
		require.Len(t, rows, expectedCount)
		for _, row := range rows {
			host := 0
			for _, kv := range row.Series.Attributes {
				if string(kv.Key) == "host" {
					host, err = strconv.Atoi(string(kv.Value.AppendText(nil)))
					require.NoError(t, err)
				}
			}
			var ts []int64
			var values []float64
			for generation := range 4 {
				stamp := postingFixtureStamp(generation, days)
				if host%7 == 0 && generation >= 2 {
					continue
				}
				if stamp >= retain {
					ts = append(ts, stamp)
					values = append(values, float64(host+generation))
				}
			}
			require.Equal(t, ts, row.Timestamps)
			require.Equal(t, values, row.Values)
		}
	}
	for _, name := range []string{"shared", signal.LabelScopeName, signal.LabelScopeVersion, "absent"} {
		it, err := e.Fetch(t.Context(), fetch.Request{Start: 0, End: 10 * int64(24*time.Hour), Matchers: []fetch.Matcher{{Name: []byte(name), Match: func(v signal.Value) bool { return v.Kind() != signal.KindEmpty }}}})
		require.NoError(t, err)
		rows, err := fetch.Drain(t.Context(), it)
		require.NoError(t, err)
		require.NoError(t, it.Close())
		if name == "absent" {
			require.Empty(t, rows)
		} else {
			expectedCount := len(input)
			if retain > 110 {
				for i := range input {
					if i%7 == 0 {
						expectedCount--
					}
				}
			}
			require.Len(t, rows, expectedCount)
		}
	}
}

func postingFixtureStamp(generation int, days bool) int64 {
	x := int64(100 + generation*10)
	if days {
		x += int64(generation/2) * int64(24*time.Hour)
	}
	return x
}
