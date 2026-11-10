package engine

import (
	"bytes"
	"encoding/binary"
	"strconv"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/backend"
	"github.com/oteldb/storage/backend/backendtest"
	"github.com/oteldb/storage/internal/seriesindex"
	"github.com/oteldb/storage/query/fetch"
	"github.com/oteldb/storage/readbudget"
	"github.com/oteldb/storage/signal"
)

//nolint:paralleltest,tparallel // Subtests mutate one shared descriptor fault and read counter.
func TestDiskCountSelectsSIDXWithoutDescriptors(t *testing.T) {
	t.Parallel()
	base := backend.Memory()
	counter := backendtest.NewByteCounter(base)
	e := New(Config{Backend: counter, Prefix: "count-ordinals", DiskSeries: true, MetricBlockRows: 64})
	const count = 2048
	var last signal.SeriesID
	var lastHost string
	for i := range count {
		host := strconv.Itoa(i)
		s := diskQuerySeries(signal.StringValue([]byte(host)))
		s.Resource.SchemaURL = bytes.Repeat([]byte("x"), 1024)
		if id := s.Hash(); i == 0 || id.Compare(last) > 0 {
			last, lastHost = id, host
		}
		for _, ts := range []int64{10, 20} {
			ok, err := e.Append(s, ts, float64(i))
			require.NoError(t, err)
			require.True(t, ok)
		}
	}
	require.NoError(t, e.Flush(t.Context()))
	pt := e.parts[0]
	_, err := e.prepareDiskPart(t.Context(), pt)
	require.NoError(t, err)
	key := seriesIndexKey(pt.prefix)
	data, err := base.Read(t.Context(), key)
	require.NoError(t, err)
	// Damage a descriptor-only page while leaving the footer, postings, and SIDX intact.
	dir := int(binary.BigEndian.Uint64(data[len(data)-72+8:]))
	pages := int(binary.BigEndian.Uint32(data[len(data)-72+16:]))
	entry := data[dir+(pages-1)*40:]
	keyOffset := int(binary.BigEndian.Uint64(entry))
	require.Equal(t, byte('s'), data[keyOffset], "fixture must end in descriptor-only pages")
	offset := int(binary.BigEndian.Uint64(entry[16:]))
	packed := int(binary.BigEndian.Uint32(entry[24:]))
	data[offset+packed-1] ^= 1
	require.NoError(t, base.Write(t.Context(), key, data))
	equal := fetch.EqualMatcher{Name: "host", Value: lastHost}
	filtered := fetch.Request{Start: 0, End: 30, Matchers: []fetch.Matcher{{Name: []byte("host"), Match: equal.Predicate(), Spec: &equal}}}
	all := fetch.Request{Start: 0, End: 30}
	job := fetch.EqualMatcher{Name: "job", Value: "api"}
	broad := fetch.Request{Start: 0, End: 30, Matchers: []fetch.Matcher{{Name: []byte("job"), Match: job.Predicate(), Spec: &job}}}
	for _, tc := range []struct {
		name string
		r    fetch.Request
		want int
	}{{"unfiltered", all, count}, {"filtered", filtered, 1}, {"postings broad", broad, count}} {
		t.Run(tc.name, func(t *testing.T) {
			budget := readbudget.New(16 << 20)
			ctx := readbudget.With(t.Context(), budget)
			counter.Reset()
			got, err := e.Count(ctx, tc.r)
			require.NoError(t, err)
			require.Equal(t, tc.want, got)
			if tc.name == "unfiltered" {
				require.NotContains(t, counter.Report(), "/series-index")
				require.LessOrEqual(t, counter.Bytes(), int64(3*count*sidxEntryW+4096))
			}
			grid, err := e.CountGrid(ctx, tc.r, fetch.WindowGrid{Start: 10, Step: 10, Width: 1, NumSteps: 2}, nil)
			require.NoError(t, err)
			require.Equal(t, map[string][]int{"": {tc.want, tc.want}}, grid)
			require.EqualValues(t, 16<<20, budget.Remaining())
			require.Zero(t, pt.refs.Load())
		})
	}
	// Attribute-bearing grouped counts still validate the selected descriptor.
	_, err = e.CountBy(t.Context(), filtered, []byte("host"))
	require.ErrorIs(t, err, seriesindex.ErrCorrupt)
	for _, limit := range []int64{1, 64 << 10} {
		budget := readbudget.New(limit)
		counter.Reset()
		_, err = e.Count(readbudget.With(t.Context(), budget), filtered)
		require.ErrorIs(t, err, readbudget.ErrExceeded)
		require.Equal(t, limit, budget.Remaining())
		require.Zero(t, counter.Reads(), "workspace rejection precedes postings/SIDX payload reads")
		require.Zero(t, pt.refs.Load())
	}
	// A count cannot turn a nominal range into an opaque whole-object fallback.
	require.NotNil(t, pt.index.paged)
	original := pt.index.paged.be
	opaque := &opaqueDiskBackend{Backend: original}
	pt.index.paged.be = opaque
	defer func() { pt.index.paged.be = original }()
	budget := readbudget.New(16 << 20)
	_, err = e.Count(readbudget.With(t.Context(), budget), all)
	require.ErrorIs(t, err, readbudget.ErrExceeded)
	require.Zero(t, opaque.reads)
	require.EqualValues(t, 16<<20, budget.Remaining())
	require.Zero(t, pt.refs.Load())
}
