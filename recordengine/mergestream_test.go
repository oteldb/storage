package recordengine_test

import (
	"context"
	"strconv"
	"testing"

	"github.com/go-faster/errors"

	"github.com/oteldb/storage/internal/mergestream/mergestreamtest"
	"github.com/oteldb/storage/query/fetch"
	"github.com/oteldb/storage/recordengine"
	"github.com/oteldb/storage/signal"
)

// recordStore instantiates the shared merge conformance suite over the record engine.
type recordStore struct {
	e      *recordengine.Engine
	stream map[signal.SeriesID]int
}

func (s *recordStore) Ingest(ctx context.Context, rows []mergestreamtest.Row) error {
	byStream := map[int][]rrec{}
	for _, r := range rows {
		byStream[r.Stream] = append(byStream[r.Stream], rrec{ts: r.Ts, sev: r.Val})
	}

	for stream, recs := range byStream {
		b := mkBatch("s"+strconv.Itoa(stream), recs...)
		s.stream[b.Stream] = stream

		if _, err := s.e.AppendBatch(b, recordengine.AppendLimits{}); err != nil {
			return err
		}
	}

	return s.e.Flush(ctx)
}

func (s *recordStore) Merge(ctx context.Context) error { return s.e.Merge(ctx, 0) }

func (s *recordStore) Rows(ctx context.Context) ([]mergestreamtest.Row, error) {
	it, err := s.e.Fetch(ctx, fetch.Request{Signal: signal.Log, Start: 0, End: 1 << 60})
	if err != nil {
		return nil, err
	}

	batches, err := fetch.Drain(ctx, it)
	if err != nil {
		return nil, err
	}

	var out []mergestreamtest.Row

	for _, b := range batches {
		sev, ok := b.Column("sev")
		if !ok {
			return nil, errors.New("batch has no severity column")
		}

		for i, ts := range b.Timestamps {
			out = append(out, mergestreamtest.Row{Stream: s.stream[b.ID], Ts: ts, Val: sev.Int64[i]})
		}
	}

	return out, nil
}

func (s *recordStore) Parts() []string { return s.e.PartPrefixes() }

// Want concatenates: records are append-only, so a merge dedups nothing.
func (s *recordStore) Want(src [][]mergestreamtest.Row) []mergestreamtest.Row {
	var out []mergestreamtest.Row
	for _, rows := range src {
		out = append(out, rows...)
	}

	return out
}

func TestMergeStreamConformance(t *testing.T) {
	t.Parallel()

	mergestreamtest.Run(t, func(t *testing.T, be *mergestreamtest.Backend) mergestreamtest.Store {
		t.Helper()

		return &recordStore{e: newEngine(t, be), stream: map[signal.SeriesID]int{}}
	})
}
