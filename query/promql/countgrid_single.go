package promql

import (
	"context"
	"math"

	"github.com/go-faster/errors"

	"github.com/oteldb/storage/query/fetch"
)

func (q *querier) countSingleGrid(ctx context.Context, r fetch.Request, group []byte) (map[string]uint64, error) {
	counter := fetch.GridCounterOf(q.fetcher)
	if counter == nil {
		return nil, fetch.ErrCountGridUnsupported
	}
	if r.Start > r.End {
		return map[string]uint64{}, nil
	}
	// Unsigned subtraction also detects windows spanning more than MaxInt64 nanoseconds.
	width := uint64(r.End) - uint64(r.Start) + 1
	if width == 0 || width > math.MaxInt64 {
		return nil, fetch.ErrCountGridUnsupported
	}
	if _, hidden := hiddenLabels[string(group)]; group != nil && hidden {
		return nil, fetch.ErrCountGridUnsupported
	}
	counts, err := counter.CountGrid(ctx, r, fetch.WindowGrid{Start: r.End, Step: 1, Width: int64(width), NumSteps: 1}, group)
	if err != nil {
		return nil, err
	}
	out := make(map[string]uint64, len(counts))
	for name, values := range counts {
		if len(values) != 1 || values[0] < 0 {
			return nil, errors.New("single count grid returned invalid counts")
		}
		if group == nil && name != "" {
			return nil, errors.New("single count grid returned an unexpected group")
		}
		if values[0] != 0 {
			out[name] = uint64(values[0])
		}
	}
	return out, nil
}
