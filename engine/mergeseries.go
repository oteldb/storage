package engine

import (
	"context"

	"github.com/oteldb/storage/internal/buildbudget"
	"github.com/oteldb/storage/internal/memsize"
	"github.com/oteldb/storage/internal/mergestream"
	"github.com/oteldb/storage/signal"
)

type seriesMerge struct {
	keys      mergestream.Keys
	sources   []partSeriesSource
	positions []mergestream.Position
	ranges    []rowRange
}

func (m *seriesMerge) Next() bool {
	for _, pos := range m.positions {
		m.ranges[pos.Source] = rowRange{}
	}
	var ok bool
	m.positions, ok = m.keys.NextPositions(m.positions)
	if !ok {
		return false
	}
	for _, pos := range m.positions {
		m.ranges[pos.Source] = m.sources[pos.Source].RangeAt(pos.Index)
	}
	return true
}

func (m *seriesMerge) Key() signal.SeriesID { return m.keys.Key() }

func (m *seriesMerge) init(ctx context.Context, parts []*part) error {
	m.sources = make([]partSeriesSource, len(parts))
	sources := make([]mergestream.Source, len(parts))
	for i, p := range parts {
		source, err := p.index.idSource(ctx)
		if err != nil {
			return err
		}
		m.sources[i], sources[i] = source, source
	}
	m.positions = make([]mergestream.Position, 0, len(parts))
	m.ranges = make([]rowRange, len(parts))
	m.keys.Reset(sources)
	return nil
}

func mergeRangeBatchBytes(inputs int) int64 {
	return buildbudget.Mul(int64(inputs), 256*memsize.Of[rowRange]())
}
