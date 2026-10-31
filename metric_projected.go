package storage

import (
	"context"

	"github.com/go-faster/errors"

	"github.com/oteldb/storage/signal"
	"github.com/oteldb/storage/signal/metric"
)

// ErrMetricProjectionUnsupported requires falling back to WriteMetrics for clustered storage.
var ErrMetricProjectionUnsupported = errors.New("storage: projected metrics require local storage")

// WriteProjectedMetrics ingests trusted, prehashed metric batches without exemplars through normal admission.
// Sources and materialized identities must remain immutable until return; project emits once per metric.
func (s *Storage) WriteProjectedMetrics(ctx context.Context, project func(emit func(*metric.Batch)) int) (acc Accepted, err error) {
	if s.cluster != nil && !s.closed.Load() && !s.opts.ReadOnly {
		return Accepted{}, ErrMetricProjectionUnsupported
	}
	ctx, finish := s.writeSpan(ctx, "storage.write.metrics")
	defer finish(&acc, &err)
	if s.closed.Load() {
		return Accepted{}, errors.Wrap(ErrClosed, "write metrics")
	}
	if s.opts.ReadOnly {
		return Accepted{}, errors.Wrap(ErrReadOnly, "write metrics")
	}
	return s.writeProjectedMetrics(ctx, project)
}

func (s *Storage) writeProjectedMetrics(ctx context.Context, project func(emit func(*metric.Batch)) int) (Accepted, error) {
	buffer := metricAppendPool.Get().(*metricAppendBuffer)
	defer func() {
		buffer.reset()
		metricAppendPool.Put(buffer)
	}()
	writer := metricWriter{store: s, buffer: buffer}
	emitted := project(writer.write)
	if writer.err == nil {
		writer.err = writer.flush()
	}
	if writer.err != nil {
		return Accepted{}, writer.err
	}
	total := writer.rejected.total()
	accepted := int64(emitted) - total
	s.emitAdmission(ctx, signal.Metric, accepted, writer.rejected, writer.sampledDropped, writer.overflowed)
	return Accepted{Accepted: accepted, Rejected: total, RejectedReason: writer.rejected.reason()}, nil
}
