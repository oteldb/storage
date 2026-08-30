package router

import (
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"

	"github.com/go-faster/errors"

	"github.com/oteldb/storage/cluster"
)

// endSpan records a hedged/fanned-out read's outcome and ends span. Every owner disclaiming the
// shard ([cluster.ErrShardAbsent]) is normal failover control flow, not a span error — it is
// surfaced as an attribute instead of span.RecordError, matching [cluster]'s own client spans.
func endSpan(span trace.Span, err error) {
	if err != nil {
		if errors.Is(err, cluster.ErrShardAbsent) {
			span.SetAttributes(attribute.Bool("storage.rpc.absent", true))
		} else {
			span.RecordError(err)
		}
	}

	span.End()
}
