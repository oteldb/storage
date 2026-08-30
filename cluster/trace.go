package cluster

import (
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"

	"github.com/go-faster/errors"
)

// endSpan records a peer RPC's outcome and ends span. [ErrShardAbsent] is normal failover control
// flow (see its doc comment), not a span error: it is surfaced as an attribute instead of
// span.RecordError, so a healthy hedge/failover does not show up as an error in traces.
func endSpan(span trace.Span, err error) {
	if err != nil {
		if errors.Is(err, ErrShardAbsent) {
			span.SetAttributes(attribute.Bool("storage.rpc.absent", true))
		} else {
			span.RecordError(err)
		}
	}

	span.End()
}
