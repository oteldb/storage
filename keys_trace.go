package storage

import (
	"github.com/go-faster/errors"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"

	"github.com/oteldb/storage/cluster"
)

func recordKeysError(span trace.Span, err error) {
	if errors.Is(err, cluster.ErrShardAbsent) && !errors.Is(err, cluster.ErrShardIncomplete) {
		span.SetAttributes(attribute.Bool("storage.rpc.absent", true))
	} else if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
	}
}
