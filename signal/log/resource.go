package log

import (
	"github.com/go-faster/errors"

	"github.com/oteldb/storage/signal"
)

// Resource reconstructs a record's complete OTLP resource from its stream identity and its
// [ColResource] column value. The column carries the stream's whole resource attribute set —
// including the keys the tenant's stream-field policy excluded from the identity — so a read
// round-trips the resource an embedder ingested regardless of how it was indexed. schema_url is
// always identifying and comes from the stream.
//
// An empty blob means the record predates the resource column (or the projector wrote no
// attributes): the stream identity is then the whole resource.
func Resource(stream signal.Series, blob []byte) (signal.Resource, error) {
	if len(blob) == 0 {
		return stream.Resource, nil
	}

	attrs, _, err := signal.DecodeAttributes(blob)
	if err != nil {
		return signal.Resource{}, errors.Wrap(err, "decode resource attributes")
	}

	return signal.Resource{SchemaURL: stream.Resource.SchemaURL, Attributes: attrs}, nil
}
