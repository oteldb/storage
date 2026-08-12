package identity_test

import (
	"os"
	"testing"

	"github.com/go-faster/sdk/gold"

	"github.com/oteldb/storage/index/identity"
	"github.com/oteldb/storage/index/series"
	"github.com/oteldb/storage/signal"
)

func TestMain(m *testing.M) {
	gold.Init() // registers -update/-clean for the golden wire-format files (see _golden/)

	os.Exit(m.Run())
}

// TestGolden pins the object's bytes. A part's identity object is durable state a reader of an
// older part must still parse, so a change here is a format change: update the golden deliberately,
// and only together with a version bump or a reader that handles both.
func TestGolden(t *testing.T) {
	t.Parallel()

	entries := []series.Entry{mkEntry(1), mkEntry(0)} // unsorted on purpose: output is id-ordered

	gold.Bytes(t, identity.Encode(nil, entries), "identity")

	// An empty set still produces a valid, parseable object (a part with no series cannot occur,
	// but the encoder must not depend on that).
	gold.Bytes(t, identity.Encode(nil, nil), "identity_empty")

	// Every value kind, so a change to the value codec shows up here too.
	s := signal.Series{Attributes: signal.NewAttributes(
		kv("b", signal.BoolValue(true)),
		kv("d", signal.DoubleValue(1.5)),
		kv("i", signal.IntValue(-7)),
		kv("s", str("x")),
	)}
	gold.Bytes(t, identity.Encode(nil, []series.Entry{{ID: s.Hash(), Series: s}}), "identity_values")
}
