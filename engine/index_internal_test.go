package engine

import (
	"encoding/binary"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/signal"
)

func TestSeqOfPrefix(t *testing.T) {
	t.Parallel()

	assert.Equal(t, 7, seqOfPrefix("default/metrics/0000000007"))
	assert.Equal(t, 0, seqOfPrefix("p/0000000000"))
	assert.Equal(t, -1, seqOfPrefix("default/metrics/not-a-number"))
}

// legacySeriesSet writes the whole-set identity object exactly as builds before part-scoped
// identity did: a count followed by length-delimited hash-input records. Nothing in the engine
// writes this any more — the fixture stands in for an older writer so the migration path stays
// tested.
func legacySeriesSet(set ...signal.Series) []byte {
	buf := binary.AppendUvarint(nil, uint64(len(set)))

	var enc []byte

	for i := range set {
		enc = set[i].AppendHashInput(enc[:0])
		buf = binary.AppendUvarint(buf, uint64(len(enc)))
		buf = append(buf, enc...)
	}

	return buf
}

func TestDecodeLegacySeriesSet(t *testing.T) {
	t.Parallel()

	set := []signal.Series{
		{Attributes: signal.NewAttributes(signal.KeyValue{Key: []byte("job"), Value: signal.StringValue([]byte("api"))})},
		{Attributes: signal.NewAttributes(signal.KeyValue{Key: []byte("job"), Value: signal.StringValue([]byte("web"))})},
	}

	var got []signal.Series

	require.NoError(t, decodeSeriesSet(legacySeriesSet(set...), func(s signal.Series) {
		got = append(got, s)
	}))

	require.Len(t, got, 2)

	for i, want := range set {
		assert.True(t, want.Equal(got[i]), "legacy identity %d round-trips", i)
	}
}

func TestDecodeSeriesSetRejectsCorrupt(t *testing.T) {
	t.Parallel()

	noop := func(signal.Series) {}
	cases := map[string][]byte{
		"empty":           {},
		"truncated count": {0x80}, // incomplete uvarint
		"missing record":  {2},    // claims 2 records, none follow
		"bad length":      {1, 200},
	}

	for name, data := range cases {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			require.Error(t, decodeSeriesSet(data, noop))
		})
	}
}
