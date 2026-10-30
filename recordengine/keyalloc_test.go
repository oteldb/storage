package recordengine

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/signal"
)

func attributeKeyCorpus() [][]byte {
	flat := signal.NewAttributes(signal.KeyValue{Key: []byte("key"), Value: signal.StringValue([]byte("value"))}).AppendHashInput(nil)
	nested := signal.NewAttributes(signal.KeyValue{
		Key: []byte("nested"), Value: signal.MapValue(signal.KeyValue{Key: []byte("child"), Value: signal.IntValue(1)}),
	}).AppendHashInput(nil)
	return [][]byte{nil, {0}, flat, nested, flat[:len(flat)-1], {0xff, 0xff}}
}

func TestAttributeKeyScratch(t *testing.T) {
	t.Parallel()

	for i, blob := range attributeKeyCorpus() {
		t.Run(string(rune('a'+i)), func(t *testing.T) {
			t.Parallel()

			checkAttributeKeyScratch(t, blob)
		})
	}
}

func checkAttributeKeyScratch(t *testing.T, blob []byte) {
	t.Helper()

	var scratch signal.Attributes
	forEachAttrKey(attributeKeyCorpus()[2], &scratch, func([]byte) {})
	var got, want [][]byte
	forEachAttrKey(blob, &scratch, func(key []byte) { got = append(got, key) })
	if attrs, _, err := signal.DecodeAttributes(blob); err == nil {
		for _, attr := range attrs {
			want = append(want, attr.Key)
		}
	}
	if len(blob) == 0 {
		want = nil
	}

	require.Equal(t, want, got)
	require.Empty(t, scratch)
	require.Equal(t, make(signal.Attributes, cap(scratch)), scratch[:cap(scratch)], "scratch holds no previous blob references")
}

func FuzzAttributeKeyScratch(f *testing.F) {
	for _, blob := range attributeKeyCorpus() {
		f.Add(blob)
	}
	f.Fuzz(func(t *testing.T, blob []byte) {
		if len(blob) > 2048 {
			t.Skip()
		}
		checkAttributeKeyScratch(t, blob)
	})
}
