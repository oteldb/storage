package seriesindex

import (
	"slices"
	"strconv"
	"testing"

	"github.com/oteldb/storage/signal"
)

func BenchmarkBuilderRepeatedLabels(b *testing.B) {
	const cardinality = 4096
	identities := make([]signal.Series, cardinality)
	for i := range identities {
		attrs := make([]signal.KeyValue, 0, 16)
		attrs = append(attrs, signal.KeyValue{Key: []byte("instance"), Value: signal.StringValue([]byte(strconv.Itoa(i)))})
		for label := range 15 {
			attrs = append(attrs, signal.KeyValue{Key: []byte("label_" + strconv.Itoa(label)), Value: signal.StringValue([]byte("production-eu-central"))})
		}
		identities[i] = signal.Series{Attributes: signal.NewAttributes(attrs...)}
	}
	slices.SortFunc(identities, func(a, b signal.Series) int { return a.Hash().Compare(b.Hash()) })
	ids := make([]signal.SeriesID, len(identities))
	for i := range identities {
		ids[i] = identities[i].Hash()
	}
	b.ReportAllocs()
	for b.Loop() {
		var builder Builder
		for i, s := range identities { //nolint:gocritic // Keep the caller identical to the measured baseline.
			if err := builder.Add(ids[i], s, Metadata{Covered: 100}); err != nil {
				b.Fatal(err)
			}
		}
		if builder.Count() != cardinality {
			b.Fatal("incomplete builder")
		}
	}
}

func BenchmarkBuilderRepeatedLabelHit(b *testing.B) {
	name := []byte("deployment.environment")
	value := signal.StringValue([]byte("production-eu-central"))
	s := signal.Series{Attributes: signal.NewAttributes(signal.KeyValue{Key: name, Value: value})}
	var builder Builder
	if err := builder.Add(s.Hash(), s, Metadata{Covered: 100}); err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	for b.Loop() {
		builder.addLabel(0, name, value)
	}
}
