package series

import (
	"fmt"
	"strconv"
	"testing"

	"github.com/oteldb/storage/signal"
)

// BenchmarkIndexAdd measures the cost of registering series identities. Today the index interns
// identities through a shared symbol table (one owned copy per distinct string, referenced by every
// series) instead of deep-cloning every key/value per series. Under a steady metrics workload the
// same resource/scope and label values repeat across many series (issue #25 cites ~100 hosts ×
// ~140k series), so the corpus models that high-duplication shape: a small set of hosts/metrics/
// cpus/modes combined into many distinct series. This is the signal.Attributes.Clone +
// slices.Clone[[]uint8] live-heap line item.
func BenchmarkIndexAdd(b *testing.B) {
	for _, n := range []int{1_000, 10_000} {
		// Mixed-radix labels so the n series are all distinct yet built from a small string pool
		// (the steady-workload shape: ~100 hosts, a handful of metrics, each shared across many
		// series). host×seq is injective over [0,n); the distinct-string count is ~100+n/100.
		corpus := make([]signal.Series, n)
		for i := range n {
			corpus[i] = signal.Series{
				Resource: signal.Resource{Attributes: attrs(
					"service.name", "node-exporter",
					"host", fmt.Sprintf("host%d", i%100),
				)},
				Scope: signal.Scope{Name: []byte("lib"), Version: []byte("1.0")},
				Attributes: attrs(
					"__name__", "node_cpu_seconds_total",
					"cpu", fmt.Sprintf("cpu%d", i%8),
					"seq", fmt.Sprintf("s%d", i/100),
				),
			}
		}

		b.Run(scaleLabel(n), func(b *testing.B) {
			b.ReportAllocs()

			for range b.N {
				ix := New()
				for i := range corpus {
					ix.Add(corpus[i])
				}
			}
		})
	}
}

func scaleLabel(n int) string {
	switch {
	case n >= 1_000_000:
		return "1M"
	case n >= 1_000:
		return strconv.Itoa(n/1000) + "k"
	default:
		return strconv.Itoa(n)
	}
}
