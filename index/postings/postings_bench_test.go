package postings

import (
	"testing"

	"github.com/oteldb/storage/signal"
)

// BenchmarkPostingsBuild measures the cost of building the inverted index for a steady-workload-shaped
// corpus (~100 hosts × N label combinations, several labels per series). This is the
// index/postings.(*MemPostings).Add live-heap line item (issue #25): every Add appends a 16-byte
// SeriesID to a sorted slice per (name,value) bucket AND to the all-set, so a series with L labels
// costs ~L×16 bytes of id storage plus slice/sort overhead.
func BenchmarkPostingsBuild(b *testing.B) {
	for _, hosts := range []int{10, 100} {
		const seriesPerHost = 1400

		n := hosts * seriesPerHost
		// Pre-build the (series, nameID, valueID) triples once; the benchmark measures Add only.
		type triple struct {
			id              signal.SeriesID
			nameID, valueID uint32
		}

		const labels = 4 // __name__, host, cpu, mode
		triples := make([]triple, 0, n*labels)

		nameName, hostName, cpuName, modeName := uint32(1), uint32(2), uint32(3), uint32(4)

		for i := range n {
			id := signal.SeriesID{Hi: uint64(i) >> 1, Lo: uint64(i)*0x9e3779b97f4a7c15 | 1}
			hostVal := uint32(100 + i%hosts)
			cpuVal := uint32(200 + i%8)
			modeVal := uint32(300 + i%4)

			triples = append(triples,
				triple{id, nameName, uint32(50)}, // metric name
				triple{id, hostName, hostVal},    // host
				triple{id, cpuName, cpuVal},      // cpu
				triple{id, modeName, modeVal},    // mode
			)
		}

		b.Run(hostsLabel(hosts), func(b *testing.B) {
			b.ReportAllocs()

			for range b.N {
				p := NewMemPostings()
				for _, t := range triples {
					p.Add(t.id, t.nameID, t.valueID)
				}

				p.EnsureSorted() // the first read's dedup cost
			}
		})
	}
}

// BenchmarkPostingsIntersect measures resolving a typical 2-matcher query (host=X AND cpu=cpu0) — the
// Intersect merge-join over sorted []SeriesID slices that a label query pays per fetch.
func BenchmarkPostingsIntersect(b *testing.B) {
	const hosts = 100
	const seriesPerHost = 1400
	n := hosts * seriesPerHost

	p := NewMemPostings()
	nameName, hostName, cpuName := uint32(1), uint32(2), uint32(3)

	for i := range n {
		id := signal.SeriesID{Hi: uint64(i) >> 1, Lo: uint64(i)*0x9e3779b97f4a7c15 | 1}
		p.Add(id, nameName, uint32(50))
		p.Add(id, hostName, uint32(100+i%hosts)) // host bucket: ~1400 series each
		p.Add(id, cpuName, uint32(200+i%8))      // cpu bucket: ~17500 series each
	}

	p.EnsureSorted()

	// host=host0 (∩) cpu=cpu0 → ~175 series.
	host0 := p.Get(hostName, uint32(100))
	cpu0 := p.Get(cpuName, uint32(200))

	b.Run("2matcher", func(b *testing.B) {
		b.ReportAllocs()

		var sink int

		for range b.N {
			it := Intersect(host0, cpu0)
			for it.Next() {
				sink++
			}
		}

		_ = sink
	})
}

// BenchmarkPostingsMergeHighCardinality measures the union path a high-cardinality matcher pays —
// e.g. `__name__=~"node_.+"` resolving to every series across ~1300 distinct metric-name buckets.
// MemPostings.ForName/Select compose those buckets with Merge; the k-way merge's per-element cost is
// the lever (a linear min-scan is O(N×buckets); a heap is O(N×log buckets)).
func BenchmarkPostingsMergeHighCardinality(b *testing.B) {
	for _, buckets := range []int{64, 1300} {
		const seriesPerBucket = 110

		n := buckets * seriesPerBucket

		p := NewMemPostings()
		nameName := uint32(1)

		for i := range n {
			id := signal.SeriesID{Hi: uint64(i) >> 1, Lo: uint64(i)*0x9e3779b97f4a7c15 | 1}
			p.Add(id, nameName, uint32(1000+i%buckets)) // one bucket per distinct metric name
		}

		p.EnsureSorted()

		b.Run(bucketsLabel(buckets), func(b *testing.B) {
			b.ReportAllocs()

			var sink int

			for range b.N {
				it := p.ForName(nameName) // union of every value bucket
				for it.Next() {
					sink++
				}
			}

			if sink/b.N != n {
				b.Fatalf("union size = %d, want %d", sink/b.N, n)
			}
		})
	}
}

func bucketsLabel(n int) string {
	switch n {
	case 1300:
		return "1300buckets"
	default:
		return "64buckets"
	}
}

func hostsLabel(h int) string {
	switch h {
	case 100:
		return "100h"
	case 10:
		return "10h"
	default:
		return "h"
	}
}
