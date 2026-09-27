package profile

import (
	"math/rand/v2"
	"strconv"
	"testing"

	"github.com/oteldb/storage/recordengine"
	"github.com/oteldb/storage/signal"
)

const (
	heapSamples   = 20_000
	heapZeroInuse = 0.987
)

var heapTypes = []struct {
	name, unit string
	inuse      bool
}{
	{"alloc_objects", "count", false},
	{"alloc_space", "bytes", false},
	{"inuse_objects", "count", true},
	{"inuse_space", "bytes", true},
}

// heapScrape is one pprofreceiver scrape of a Go heap profile: one profile per sample type over a
// shared set of stacks, the empty pprof.profile.* attributes on every profile, and inuse_* values
// that are almost all zero.
func heapScrape(tb testing.TB, samples int) *Profiles {
	tb.Helper()

	rng := rand.New(rand.NewPCG(696, 1))

	pd := &Profiles{}
	d := &pd.Dictionary

	fns := make([]int32, 2000)
	for i := range fns {
		name := d.InternString([]byte("pkg.fn" + strconv.Itoa(i)))
		file := d.InternString([]byte("pkg/file" + strconv.Itoa(i%200) + ".go"))
		fn := d.AddFunction(Function{NameStrindex: name, SystemNameStrindex: name, FilenameStrindex: file})
		fns[i] = d.AddLocation(Location{Address: uint64(0x400000 + i*16), Lines: []Line{{FunctionIndex: fn, Line: int64(i % 500)}}})
	}

	stacks := make([]int32, samples)
	for i := range stacks {
		locs := make([]int32, 4+rng.IntN(12))
		for j := range locs {
			locs[j] = fns[rng.IntN(len(fns))]
		}

		stacks[i] = d.AddStack(locs...)
	}

	attrs := make([]int32, 0, 4)
	for _, kv := range []KeyValueAndUnit{
		{KeyStrindex: d.InternString([]byte("pprof.profile.comment")), Value: signal.SliceValue()},
		{KeyStrindex: d.InternString([]byte("pprof.profile.doc_url")), Value: signal.StringValue(nil)},
		{KeyStrindex: d.InternString([]byte("pprof.profile.drop_frames")), Value: signal.StringValue(nil)},
		{KeyStrindex: d.InternString([]byte("pprof.profile.keep_frames")), Value: signal.StringValue(nil)},
	} {
		attrs = append(attrs, d.AddAttribute(kv))
	}

	rp := pd.AddResource()
	rp.Resource = svcResource("heap")
	sp := rp.AddScope()

	space, bytes := d.InternString([]byte("space")), d.InternString([]byte("bytes"))

	for _, typ := range heapTypes {
		pr := sp.AddProfile()
		pr.SampleType = ValueType{TypeStrindex: d.InternString([]byte(typ.name)), UnitStrindex: d.InternString([]byte(typ.unit))}
		pr.PeriodType = ValueType{TypeStrindex: space, UnitStrindex: bytes}
		pr.Period = 512 * 1024
		pr.TimeNanos = 1_700_000_000_000_000_000
		pr.ProfileID = []byte("0123456789abcdef")
		pr.AttributeIndices = attrs

		for _, st := range stacks {
			v := 1 + rng.Int64N(1<<20)
			if typ.inuse && rng.Float64() < heapZeroInuse {
				v = 0
			}

			s := pr.AddSample()
			s.StackIndex = st
			s.Values = []int64{v}
		}
	}

	return pd
}

type footprint struct {
	profiles, rows, bytes, attrs int64
}

func (f footprint) rowsPerProfile() float64 { return float64(f.rows) / float64(f.profiles) }
func (f footprint) bytesPerRow() float64    { return float64(f.bytes) / float64(f.rows) }
func (f footprint) attrsPerRow() float64    { return float64(f.attrs) / float64(f.rows) }

// measure projects pd and returns the decoded footprint of the rows it emits: [recordengine.Batch.ByteSize]
// counts the same bytes a part's manifest records as its decoded size.
func measure(pd *Profiles) footprint {
	var f footprint

	for _, rp := range pd.Resources {
		for _, sp := range rp.Scopes {
			f.profiles += int64(len(sp.Profiles))
		}
	}

	Project(pd, func(b *recordengine.Batch) {
		f.rows += int64(b.Len())
		f.bytes += b.ByteSize()

		for _, v := range b.Bytes[bAttrs] {
			f.attrs += int64(len(v))
		}
	})

	return f
}

func BenchmarkProjectHeap(b *testing.B) {
	pd := heapScrape(b, heapSamples)
	f := measure(pd)

	b.SetBytes(f.bytes)
	b.ReportAllocs()

	for b.Loop() {
		Project(pd, func(*recordengine.Batch) {})
	}

	b.ReportMetric(f.rowsPerProfile(), "rows/profile")
	b.ReportMetric(f.bytesPerRow(), "B/row")
	b.ReportMetric(f.attrsPerRow(), "attrsB/row")
}
