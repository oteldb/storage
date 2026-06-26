package pdataconv

import (
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/pprofile"

	"github.com/oteldb/storage/signal"
	"github.com/oteldb/storage/signal/profile"
)

// AppendProfiles converts an OTLP profiles batch into dst, reusing dst's retained capacity (call
// [profile.Profiles.Reset] or use [profile.GetProfiles] for a recycled batch). The OTLP shared
// ProfilesDictionary maps one-to-one onto [profile.Dictionary], so its tables are copied
// index-preserving (samples/stacks/locations keep referencing the same indices). Everything is
// representable, so dropped is always 0; it is returned for symmetry with [AppendMetrics].
//
//nolint:dupl // per-signal OTLP converter; identical resource/scope walk, types differ
func AppendProfiles(dst *profile.Profiles, pd pprofile.Profiles) (dropped int) {
	convertDictionary(&dst.Dictionary, pd.Dictionary())

	rps := pd.ResourceProfiles()
	for i := range rps.Len() {
		srp := rps.At(i)

		rp := dst.AddResource()
		rp.Resource = signal.Resource{
			SchemaURL:  []byte(srp.SchemaUrl()),
			Attributes: convertMap(srp.Resource().Attributes()),
		}

		sps := srp.ScopeProfiles()
		for j := range sps.Len() {
			ssp := sps.At(j)

			sp := rp.AddScope()
			sp.Scope = signal.Scope{
				Name:       []byte(ssp.Scope().Name()),
				Version:    []byte(ssp.Scope().Version()),
				SchemaURL:  []byte(ssp.SchemaUrl()),
				Attributes: convertMap(ssp.Scope().Attributes()),
			}

			profiles := ssp.Profiles()
			for k := range profiles.Len() {
				appendProfile(sp, profiles.At(k))
			}
		}
	}

	return dropped
}

// convertDictionary copies the OTLP shared dictionary into dst index-preserving. Strings are
// appended directly (not interned) so the table indices that samples/stacks/locations reference
// survive unchanged; OTLP guarantees Strings[0] == "" just as the internal model expects.
func convertDictionary(dst *profile.Dictionary, src pprofile.ProfilesDictionary) {
	strs := src.StringTable()
	for i := range strs.Len() {
		dst.Strings = append(dst.Strings, []byte(strs.At(i)))
	}

	fns := src.FunctionTable()
	for i := range fns.Len() {
		fn := fns.At(i)
		dst.AddFunction(profile.Function{
			NameStrindex:       fn.NameStrindex(),
			SystemNameStrindex: fn.SystemNameStrindex(),
			FilenameStrindex:   fn.FilenameStrindex(),
			StartLine:          fn.StartLine(),
		})
	}

	mps := src.MappingTable()
	for i := range mps.Len() {
		mp := mps.At(i)
		dst.AddMapping(profile.Mapping{
			AttributeIndices: int32sCopy(mp.AttributeIndices()),
			MemoryStart:      mp.MemoryStart(),
			MemoryLimit:      mp.MemoryLimit(),
			FileOffset:       mp.FileOffset(),
			FilenameStrindex: mp.FilenameStrindex(),
		})
	}

	locs := src.LocationTable()
	for i := range locs.Len() {
		loc := locs.At(i)

		lines := loc.Lines()
		ls := make([]profile.Line, 0, lines.Len())
		for j := range lines.Len() {
			ln := lines.At(j)
			ls = append(ls, profile.Line{
				FunctionIndex: ln.FunctionIndex(),
				Line:          ln.Line(),
				Column:        ln.Column(),
			})
		}

		dst.AddLocation(profile.Location{
			Lines:            ls,
			AttributeIndices: int32sCopy(loc.AttributeIndices()),
			Address:          loc.Address(),
			MappingIndex:     loc.MappingIndex(),
		})
	}

	stacks := src.StackTable()
	for i := range stacks.Len() {
		dst.AddStack(int32sCopy(stacks.At(i).LocationIndices())...)
	}

	attrs := src.AttributeTable()
	for i := range attrs.Len() {
		attr := attrs.At(i)
		dst.AddAttribute(profile.KeyValueAndUnit{
			Value:        convertValue(attr.Value()),
			KeyStrindex:  attr.KeyStrindex(),
			UnitStrindex: attr.UnitStrindex(),
		})
	}

	links := src.LinkTable()
	for i := range links.Len() {
		ln := links.At(i)
		dst.AddLink(profile.Link{
			TraceID: traceIDBytes(ln.TraceID()),
			SpanID:  spanIDBytes(ln.SpanID()),
		})
	}
}

func appendProfile(sp *profile.ScopeProfiles, p pprofile.Profile) {
	pr := sp.AddProfile()
	pr.SampleType = convertValueType(p.SampleType())
	pr.PeriodType = convertValueType(p.PeriodType())
	pr.TimeNanos = int64(p.Time())
	pr.DurationNanos = int64(p.DurationNano())
	pr.Period = p.Period()
	pr.ProfileID = profileIDBytes(p.ProfileID())
	pr.AttributeIndices = int32sCopy(p.AttributeIndices())
	pr.Dropped = p.DroppedAttributesCount()

	samples := p.Samples()
	for i := range samples.Len() {
		s := samples.At(i)

		out := pr.AddSample()
		out.StackIndex = s.StackIndex()
		out.Values = append([]int64(nil), s.Values().AsRaw()...)
		out.TimestampsUnixNano = append([]uint64(nil), s.TimestampsUnixNano().AsRaw()...)
		out.AttributeIndices = int32sCopy(s.AttributeIndices())
		out.LinkIndex = s.LinkIndex()
	}
}

func convertValueType(vt pprofile.ValueType) profile.ValueType {
	return profile.ValueType{
		TypeStrindex: vt.TypeStrindex(),
		UnitStrindex: vt.UnitStrindex(),
	}
}

// int32sCopy copies an OTLP int32 index slice into an owned slice (nil when empty).
func int32sCopy(s pcommon.Int32Slice) []int32 {
	if s.Len() == 0 {
		return nil
	}

	return append([]int32(nil), s.AsRaw()...)
}

// profileIDBytes copies a 16-byte OTLP profile id into an owned slice, returning nil when unset.
func profileIDBytes(id pprofile.ProfileID) []byte {
	if id.IsEmpty() {
		return nil
	}

	return append([]byte(nil), id[:]...)
}
