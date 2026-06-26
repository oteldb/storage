package profile

import (
	"github.com/oteldb/storage/encoding/chunk"
	"github.com/oteldb/storage/recordengine"
	"github.com/oteldb/storage/signal"
)

// Reserved stream-label keys carrying the profile type. The type (sample/period type+unit) is folded
// into the stream identity — like a metric's __name__ — so a query selects a profile type with an
// ordinary label matcher and the available types enumerate through the postings index. The values
// are the raw OTLP dictionary strings; an embedder assembles them (and derives a Pyroscope-style
// profile name) into its own profile-type representation.
var (
	LabelSampleType = []byte("otel.profile.sample_type")
	LabelSampleUnit = []byte("otel.profile.sample_unit")
	LabelPeriodType = []byte("otel.profile.period_type")
	LabelPeriodUnit = []byte("otel.profile.period_unit")
)

// Column names of the profiles sample schema. The sample timestamp is the implicit primary
// timestamp / sort key; the stream id is the Resource+Scope+type hash. stack_id is a content-
// addressed reference into the symbol store (resolved via [Resolver]).
const (
	ColValue     = "value"
	ColPeriod    = "period"     // profile period value (denormalized onto each sample)
	ColDuration  = "duration"   // profile duration_nanos (denormalized)
	ColStackID   = "stack_id"   // 16-byte content id into the symbol-store stack table
	ColProfileID = "profile_id" // 16-byte OTLP profile id
	ColTraceID   = "trace_id"   // linked span's trace id (16 bytes) or empty
	ColSpanID    = "span_id"    // linked span's span id (8 bytes) or empty
	ColAttrs     = "attrs"      // serialized per-sample attributes (profile ∪ sample)
)

// Schema is the profiles vertical's record-engine column schema: one row per sample observation.
// The profile type lives in the stream identity (see the reserved labels above), not a column.
// profile_id carries an equality bloom (profile-by-id pruning, future); attrs the attribute bloom.
var Schema = recordengine.NewSchema(
	recordengine.Column{Name: ColValue, Kind: recordengine.KindInt64, Codec: chunk.CodecT64},
	recordengine.Column{Name: ColPeriod, Kind: recordengine.KindInt64, Codec: chunk.CodecT64},
	recordengine.Column{Name: ColDuration, Kind: recordengine.KindInt64, Codec: chunk.CodecT64},
	recordengine.Column{Name: ColStackID, Kind: recordengine.KindBytes, Codec: chunk.CodecDict},
	recordengine.Column{Name: ColProfileID, Kind: recordengine.KindBytes, Codec: chunk.CodecDict, Bloom: recordengine.BloomEquality},
	recordengine.Column{Name: ColTraceID, Kind: recordengine.KindBytes, Codec: chunk.CodecDict},
	recordengine.Column{Name: ColSpanID, Kind: recordengine.KindBytes, Codec: chunk.CodecDict},
	recordengine.Column{Name: ColAttrs, Kind: recordengine.KindBytes, Codec: chunk.CodecDict, Bloom: recordengine.BloomAttrs},
)

// int/byte column indices in the declaration order above.
const (
	iValue = iota
	iPeriod
	iDuration
)

const (
	bStackID = iota
	bProfileID
	bTraceID
	bSpanID
	bAttrs
)

// profileType is one profile's resolved (sample/period type+unit) strings — the stream-identity key.
type profileType struct {
	sampleType, sampleUnit, periodType, periodUnit []byte
}

func (t profileType) key() string {
	return string(t.sampleType) + "\x00" + string(t.sampleUnit) + "\x00" + string(t.periodType) + "\x00" + string(t.periodUnit)
}

// Project iterates a [Profiles] batch and calls emit once per stream — each (Resource, Scope,
// profile-type) group — with a [recordengine.Batch] of that group's sample rows in the profiles
// [Schema]'s column order, plus the content-addressed symbol delta (Batch.Side) the rows reference.
// It returns the number of rows emitted.
//
// A scope's profiles are grouped by their type so each emitted stream carries exactly one type
// (folded into its identity). Each sample flattens to rows: a sample with TimestampsUnixNano emits
// one row per (timestamp, value); an aggregated sample emits one row at the profile's TimeNanos.
// Out-of-range dictionary indices are tolerated (resolve to zero), so a malformed batch never panics.
func Project(pd *Profiles, emit func(*recordengine.Batch)) (rows int) {
	d := &pd.Dictionary

	var b recordengine.Batch

	b.Ints = make([][]int64, 3)
	b.Bytes = make([][][]byte, 5)

	for ri := range pd.Resources {
		rp := &pd.Resources[ri]
		for si := range rp.Scopes {
			sp := &rp.Scopes[si]

			groups, order := groupByType(d, sp.Profiles)
			for _, k := range order {
				g := groups[k]
				n := fillBatch(&b, d, rp.Resource, sp.Scope, g.typ, g.profiles)
				if n == 0 {
					continue
				}

				emit(&b)
				rows += n
			}
		}
	}

	return rows
}

type typeGroup struct {
	typ      profileType
	profiles []*Profile
}

// groupByType buckets a scope's profiles by their resolved type, preserving first-seen order.
func groupByType(d *Dictionary, profiles []Profile) (map[string]*typeGroup, []string) {
	groups := map[string]*typeGroup{}

	var order []string

	for pi := range profiles {
		pr := &profiles[pi]
		typ := resolveType(d, pr)
		k := typ.key()

		g := groups[k]
		if g == nil {
			g = &typeGroup{typ: typ}
			groups[k] = g
			order = append(order, k)
		}

		g.profiles = append(g.profiles, pr)
	}

	return groups, order
}

// resolveType reads a profile's sample/period type+unit strings from the dictionary.
func resolveType(d *Dictionary, pr *Profile) profileType {
	return profileType{
		sampleType: dictString(d, pr.SampleType.TypeStrindex),
		sampleUnit: dictString(d, pr.SampleType.UnitStrindex),
		periodType: dictString(d, pr.PeriodType.TypeStrindex),
		periodUnit: dictString(d, pr.PeriodType.UnitStrindex),
	}
}

func dictString(d *Dictionary, idx int32) []byte {
	if inRange(d.Strings, idx) {
		return d.Strings[idx]
	}

	return nil
}

// streamSeries builds the stream identity for a (resource, scope, type): the resource attributes
// with the four reserved profile-type labels folded in, so the type is matchable and enumerable.
func streamSeries(res signal.Resource, scope signal.Scope, typ profileType) signal.Series {
	attrs := make([]signal.KeyValue, 0, len(res.Attributes)+4)
	attrs = append(attrs, res.Attributes...)
	attrs = append(attrs,
		signal.KeyValue{Key: LabelSampleType, Value: signal.StringValue(typ.sampleType)},
		signal.KeyValue{Key: LabelSampleUnit, Value: signal.StringValue(typ.sampleUnit)},
		signal.KeyValue{Key: LabelPeriodType, Value: signal.StringValue(typ.periodType)},
		signal.KeyValue{Key: LabelPeriodUnit, Value: signal.StringValue(typ.periodUnit)},
	)

	return signal.Series{
		Resource: signal.Resource{SchemaURL: res.SchemaURL, Attributes: signal.NewAttributes(attrs...)},
		Scope:    scope,
	}
}

// fillBatch resets b and populates it from one (Resource, Scope, type) group's profiles, returning
// the number of rows. It builds the content-addressed symbol delta into b.Side as it resolves each
// sample's stack.
func fillBatch(b *recordengine.Batch, d *Dictionary, res signal.Resource, scope signal.Scope, typ profileType, profiles []*Profile) int {
	series := streamSeries(res, scope, typ)
	b.Stream = series.Hash()
	b.Identity = func() signal.Series { return series }

	b.Ts = b.Ts[:0]
	for k := range b.Ints {
		b.Ints[k] = b.Ints[k][:0]
	}

	for k := range b.Bytes {
		b.Bytes[k] = b.Bytes[k][:0]
	}

	bld := newBuilder(d)
	rows := 0

	for _, pr := range profiles {
		for sx := range pr.Samples {
			s := &pr.Samples[sx]
			stackID := bld.stackID(s.StackIndex).AppendBinary(nil)
			traceID, spanID := linkIDs(d, s.LinkIndex)
			attrs := resolveAttributes(d, pr.AttributeIndices, s.AttributeIndices).AppendHashInput(nil)

			emitRow := func(ts, value int64) {
				b.Ts = append(b.Ts, ts)
				b.Ints[iValue] = append(b.Ints[iValue], value)
				b.Ints[iPeriod] = append(b.Ints[iPeriod], pr.Period)
				b.Ints[iDuration] = append(b.Ints[iDuration], pr.DurationNanos)
				b.Bytes[bStackID] = append(b.Bytes[bStackID], stackID)
				b.Bytes[bProfileID] = append(b.Bytes[bProfileID], pr.ProfileID)
				b.Bytes[bTraceID] = append(b.Bytes[bTraceID], traceID)
				b.Bytes[bSpanID] = append(b.Bytes[bSpanID], spanID)
				b.Bytes[bAttrs] = append(b.Bytes[bAttrs], attrs)
				rows++
			}

			if len(s.TimestampsUnixNano) > 0 {
				for i, ts := range s.TimestampsUnixNano {
					emitRow(int64(ts), valueAt(s.Values, i))
				}

				continue
			}

			emitRow(pr.TimeNanos, valueAt(s.Values, 0))
		}
	}

	b.Side = encodeDelta(bld.tables)

	return rows
}

// valueAt returns vals[i] or 0 if out of range.
func valueAt(vals []int64, i int) int64 {
	if i >= 0 && i < len(vals) {
		return vals[i]
	}

	return 0
}

// linkIDs resolves a sample's link index to the linked span's (trace id, span id), or (nil, nil) if
// the index is out of range.
func linkIDs(d *Dictionary, idx int32) (traceID, spanID []byte) {
	if inRange(d.Links, idx) {
		return d.Links[idx].TraceID, d.Links[idx].SpanID
	}

	return nil, nil
}

// resolveAttributes builds the [signal.Attributes] for a sample from the profile-level and
// sample-level attribute indices into the dictionary (out-of-range indices skipped).
func resolveAttributes(d *Dictionary, profileIdx, sampleIdx []int32) signal.Attributes {
	if len(profileIdx)+len(sampleIdx) == 0 {
		return nil
	}

	kvs := make([]signal.KeyValue, 0, len(profileIdx)+len(sampleIdx))

	add := func(indices []int32) {
		for _, ai := range indices {
			if !inRange(d.Attributes, ai) {
				continue
			}

			a := d.Attributes[ai]

			var key []byte
			if inRange(d.Strings, a.KeyStrindex) {
				key = d.Strings[a.KeyStrindex]
			}

			kvs = append(kvs, signal.KeyValue{Key: key, Value: a.Value})
		}
	}

	add(profileIdx)
	add(sampleIdx)

	return signal.NewAttributes(kvs...)
}
