package profile

import (
	"github.com/oteldb/storage/encoding/chunk"
	"github.com/oteldb/storage/recordengine"
	"github.com/oteldb/storage/signal"
)

// Column names of the profiles sample schema. The sample timestamp is the implicit primary
// timestamp / sort key, and the stream id is the Resource+Scope hash. stack_id and sample_type_id
// are content-addressed references into the symbol store (resolved by the embedder, deferred at the
// fetch seam this milestone).
const (
	ColValue      = "value"
	ColSampleType = "sample_type" // content id of the (type, unit) pair, as int64
	ColPeriod     = "period"      // profile period (denormalized onto each sample)
	ColDuration   = "duration"    // profile duration_nanos (denormalized)
	ColStackID    = "stack_id"    // 16-byte content id into the symbol-store stack table
	ColProfileID  = "profile_id"  // 16-byte OTLP profile id
	ColTraceID    = "trace_id"    // linked span's trace id (16 bytes) or empty
	ColSpanID     = "span_id"     // linked span's span id (8 bytes) or empty
	ColAttrs      = "attrs"       // serialized per-sample attributes (profile ∪ sample)
)

// Schema is the profiles vertical's record-engine column schema: one row per sample observation.
// profile_id carries an equality bloom (profile-by-id pruning, future); attrs the attribute bloom.
var Schema = recordengine.NewSchema(
	recordengine.Column{Name: ColValue, Kind: recordengine.KindInt64, Codec: chunk.CodecT64},
	recordengine.Column{Name: ColSampleType, Kind: recordengine.KindInt64, Codec: chunk.CodecT64},
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
	iSampleType
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

// Project iterates a [Profiles] batch and calls emit once per stream (each Resource+Scope group)
// with a [recordengine.Batch] of that stream's sample rows in the profiles [Schema]'s column order,
// plus the content-addressed symbol delta (Batch.Side) the rows reference. It returns the number of
// rows emitted.
//
// Each sample flattens to rows: a sample with TimestampsUnixNano emits one row per (timestamp,
// value); an aggregated sample (no timestamps) emits one row at the profile's TimeNanos. Stacks and
// the symbols they reference are interned into the per-stream delta via a content-addressed
// [builder]; sample_type, profile id, linked trace/span ids and attributes are denormalized onto
// each row. Out-of-range dictionary indices are tolerated (resolve to zero), so a malformed batch
// never panics.
func Project(pd *Profiles, emit func(*recordengine.Batch)) (rows int) {
	d := &pd.Dictionary

	var b recordengine.Batch

	b.Ints = make([][]int64, 4)
	b.Bytes = make([][][]byte, 5)

	for ri := range pd.Resources {
		rp := &pd.Resources[ri]
		for si := range rp.Scopes {
			sp := &rp.Scopes[si]
			if len(sp.Profiles) == 0 {
				continue
			}

			n := fillBatch(&b, d, rp.Resource, sp.Scope, sp.Profiles)
			if n == 0 {
				continue
			}

			emit(&b)
			rows += n
		}
	}

	return rows
}

// fillBatch resets b and populates it from one stream's profiles (every sample of every profile in
// the scope), returning the number of rows. It builds the content-addressed symbol delta into
// b.Side as it resolves each sample's stack.
func fillBatch(b *recordengine.Batch, d *Dictionary, res signal.Resource, scope signal.Scope, profiles []Profile) int {
	series := signal.Series{Resource: res, Scope: scope}
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

	for pi := range profiles {
		pr := &profiles[pi]
		sampleType := sampleTypeID(d, pr.SampleType)

		for sx := range pr.Samples {
			s := &pr.Samples[sx]
			stackID := bld.stackID(s.StackIndex).AppendBinary(nil)
			traceID, spanID := linkIDs(d, s.LinkIndex)
			attrs := resolveAttributes(d, pr.AttributeIndices, s.AttributeIndices).AppendHashInput(nil)

			emitRow := func(ts, value int64) {
				b.Ts = append(b.Ts, ts)
				b.Ints[iValue] = append(b.Ints[iValue], value)
				b.Ints[iSampleType] = append(b.Ints[iSampleType], sampleType)
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
