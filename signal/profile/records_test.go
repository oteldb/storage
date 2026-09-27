package profile

import (
	"math/rand/v2"
	"strconv"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/recordengine"
	"github.com/oteldb/storage/signal"
)

// sideStacks returns the stack ids a batch's symbol delta carries.
func sideStacks(t *testing.T, b *recordengine.Batch) map[string]struct{} {
	t.Helper()

	store := NewSymbolStore()
	require.NoError(t, store.Absorb(b.Side))

	stacks := map[signal.SeriesID][]byte{}
	require.NoError(t, decodeTable(stacks, store.Encode()["stacks"]))

	out := make(map[string]struct{}, len(stacks))
	for id := range stacks {
		out[string(id.AppendBinary(nil))] = struct{}{}
	}

	return out
}

func refStacks(b *recordengine.Batch) map[string]struct{} {
	out := map[string]struct{}{}
	for _, id := range b.Bytes[bStackID] {
		out[string(id)] = struct{}{}
	}

	return out
}

func TestProjectDropsZeroObservations(t *testing.T) {
	t.Parallel()

	var pd Profiles
	d := &pd.Dictionary
	live := buildStack(d, "live", "live.go")
	dead := buildStack(d, "dead", "dead.go")
	cpu := ValueType{TypeStrindex: d.InternString([]byte("cpu")), UnitStrindex: d.InternString([]byte("nanoseconds"))}
	inuse := ValueType{TypeStrindex: d.InternString([]byte("inuse_space")), UnitStrindex: d.InternString([]byte("bytes"))}

	rp := pd.AddResource()
	rp.Resource = svcResource("api")
	sp := rp.AddScope()

	pr := sp.AddProfile()
	pr.SampleType = cpu
	pr.TimeNanos = 1000

	zero := pr.AddSample()
	zero.StackIndex, zero.Values = dead, []int64{0}

	mixed := pr.AddSample()
	mixed.StackIndex = live
	mixed.Values = []int64{0, 5, 0, -2}
	mixed.TimestampsUnixNano = []uint64{2000, 3000, 4000, 5000}

	counted := pr.AddSample()
	counted.StackIndex = live
	counted.TimestampsUnixNano = []uint64{6000, 7000}

	empty := sp.AddProfile()
	empty.SampleType = inuse
	empty.TimeNanos = 1000
	s := empty.AddSample()
	s.StackIndex, s.Values = dead, []int64{0}

	batches := collect(&pd)
	require.Len(t, batches, 1, "a stream whose every observation is 0 emits no batch")

	b := batches[0]
	assert.Equal(t, []int64{3000, 5000, 6000, 7000}, b.Ts)
	assert.Equal(t, []int64{5, -2, 1, 1}, b.Ints[iValue], "timestamps without values count 1 each")
	assert.Equal(t, refStacks(b), sideStacks(t, b), "the delta holds no stack only a dropped row referenced")
}

// TestProjectKeepsSums checks that dropping zero observations changes no sum: over random batches,
// every (stream, stack, timestamp, attributes, profile, link) group sums to what the OTLP
// observations sum to, and each batch's delta carries exactly the stacks its rows reference.
func TestProjectKeepsSums(t *testing.T) {
	t.Parallel()

	for seed := range uint64(200) {
		rng := rand.New(rand.NewPCG(seed, 696))
		pd := randomProfiles(rng)

		want := observationSums(&pd)
		got := map[string]int64{}

		for _, b := range collect(&pd) {
			assert.Equal(t, refStacks(b), sideStacks(t, b), "seed %d", seed)

			for i := range b.Ts {
				v := b.Ints[iValue][i]
				assert.NotZero(t, v, "seed %d: a zero observation was projected", seed)
				got[rowKey(b.Stream, b, i)] += v
			}
		}

		require.Equal(t, nonZero(want), nonZero(got), "seed %d", seed)
	}
}

func rowKey(stream signal.SeriesID, b *recordengine.Batch, i int) string {
	k := stream.AppendBinary(nil)
	k = strconv.AppendInt(k, b.Ts[i], 10)
	k = strconv.AppendInt(k, b.Ints[iPeriod][i], 10)
	k = strconv.AppendInt(k, b.Ints[iDuration][i], 10)

	for _, col := range b.Bytes {
		k = strconv.AppendQuote(k, string(col[i]))
	}

	return string(k)
}

// observationSums is the reference: every OTLP observation, zero or not, keyed like [rowKey].
func observationSums(pd *Profiles) map[string]int64 {
	d := &pd.Dictionary
	sums := map[string]int64{}

	for _, rp := range pd.Resources {
		for _, sp := range rp.Scopes {
			for pi := range sp.Profiles {
				pr := &sp.Profiles[pi]
				profileAttrs := resolveAttributes(d, pr.AttributeIndices)
				stream := streamSeries(rp.Resource, sp.Scope, resolveType(d, pr), profileAttrs).Hash()

				for sx := range pr.Samples {
					s := &pr.Samples[sx]
					traceID, spanID := linkIDs(d, s.LinkIndex)

					var b recordengine.Batch

					b.Ints = make([][]int64, 3)
					b.Bytes = make([][][]byte, 5)
					b.Ts = []int64{0}
					b.Ints[iPeriod] = []int64{pr.Period}
					b.Ints[iDuration] = []int64{pr.DurationNanos}
					b.Bytes[bStackID] = [][]byte{newBuilder(d).stackID(s.StackIndex).AppendBinary(nil)}
					b.Bytes[bProfileID] = [][]byte{pr.ProfileID}
					b.Bytes[bTraceID] = [][]byte{traceID}
					b.Bytes[bSpanID] = [][]byte{spanID}
					b.Bytes[bAttrs] = [][]byte{sampleAttributes(d, profileAttrs, s.AttributeIndices).AppendHashInput(nil)}

					switch {
					case len(s.TimestampsUnixNano) == 0:
						b.Ts[0] = pr.TimeNanos
						sums[rowKey(stream, &b, 0)] += s.Values[0]
					case len(s.Values) == 0:
						for _, ts := range s.TimestampsUnixNano {
							b.Ts[0] = int64(ts)
							sums[rowKey(stream, &b, 0)]++
						}
					default:
						for i, ts := range s.TimestampsUnixNano {
							b.Ts[0] = int64(ts)
							sums[rowKey(stream, &b, 0)] += s.Values[i]
						}
					}
				}
			}
		}
	}

	return sums
}

func nonZero(m map[string]int64) map[string]int64 {
	out := map[string]int64{}

	for k, v := range m {
		if v != 0 {
			out[k] = v
		}
	}

	return out
}

// randomProfiles builds a well-formed batch in every OTLP sample shape, with half its values 0.
func randomProfiles(rng *rand.Rand) Profiles {
	var pd Profiles

	d := &pd.Dictionary

	stacks := make([]int32, 6)
	for i := range stacks {
		stacks[i] = buildStack(d, "fn"+strconv.Itoa(i), "f.go")
	}

	attrs := make([]int32, 3)
	for i := range attrs {
		attrs[i] = d.AddAttribute(KeyValueAndUnit{
			KeyStrindex: d.InternString([]byte("k" + strconv.Itoa(i))),
			Value:       signal.StringValue([]byte("v" + strconv.Itoa(i))),
		})
	}

	d.AddLink(Link{})
	d.AddLink(Link{TraceID: []byte("trace"), SpanID: []byte("span")})

	types := make([]ValueType, 3)
	for i := range types {
		types[i] = ValueType{TypeStrindex: d.InternString([]byte("type" + strconv.Itoa(i)))}
	}

	value := func() int64 {
		if rng.IntN(2) == 0 {
			return 0
		}

		return rng.Int64N(21) - 5
	}

	pick := func(src []int32) []int32 {
		var out []int32

		for _, v := range src {
			if rng.IntN(2) == 0 {
				out = append(out, v)
			}
		}

		return out
	}

	for ri := range 1 + rng.IntN(2) {
		rp := pd.AddResource()
		rp.Resource = svcResource("svc" + strconv.Itoa(ri))

		for range 1 + rng.IntN(2) {
			sp := rp.AddScope()

			for range 1 + rng.IntN(4) {
				pr := sp.AddProfile()
				pr.SampleType = types[rng.IntN(len(types))]
				pr.TimeNanos = rng.Int64N(3)
				pr.Period = rng.Int64N(2)
				pr.AttributeIndices = pick(attrs)

				for range rng.IntN(10) {
					s := pr.AddSample()
					s.StackIndex = stacks[rng.IntN(len(stacks))]
					s.LinkIndex = int32(rng.IntN(2))
					s.AttributeIndices = pick(attrs)

					n := 1 + rng.IntN(3)

					switch rng.IntN(3) {
					case 0:
						s.Values = []int64{value()}
					case 1:
						for range n {
							s.TimestampsUnixNano = append(s.TimestampsUnixNano, uint64(rng.IntN(4)))
						}
					default:
						for range n {
							s.TimestampsUnixNano = append(s.TimestampsUnixNano, uint64(rng.IntN(4)))
							s.Values = append(s.Values, value())
						}
					}
				}
			}
		}
	}

	return pd
}

func attr(d *Dictionary, key, value string) int32 {
	return d.AddAttribute(KeyValueAndUnit{KeyStrindex: d.InternString([]byte(key)), Value: signal.StringValue([]byte(value))})
}

func TestProjectProfileAttributesInIdentity(t *testing.T) {
	t.Parallel()

	var pd Profiles
	d := &pd.Dictionary
	st := buildStack(d, "f", "f.go")
	docURL := attr(d, "pprof.profile.doc_url", "")
	thread := attr(d, "thread.name", "worker")

	rp := pd.AddResource()
	rp.Resource = svcResource("api")
	sp := rp.AddScope()

	for _, attrs := range [][]int32{{docURL}, {docURL}, nil} {
		pr := sp.AddProfile()
		pr.TimeNanos = 1
		pr.AttributeIndices = attrs
		s := pr.AddSample()
		s.StackIndex, s.Values, s.AttributeIndices = st, []int64{1}, []int32{thread}
	}

	var ids []signal.Series

	Project(&pd, func(b *recordengine.Batch) {
		id := b.Identity()
		assert.Equal(t, id.Hash(), b.Stream)
		ids = append(ids, id)

		for _, cell := range b.Bytes[bAttrs] {
			assert.Equal(t, signal.NewAttributes(
				signal.KeyValue{Key: []byte("thread.name"), Value: signal.StringValue([]byte("worker"))},
			).AppendHashInput(nil), cell, "the attrs column holds sample attributes only")
		}
	})

	require.Len(t, ids, 2, "profiles sharing attributes share a stream")
	assert.Equal(t, signal.NewAttributes(
		signal.KeyValue{Key: []byte("pprof.profile.doc_url"), Value: signal.StringValue([]byte(""))},
	), ids[0].Attributes)
	assert.Empty(t, ids[1].Attributes)
}

// TestProjectDuplicateAttributeKey pins the precedence of a key set at both levels: the profile's
// value answers a lookup on the row, as it did when both levels shared the attrs column.
func TestProjectDuplicateAttributeKey(t *testing.T) {
	t.Parallel()

	var pd Profiles
	d := &pd.Dictionary

	rp := pd.AddResource()
	rp.Resource = svcResource("api")
	pr := rp.AddScope().AddProfile()
	pr.TimeNanos = 1
	pr.AttributeIndices = []int32{attr(d, "k", "profile"), attr(d, "p", "only")}
	s := pr.AddSample()
	s.StackIndex = buildStack(d, "f", "f.go")
	s.Values = []int64{1}
	s.AttributeIndices = []int32{attr(d, "k", "sample"), attr(d, "s", "only")}

	var (
		identity signal.Attributes
		cell     []byte
	)

	Project(&pd, func(b *recordengine.Batch) {
		identity = b.Identity().Attributes
		cell = append([]byte(nil), b.Bytes[bAttrs][0]...)
	})

	v, ok := identity.Get([]byte("k"))
	require.True(t, ok)
	assert.Equal(t, "profile", string(v.Str()))

	for key, want := range map[string]string{"k": "profile", "s": "only"} {
		v, found, err := signal.LookupAttribute(cell, key)
		require.NoError(t, err)
		require.True(t, found, key)
		assert.Equal(t, want, string(v.Str()), key)
	}

	_, found, err := signal.LookupAttribute(cell, "p")
	require.NoError(t, err)
	assert.False(t, found, "a profile attribute no sample shadows stays out of the row")
}
