package pdataconv

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/pprofile"

	"github.com/oteldb/storage/signal"
	"github.com/oteldb/storage/signal/profile"
)

func TestAppendProfiles(t *testing.T) {
	t.Parallel()

	pd := pprofile.NewProfiles()

	// Shared dictionary: strings 0="" 1="cpu" 2="nanoseconds" 3="main".
	dict := pd.Dictionary()
	dict.StringTable().Append("", "cpu", "nanoseconds", "main")

	fn := dict.FunctionTable().AppendEmpty()
	fn.SetNameStrindex(3)
	fn.SetStartLine(10)

	mp := dict.MappingTable().AppendEmpty()
	mp.SetMemoryStart(0x1000)
	mp.SetFilenameStrindex(3)

	loc := dict.LocationTable().AppendEmpty()
	loc.SetMappingIndex(0)
	loc.SetAddress(0x1234)
	line := loc.Lines().AppendEmpty()
	line.SetFunctionIndex(0)
	line.SetLine(42)
	line.SetColumn(7)

	stack := dict.StackTable().AppendEmpty()
	stack.LocationIndices().Append(0)

	attr := dict.AttributeTable().AppendEmpty()
	attr.SetKeyStrindex(1)
	attr.Value().SetStr("v")
	attr.SetUnitStrindex(2)

	traceID := pcommon.TraceID([16]byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16})
	spanID := pcommon.SpanID([8]byte{1, 2, 3, 4, 5, 6, 7, 8})
	link := dict.LinkTable().AppendEmpty()
	link.SetTraceID(traceID)
	link.SetSpanID(spanID)

	rp := pd.ResourceProfiles().AppendEmpty()
	rp.Resource().Attributes().PutStr("service.name", "api")
	scp := rp.ScopeProfiles().AppendEmpty()
	scp.Scope().SetName("lib")

	p := scp.Profiles().AppendEmpty()
	p.SampleType().SetTypeStrindex(1)
	p.SampleType().SetUnitStrindex(2)
	p.PeriodType().SetTypeStrindex(1)
	p.PeriodType().SetUnitStrindex(2)
	p.SetTime(pcommon.Timestamp(5000))
	p.SetDurationNano(1000)
	p.SetPeriod(100)
	p.SetProfileID(pprofile.ProfileID([16]byte{9, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9}))
	p.AttributeIndices().Append(0)

	s := p.Samples().AppendEmpty()
	s.SetStackIndex(0)
	s.SetLinkIndex(0)
	s.Values().Append(123)
	s.TimestampsUnixNano().Append(5000)
	s.AttributeIndices().Append(0)

	var out profile.Profiles
	require.Equal(t, 0, AppendProfiles(&out, pd))

	// Dictionary tables survived index-preserving.
	d := out.Dictionary
	require.Len(t, d.Strings, 4)
	assert.Equal(t, []byte(""), d.Strings[0])
	assert.Equal(t, []byte("cpu"), d.Strings[1])
	assert.Equal(t, []byte("main"), d.Strings[3])

	require.Len(t, d.Functions, 1)
	assert.Equal(t, int32(3), d.Functions[0].NameStrindex)
	assert.Equal(t, int64(10), d.Functions[0].StartLine)

	require.Len(t, d.Mappings, 1)
	assert.Equal(t, uint64(0x1000), d.Mappings[0].MemoryStart)

	require.Len(t, d.Locations, 1)
	assert.Equal(t, uint64(0x1234), d.Locations[0].Address)
	require.Len(t, d.Locations[0].Lines, 1)
	assert.Equal(t, int64(42), d.Locations[0].Lines[0].Line)
	assert.Equal(t, int64(7), d.Locations[0].Lines[0].Column)

	require.Len(t, d.Stacks, 1)
	assert.Equal(t, []int32{0}, d.Stacks[0].LocationIndices)

	require.Len(t, d.Attributes, 1)
	assert.Equal(t, int32(1), d.Attributes[0].KeyStrindex)
	assert.Equal(t, int32(2), d.Attributes[0].UnitStrindex)
	assert.True(t, signal.StringValue([]byte("v")).Equal(d.Attributes[0].Value))

	require.Len(t, d.Links, 1)
	assert.Equal(t, traceID[:], d.Links[0].TraceID)
	assert.Equal(t, spanID[:], d.Links[0].SpanID)

	// Resource/scope/profile/sample structure.
	require.Len(t, out.Resources, 1)
	rv, _ := out.Resources[0].Resource.Attributes.Get([]byte("service.name"))
	assert.Equal(t, []byte("api"), rv.Str())
	require.Len(t, out.Resources[0].Scopes, 1)
	assert.Equal(t, []byte("lib"), out.Resources[0].Scopes[0].Scope.Name)

	require.Len(t, out.Resources[0].Scopes[0].Profiles, 1)
	pr := out.Resources[0].Scopes[0].Profiles[0]
	assert.Equal(t, profile.ValueType{TypeStrindex: 1, UnitStrindex: 2}, pr.SampleType)
	assert.Equal(t, profile.ValueType{TypeStrindex: 1, UnitStrindex: 2}, pr.PeriodType)
	assert.Equal(t, int64(5000), pr.TimeNanos)
	assert.Equal(t, int64(1000), pr.DurationNanos)
	assert.Equal(t, int64(100), pr.Period)
	assert.Equal(t, []byte{9, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9}, pr.ProfileID)
	assert.Equal(t, []int32{0}, pr.AttributeIndices)

	require.Len(t, pr.Samples, 1)
	sm := pr.Samples[0]
	assert.Equal(t, int32(0), sm.StackIndex)
	assert.Equal(t, []int64{123}, sm.Values)
	assert.Equal(t, []uint64{5000}, sm.TimestampsUnixNano)
	assert.Equal(t, []int32{0}, sm.AttributeIndices)

	// The sample's stack reference resolves into the dictionary's stack -> location.
	require.Less(t, sm.StackIndex, int32(len(d.Stacks)))
	locIdx := d.Stacks[sm.StackIndex].LocationIndices[0]
	require.Less(t, locIdx, int32(len(d.Locations)))
	assert.Equal(t, uint64(0x1234), d.Locations[locIdx].Address)
}
