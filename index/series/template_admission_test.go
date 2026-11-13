package series

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestTemplatePolicyRetries(t *testing.T) {
	t.Parallel()
	var p templatePolicy
	for range templateProbeWindow {
		require.True(t, p.probe())
		p.observe(false)
	}
	for range templateRetryDelay {
		require.False(t, p.probe())
	}
	for range templateProbeWindow {
		require.True(t, p.probe())
		p.observe(true)
	}
	require.Zero(t, p.skip)
}

func TestTemplateAdmissionTwoUseTail(t *testing.T) {
	t.Parallel()
	p := NewAttributeTemplates(8<<20, []byte("instance"))
	ix := NewWithAttributeTemplates(nil, p)
	for pattern := range 5000 {
		for host := range 2 {
			ix.Add(templateSeries(host, pattern))
		}
	}
	require.Empty(t, ix.templateRows)
	require.Positive(t, ix.templatePolicy.skip, "unprofitable input bypasses candidate construction temporarily")
	require.LessOrEqual(t, p.SizeBytes(), int64(8<<20))
}

func TestTemplateAdmissionIgnoresDuplicateIDs(t *testing.T) {
	t.Parallel()
	p := NewAttributeTemplates(8<<20, []byte("instance"))
	ix := NewWithAttributeTemplates(nil, p)
	s := templateSeries(0, 0)
	for range 1000 {
		ix.Add(s)
	}
	require.Empty(t, ix.templateRows)
	var observations uint64
	for i := range p.shards {
		for _, e := range p.shards[i].probation {
			observations += e.uses
		}
	}
	require.EqualValues(t, 1, observations)
}
