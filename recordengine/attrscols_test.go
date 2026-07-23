package recordengine_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/backend"
	"github.com/oteldb/storage/encoding/chunk"
	"github.com/oteldb/storage/query/fetch"
	"github.com/oteldb/storage/recordengine"
	"github.com/oteldb/storage/signal"
)

// dualAttrsSchema has two serialized-attribute columns: the record's own attributes and the
// stream's resource attributes stored per record. The record column is declared first, so it
// shadows the resource column for a key both carry.
var dualAttrsSchema = recordengine.NewSchema(
	recordengine.Column{Name: "body", Kind: recordengine.KindBytes, Codec: chunk.CodecDict, Bloom: recordengine.BloomFullText},
	recordengine.Column{Name: "attrs", Kind: recordengine.KindBytes, Codec: chunk.CodecDict, Bloom: recordengine.BloomAttrs},
	recordengine.Column{
		Name: "resource", Kind: recordengine.KindBytes, Codec: chunk.CodecDict,
		Bloom: recordengine.BloomAttrs, KeyScope: recordengine.KeyScopeResource,
	},
)

const (
	dBody = iota
	dAttrs
	dResource
)

func attrBlob(kvs ...[2]string) []byte {
	if len(kvs) == 0 {
		return nil
	}

	out := make([]signal.KeyValue, 0, len(kvs))
	for _, kv := range kvs {
		out = append(out, signal.KeyValue{Key: []byte(kv[0]), Value: signal.StringValue([]byte(kv[1]))})
	}

	return signal.NewAttributes(out...).AppendHashInput(nil)
}

// dualBatch builds a one-stream batch whose identity carries only svc, with each record's own
// attributes and the (constant) resource blob.
func dualBatch(svc string, resource []byte, recs ...struct {
	ts   int64
	body string
	attr []byte
},
) *recordengine.Batch {
	series := signal.Series{Resource: signal.Resource{Attributes: signal.NewAttributes(
		signal.KeyValue{Key: []byte("service.name"), Value: signal.StringValue([]byte(svc))},
	)}}

	b := &recordengine.Batch{
		Stream:   series.Hash(),
		Identity: func() signal.Series { return series },
		Ints:     make([][]int64, 0),
		Bytes:    make([][][]byte, 3),
	}

	for _, r := range recs {
		b.Ts = append(b.Ts, r.ts)
		b.Bytes[dBody] = append(b.Bytes[dBody], []byte(r.body))
		b.Bytes[dAttrs] = append(b.Bytes[dAttrs], r.attr)
		b.Bytes[dResource] = append(b.Bytes[dResource], resource)
	}

	return b
}

type dualRec = struct {
	ts   int64
	body string
	attr []byte
}

func newDualEngine(t *testing.T, be backend.Backend) *recordengine.Engine {
	t.Helper()

	return recordengine.New(recordengine.Config{Schema: dualAttrsSchema, Backend: be, Prefix: "p"})
}

func dualFetch(t *testing.T, e *recordengine.Engine, conds []fetch.Condition) []string {
	t.Helper()

	ctx := context.Background()
	it, err := e.Fetch(ctx, fetch.Request{
		Start: 0, End: 1000, Conditions: conds, AllConditions: true,
	})
	require.NoError(t, err)

	batches, err := fetch.Drain(ctx, it)
	require.NoError(t, err)

	var out []string
	for _, b := range batches {
		col, _ := b.Column("body")
		for _, v := range col.Bytes {
			out = append(out, string(v))
		}
	}

	return out
}

func equalCond(name, value string) fetch.Condition {
	return fetch.Condition{
		Column: name,
		Match:  func(v signal.Value) bool { return string(v.Str()) == value },
		Equal:  &fetch.EqualMatcher{Name: name, Value: value},
	}
}

// TestAttrsColumnsResolveEitherBlob is the multi-column lookup: a condition names a key that lives
// in only one of the two attribute columns and must find it either way.
func TestAttrsColumnsResolveEitherBlob(t *testing.T) {
	t.Parallel()

	e := newDualEngine(t, nil)
	res := attrBlob([2]string{"service.name", "api"}, [2]string{"service.instance.id", "inst-1"})

	b := dualBatch("api", res,
		dualRec{ts: 100, body: "first", attr: attrBlob([2]string{"http.method", "GET"})},
		dualRec{ts: 200, body: "second", attr: attrBlob([2]string{"http.method", "POST"})},
	)
	_, err := e.AppendBatch(b, recordengine.AppendLimits{})
	require.NoError(t, err)

	assert.Equal(t, []string{"first"}, dualFetch(t, e, []fetch.Condition{equalCond("http.method", "GET")}),
		"record-column key")
	assert.Equal(t, []string{"first", "second"}, dualFetch(t, e, []fetch.Condition{equalCond("service.instance.id", "inst-1")}),
		"resource-column key")
	assert.Empty(t, dualFetch(t, e, []fetch.Condition{equalCond("service.instance.id", "inst-9")}))
}

// TestAttrsColumnsRecordShadowsResource pins the declaration-order precedence: a key present in
// both blobs resolves from the column declared first.
func TestAttrsColumnsRecordShadowsResource(t *testing.T) {
	t.Parallel()

	e := newDualEngine(t, nil)
	res := attrBlob([2]string{"env", "resource-value"})

	b := dualBatch("api", res, dualRec{ts: 100, body: "row", attr: attrBlob([2]string{"env", "record-value"})})
	_, err := e.AppendBatch(b, recordengine.AppendLimits{})
	require.NoError(t, err)

	assert.Equal(t, []string{"row"}, dualFetch(t, e, []fetch.Condition{equalCond("env", "record-value")}))
	assert.Empty(t, dualFetch(t, e, []fetch.Condition{equalCond("env", "resource-value")}),
		"the record blob shadows the resource blob")
}

// TestAttrsColumnsBloomPrunesAcrossColumns checks the part-level prune stays sound with two attrs
// blooms: a key absent from *both* prunes the part, one present in *either* must not.
func TestAttrsColumnsBloomPrunesAcrossColumns(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	e := newDualEngine(t, backend.Memory())

	res := attrBlob([2]string{"service.instance.id", "inst-1"})
	b := dualBatch("api", res, dualRec{ts: 100, body: "row", attr: attrBlob([2]string{"http.method", "GET"})})
	_, err := e.AppendBatch(b, recordengine.AppendLimits{})
	require.NoError(t, err)
	require.NoError(t, e.Flush(ctx))
	require.Equal(t, 1, e.PartCount())

	assert.Equal(t, []string{"row"}, dualFetch(t, e, []fetch.Condition{equalCond("http.method", "GET")}),
		"present in the record bloom only")
	assert.Equal(t, []string{"row"}, dualFetch(t, e, []fetch.Condition{equalCond("service.instance.id", "inst-1")}),
		"present in the resource bloom only")
	assert.Empty(t, dualFetch(t, e, []fetch.Condition{equalCond("absent.key", "x")}),
		"absent from both blooms ⇒ the part is pruned")
}

// TestAttrsColumnsKeyScopes checks Keys reports each column's provenance, and that a key already in
// the stream identity is not also advertised as condition-only.
func TestAttrsColumnsKeyScopes(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	e := newDualEngine(t, backend.Memory())

	res := attrBlob([2]string{"service.name", "api"}, [2]string{"service.instance.id", "inst-1"})
	b := dualBatch("api", res, dualRec{ts: 100, body: "row", attr: attrBlob([2]string{"http.method", "GET"})})
	_, err := e.AppendBatch(b, recordengine.AppendLimits{})
	require.NoError(t, err)

	for _, stage := range []string{"head", "flushed"} {
		got := keyScopes(e.Keys(0, 0))
		assert.Equal(t, recordengine.KeyScopeResource|recordengine.KeyScopeIndexed, got["service.name"], stage)
		assert.Equal(t, recordengine.KeyScopeResource|recordengine.KeyScopeRecord, got["service.instance.id"], stage)
		assert.Equal(t, recordengine.KeyScopeRecord, got["http.method"], stage)

		require.NoError(t, e.Flush(ctx))
	}
}
