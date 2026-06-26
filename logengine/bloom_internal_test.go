package logengine

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/index/bloom"
	"github.com/oteldb/storage/query/fetch"
	"github.com/oteldb/storage/signal"
)

func TestBuildBodyBloomContainsTokens(t *testing.T) {
	t.Parallel()

	data := buildBodyBloom([][]byte{[]byte("connection refused"), []byte("timeout error")})
	f, _, err := bloom.Decode(data)
	require.NoError(t, err)

	for _, tok := range []string{"connection", "refused", "timeout", "error"} {
		assert.Truef(t, f.Test([]byte(tok)), "body token %q present in the bloom", tok)
	}
}

func TestBodyTokensPresent(t *testing.T) {
	t.Parallel()

	f, _, err := bloom.Decode(buildBodyBloom([][]byte{[]byte("alpha beta")}))
	require.NoError(t, err)

	withBloom := &part{bodyBloom: f}
	noBloom := &part{} // nil bloom ⇒ never pruned

	all := func(toks ...string) []fetch.Condition {
		c := fetch.Condition{Column: "body"}
		for _, tk := range toks {
			c.Tokens = append(c.Tokens, []byte(tk))
		}

		return []fetch.Condition{c}
	}

	assert.True(t, withBloom.bodyTokensPresent(all("alpha")), "present token ⇒ keep")
	assert.True(t, withBloom.bodyTokensPresent(all("alpha", "beta")), "all present ⇒ keep")
	assert.False(t, withBloom.bodyTokensPresent(all("gamma")), "absent token ⇒ prune")
	assert.False(t, withBloom.bodyTokensPresent(all("alpha", "gamma")), "any absent ⇒ prune")
	assert.True(t, noBloom.bodyTokensPresent(all("anything")), "no bloom ⇒ always scan")
}

func TestAttrEqualsPresent(t *testing.T) {
	t.Parallel()

	// Build an attribute bloom over two records' serialized attributes.
	rec := func(key, val string) []byte {
		return signal.NewAttributes(signal.KeyValue{Key: []byte(key), Value: signal.StringValue([]byte(val))}).AppendHashInput(nil)
	}

	f, _, err := bloom.Decode(buildAttrBloom([][]byte{rec("user", "alice"), rec("msg", "connection refused")}))
	require.NoError(t, err)

	withBloom := &part{attrBloom: f}
	noBloom := &part{}

	eq := func(specs ...[2]string) []fetch.Condition {
		out := make([]fetch.Condition, len(specs))
		for i, s := range specs {
			out[i] = fetch.Condition{Column: s[0], Equal: &fetch.EqualMatcher{Name: s[0], Value: s[1]}}
		}

		return out
	}
	contains := func(col string, toks ...string) []fetch.Condition {
		c := fetch.Condition{Column: col}
		for _, tk := range toks {
			c.Tokens = append(c.Tokens, []byte(tk))
		}

		return []fetch.Condition{c}
	}

	// Equality pruning.
	assert.True(t, withBloom.attrConditionsPresent(eq([2]string{"user", "alice"})), "present key=value ⇒ keep")
	assert.True(t, withBloom.attrConditionsPresent(eq([2]string{"user", "alice"}, [2]string{"msg", "connection refused"})), "all present ⇒ keep")
	assert.False(t, withBloom.attrConditionsPresent(eq([2]string{"user", "bob"})), "absent value ⇒ prune")
	assert.False(t, withBloom.attrConditionsPresent(eq([2]string{"team", "x"})), "absent key ⇒ prune")
	assert.True(t, withBloom.attrConditionsPresent([]fetch.Condition{{Column: "user"}}), "no hints ⇒ not consulted")

	// Non-equality (contains) pruning over key-scoped value tokens.
	assert.True(t, withBloom.attrConditionsPresent(contains("msg", "connection")), "present word ⇒ keep")
	assert.True(t, withBloom.attrConditionsPresent(contains("msg", "connection", "refused")), "all words present ⇒ keep")
	assert.False(t, withBloom.attrConditionsPresent(contains("msg", "timeout")), "absent word ⇒ prune")
	assert.False(t, withBloom.attrConditionsPresent(contains("user", "connection")), "word under the wrong key ⇒ prune")

	assert.True(t, noBloom.attrConditionsPresent(eq([2]string{"user", "alice"})), "no bloom ⇒ always scan")
}
