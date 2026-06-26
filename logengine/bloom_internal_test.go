package logengine

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/index/bloom"
	"github.com/oteldb/storage/query/fetch"
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
