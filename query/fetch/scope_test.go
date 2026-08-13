package fetch_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/oteldb/storage/query/fetch"
)

func TestScopeContext(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	assert.Nil(t, fetch.ScopeFrom(ctx), "an unmarked context carries no scope")

	s := fetch.NewScope()
	assert.Same(t, s, fetch.ScopeFrom(fetch.WithScope(ctx, s)))
}

// TestScopeEnterCharge pins the Enter/Charge/Release contract admission control relies on: a scope
// reports "already holding" only between a Charge and the matching Release.
func TestScopeEnterCharge(t *testing.T) {
	t.Parallel()

	s := fetch.NewScope()

	assert.False(t, s.Enter())
	s.Charge(10)

	assert.True(t, s.Enter())
	s.Abort()

	s.Release(10)
	assert.False(t, s.Enter())
	s.Abort()

	// A nil scope never holds and never blocks (the unscoped path).
	var nilScope *fetch.Scope

	assert.False(t, nilScope.Enter())
	nilScope.Charge(10)
	nilScope.Release(10)
	nilScope.Abort()
}
