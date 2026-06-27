package engine

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

// TestPickPrecision covers tier selection by part age: recent ⇒ lossless, and an older part takes
// the most aggressive (fewest-bits) tier it qualifies for.
func TestPickPrecision(t *testing.T) {
	t.Parallel()

	tiers := []PrecisionTier{{Before: 100, Bits: 24}, {Before: 50, Bits: 12}}

	assert.Equal(t, uint8(0), pickPrecision(tiers, 200), "recent part ⇒ lossless")
	assert.Equal(t, uint8(24), pickPrecision(tiers, 70), "older than the 24-bit tier only")
	assert.Equal(t, uint8(12), pickPrecision(tiers, 30), "older than both ⇒ most aggressive")
	assert.Equal(t, uint8(0), pickPrecision(nil, 0), "no tiers ⇒ lossless")
	assert.Equal(t, uint8(0), pickPrecision([]PrecisionTier{{Before: 100, Bits: 0}, {Before: 100, Bits: 64}}, 10),
		"lossless/out-of-range tiers are ignored")
}
