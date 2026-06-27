package engine

// PrecisionTier is the absolute (wall-clock-free) form of a tenant float-precision policy: a part
// whose newest sample is older than Before is re-encoded, at merge, retaining only Bits
// significant mantissa bits in its value column (scaled-decimal, lossy). Fewer bits ⇒ denser, less
// accurate. It mirrors [RecompressSpec] (a per-part, age-gated cold rewrite) but as a list of
// coarsening tiers, so only old data trades accuracy for size. The caller ([storage.Storage])
// builds these from [tenant.PrecisionTier] and the current time.
type PrecisionTier struct {
	Before int64 // a part whose maxTime < Before is subject to this tier
	Bits   uint8 // significant mantissa bits to retain (1..63); 0 or ≥64 ⇒ lossless, ignored
}

// pickPrecision returns the precision budget for a part whose newest sample is maxTime: among the
// tiers the part is fully older than (maxTime < Before), the most aggressive (fewest Bits), or 0
// (lossless) when none applies. A part picks a coarser budget as it ages into older tiers.
func pickPrecision(tiers []PrecisionTier, maxTime int64) uint8 {
	best := uint8(0) // lossless
	for _, t := range tiers {
		if t.Bits == 0 || t.Bits >= 64 || maxTime >= t.Before {
			continue
		}

		if best == 0 || t.Bits < best {
			best = t.Bits
		}
	}

	return best
}

// partPrecision is the precision budget a part's value column was last encoded under: the value
// column's recorded FloatPrecisionBits, or 64 (lossless) when none was applied. It is the basis
// for the precision fixed point.
func partPrecision(p *part) uint8 {
	for _, c := range p.reader.Manifest().Columns {
		if c.Name == colValue {
			if c.FloatPrecisionBits == 0 {
				return 64
			}

			return c.FloatPrecisionBits
		}
	}

	return 64
}

// precisionApplies reports whether a (single) part should be rewritten to coarsen its value-column
// precision: an age tier targets a budget finer than 64 and the part is currently encoded at a
// finer budget than the target. The second test is the fixed point — once a part is at (or below)
// its target precision it is never rewritten again for precision, so a lone cold part does not
// churn the backend on every merge tick.
func precisionApplies(p *part, tiers []PrecisionTier) bool {
	target := pickPrecision(tiers, p.maxTime)
	if target == 0 {
		return false
	}

	return partPrecision(p) > target
}
