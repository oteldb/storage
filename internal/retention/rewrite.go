// Package retention bounds repeated background rewrites at a moving expiry cutoff.
package retention

import "time"

// RewriteInterval is the minimum interval between forced background retention rewrites.
const RewriteInterval = time.Hour

// Due reports whether a partial-retention rewrite may be forced. A zero now bypasses coalescing.
func Due(last, now time.Time) bool {
	return now.IsZero() || last.IsZero() || now.Sub(last) >= RewriteInterval
}
