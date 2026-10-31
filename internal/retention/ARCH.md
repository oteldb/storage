# Background retention rewrites

Both engines share a one-hour minimum interval between forced rewrites of a part
whose expired rows were already removed. The moving cutoff can then advance
without rewriting its whole bucket on every maintenance tick.

Engine-owned part timestamps are immutable once published and inherited by merges.
Ordinary compaction still trims expired rows; whole-part expiry and foreground
compaction bypass the interval. Restarting loses the stamp and permits one extra
rewrite. No disk format or read filtering is introduced, so expired rows can
remain queryable during the physical cleanup interval.
