# Background retention rewrites

Both engines share a one-hour minimum interval between forced rewrites of a part
whose expired rows were already removed. The moving cutoff can then advance
without rewriting its whole bucket on every maintenance tick.

Engine-owned part timestamps are immutable once published and inherited by merges.
Ordinary compaction still trims expired rows; whole-part expiry and foreground
compaction bypass the interval. Restarting loses the stamp and permits one extra
rewrite. The gate changes neither disk format nor low-level engine read filtering.
The storage facade separately clamps sample and aggregate reads to the signal's
age cutoff, hiding age-cleanup lag. Size-based retention and direct engine reads
remain eventual and can expose rows until physical cleanup removes them.

`State` atomically publishes a requested cutoff, its winning bound and ownership term. A reader sees
one coherent request only for its current term; stale requests cannot replace a newer term. There
is no disk-format state. Identical requests
reuse the existing snapshot. Both engines record only after authorization, keeping deferred cleanup
visible without attributing requests from an earlier ownership tenure.
