# Checked sidecar streaming

One pooled 32-KiB buffer coalesces fixed-bound entries. Intermediate writes update CRC32C;
the terminal CRC is appended after the final body bytes. Short writes, cancellation and
commit errors abort the private object. The caller supplies existing framing and owns
record ordering; this layer neither changes formats nor publishes catalog references.

Callers select this path only for bounded native object writers and include both writer
memory and encoder scratch in admission. Buffered backends retain their whole-object path.
