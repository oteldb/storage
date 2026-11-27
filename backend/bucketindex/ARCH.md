# Bucket index

The index commits sample entries, WAL epochs, repair obligations and immutable series roots
in one conditional write. Sample block lineage applies only to `Entries` and `Wanted`.

`SeriesRoots` binds identity-only run prefixes to SHA-256 manifest digests. Roots carry no
sample times, coverage, statistics or ancestry. Missing roots must never become sample holes.

Readers accept v1 through v12. Without later features, writers preserve v8 or append sorted
series roots in v9. V10 adds retirements, and v11 fences ordinal sidecars and descriptor WAL
references. V12 persists metric capability bits, including the v11 compact-metadata flag. An
engine enables packed WAL only after the capability is confirmed by publication or catalog load.
Unrecognized flags fail closed.
Malformed paths, missing bindings, duplicate prefixes and conflicting bindings fail closed.

Use `MergeSeriesRoots` during adoption/rebase and include pending roots in every retry.
The helper returns owned storage and preserves all inputs. `AddSeriesRoot` also refuses
replacement with a different digest; `Save` validates the canonical list before its CAS.

Roots are append-only until metadata GC implements dependency and snapshot ownership.
Callers must sync root objects before publishing the index, pin read snapshots, protect roots
from orphan sweeps, and copy/repair their closure before installing dependent sample catalogs.

The wire format alone enables no clustered, EC or export support. Those callers must reject
unsupported root-bearing catalogs before mutation, even when their write option is disabled.
