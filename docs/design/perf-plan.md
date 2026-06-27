# Performance plan — query-path allocation/GC reduction

**Status:** P0 + P1.4 + four loop iterations done — read path −74.7% time cumulative (see below).
Remaining gains are structural (P1.3 batch-release lifecycle, a public-API decision). Source:
`/src/oteldb/benchmark/results/pprof/FINDINGS.md` and the captured profiles (oteldb v0.41.0, `file`
backend). Grounded in the current storage code.

## Done — measured by `scripts/bench-pprof.sh` (loop)

The bench loop (`scripts/bench-pprof.sh {run,cmp,top}`) captures benchstat timings+allocs and CPU +
alloc_space pprof tops per labeled run (`.bench/`, gitignored). Each step below was profile-picked,
implemented, and verified with it; correctness via codec round-trip/fuzz + `-race` + golden.

**Cumulative read-path result (benchstat, baseline → final, p=0.002):**
`fetch_all` **−74.7% time / +295% throughput / B/op −63.5%**; `fetch_recent` **−49.8% time**.

In order:
1. **P0.2 — peek var-length prefixes** (`bitstream.Buffered/Peek/Skip`; `readDoD`/`xorRead`):
   GorillaDecode +26% throughput / −20% time.
2. **P0.1 — pool part decode buffers** (`engine.decPool`, no-cross-fetch-cache path): read B/op −19%;
   `chunk.resize` leaves the alloc profile. Race-clean (merge copies out of the pooled `decodedPart`).
3. **P1.4 — map-free sample merge** (single-source fast path + k-way run merge, freshest-wins):
   **fetch_all −70% time / +237% throughput**, allocs −37%. The dominant lever — eliminated the
   per-series `map[int64]sfval` and its GC scan.
4. **decimal decode, no scratch** — fold accumulate+convert into one pass into `dst` (dropped a
   `make([]int64, rows)` per decode, ~25% of read allocs): fetch_recent −13% time.
5. **`ReadUvarint` buffered fast path** — byte-from-word, no `ReadByte→ReadBits` per byte.
6. **decimal scale hoist** — precompute `10^exp` once: DecimalDecode ~10µs → ~6.8µs (~32%).
7. **`sortedWindow` in-order fast path** — skip scratch+sort for ascending head/flush buffers
   (live-head read path; flat on the flush-heavy golden bench).

### P1.3 — opt-in batch-release buffer pooling (DONE)

The result buffers (`collectOne` ~36% of read allocs) back the returned batches, so pooling them
needs a release signal. Added `fetch.Request.Recycle` + `fetch.Batch.Release()` (see ARCHITECTURE
§3g): opt-in, default-off (the non-recycling path is byte-for-byte unchanged — verified by
`efficiency_test.go` and a `rel0→rel2` benchstat showing `fetch_all` flat, p=0.18). When a caller
sets `Recycle` and releases each batch, the engine recycles the ts/value buffers via a shared hook
(no per-batch closure). The multi-child `Merge` and the cluster read handler release their inputs.
**Recycling read path: B/op −59%, time −24%** (benchstat `fetch_all` vs `fetch_all_release`); a
`-race` recycle + concurrent test guards correctness.

### P1.5 — record engine read-path pooling (DONE)

The logs/traces/profiles read path now pools two of its three dominant allocators (measured by
`BenchmarkLogReadAll` plain / `BenchmarkLogReadRelease` recycle; profile tops: `newRecordCols` 37%,
`readBytes` 27.6%, `fillConst[int64]`+`resize` ~25%):

- **Part-decode int columns** (`Engine.i64Pool`, always-on, both paths): a part's decoded timestamp
  + int columns are copied **by value** into the per-stream accumulators, so they are dead once the
  part is distributed (`recycleDecodeInts`) and can be reused with no aliasing risk. Targets the
  `fillConst`/`resize` ~25%. The byte columns are *not* pooled here (the accumulators alias them).
- **Per-stream accumulators** (`Engine.recPool`, opt-in via `Recycle`): `newRecordCols` (37%, the
  logs FINDINGS' #1 alloc) backs the returned batches, so it is pooled through the batch-release
  lifecycle — `fetch.Batch.SetReleaseState` carries the `*recordCols` handle (a pointer, no alloc)
  to the engine's one shared `recycle` hook, which re-arms it via `recordCols.prepare` on the next
  fetch. Differing projection/selection across fetches is handled by `prepare` (reuse backing where
  cap suffices, drop deselected columns to nil).

**Result (baseline → final):** plain read **B/op −25%, allocs −23%** (the int pooling alone, no API
change); recycle read **B/op −63%, time −44%, throughput +~85%, allocs −33%**. Race-clean; a new
`TestFetchRecycleMatchesPlain` (multi-part + head, alternating projections, many release→fetch
rounds) guards that reuse never corrupts a later result.

### What's left

- **Record byte columns** (`readBytes` 27.6%, the decoded log/attr bytes) alias into the returned
  batches, so pooling them needs either per-part refcounting (release the part-decode bytes once all
  referencing batches are released) or a record decode cache (like the metric `decodeCache`) — the
  structural remainder after P1.5.
- **Win1 (pool head/flush windows)** — deferred: golden-flat (the lib bench is flush-heavy with a
  cold head); it helps only the live-ingestion head path the integration profile sees.
- CPU is now the inherently **serial** DoD/varint decode (not SIMD-amenable) + the result copy.

## Diagnosis (what the profiles actually show)

Query latency is **allocation/GC-bound**. Two root allocation sources plus one CPU hotspot:

- **Metrics** — decode runs *every query* and allocates fresh buffers:
  `chunk.resize[int64]` 35.5% + `chunk.resize[float64]` 35.3% = **70.8% of allocs**;
  `compress.Decompress` 11% and `os.readFileContents` 11% (parts re-read + re-decompressed per
  query → the decode/read caches are off or ineffective in this build). CPU is **bit-at-a-time
  decode**: `bitstream.ReadBit` **16.7% flat** (#1), `DecodeFloats` 32.6% cum, `DecodeTimestamps`
  25% cum, `xorRead` 22.6% cum; GC (`spanClass`+`memclr`+`tryDeferToSpanScan`) ~30%, driven by the
  resize allocs.
- **Logs** — `recordengine.newRecordCols` **67% of allocs** (93.9 GB in 20 s); `signal.DecodeAttributes`
  6.8%. GC is ~60% of CPU.
- **Traces** — `newRecordCols` 13%, `DecodeAttributes` 8%, `permute[[]uint8]` 9% (byte copies during
  sort); GC ~50%.
- **Resident heap** everywhere is dominated by `recordCols.appendClone`/`cloneBytes` (65–78%) — the
  **head** holding cloned ingested bytes (the live data itself), distinct from the query-path churn.

### The recurring constraint

Results are **zero-copy**: a `fetch.Batch`'s slices alias decoded buffers. So buffers can only be
pooled where either (a) the result does **not** alias them, or (b) a release lifecycle returns them
after the caller is done. Verified in code:

- **Metrics: safe to pool now.** `sampleMerge.add` copies decoded values into a per-series
  `map[int64]sfval`, and `collect()` allocates fresh result slices — so the cached `decodedPart`
  buffers are **not** aliased by results. They can be pooled.
- **Records: results alias.** `recordCols.appendRange`/`appendRow` copy byte slices **by reference**
  from the decoded part into the accumulators, which become the result batches. Pooling the record
  decode/accumulator buffers therefore needs a batch-release lifecycle.

## Plan (prioritized by impact × safety)

### P0 — metrics, high impact, safe, no API change

1. **Pool the chunk decode buffers** (kills ~70% of metric allocs). Add `sync.Pool`s of `[]int64`
   and `[]float64` in `engine` (or `chunk`), `Get` a buffer before `DecodeTimestamps`/`DecodeFloats`
   and `Put` it back after `mergeSeriesInto` has copied the values out. Safe because the merge copies
   (above). `resize` already pre-sizes to the header row count, so a pooled buffer of sufficient cap
   is reused with no `make`. *Expected:* metric decode allocs ~70% → ~0 on the miss path; large GC
   drop. With the decode cache enabled, integrate via eviction→pool + a part-style refcount (P1).

2. **Vectorize the bitstream decode** (cuts the #1 CPU item, `ReadBit` 16.7% flat). `xorRead`/`readDoD`
   loop `ReadBit()` one bit at a time to read variable-length control prefixes (Gorilla
   leading/trailing selectors, DoD bucket prefix). Add a `Peek(nbits)`/`SkipBits(n)` (or
   count-leading-ones) primitive to `bitstream.Reader` and decode each prefix from one peeked word.
   `ReadBits` is already word-buffered — reuse its machinery. *Expected:* removes most of the 16.7%
   flat and speeds `DecodeFloats`/`DecodeTimestamps` (33%/25% cum). Safety net: the codec
   round-trip + fuzz tests already exist; keep them green.

### P1 — records, biggest record win, needs a release lifecycle

3. **Pool `recordCols` via batch release** (targets `newRecordCols` 67% of log allocs, ~60% of log
   CPU). Add an optional release hook to the fetch contract — e.g. `Batch.Release()` or an
   iterator-scoped allocator returned on `Iterator.Close()` *after* the caller has drained — so the
   record engine recycles both the per-stream accumulators and the per-part decode `recordCols`
   (`sync.Pool` keyed by schema). Opt-in: the default path stays GC-collected (today's behavior), so
   existing embedders are unaffected; oteldb adopts the release call. *Expected:* the 93.9 GB/20 s
   churn becomes reuse; ~60% of logs CPU recovered. Higher effort — the aliasing means correctness
   hinges on the caller honoring "done after release"; gate behind a fuzz/race test that reuse never
   corrupts a live batch.

4. **Metric merge without the per-series map** (cuts GC `scanObject` and the per-query map alloc).
   `sampleMerge` builds a `map[int64]sfval` per series per query purely to dedup timestamps across
   sources. Parts are time-ordered and the head is newest, so sources rarely overlap: do a
   sequential/k-way merge of the already-sorted runs into pre-sized slices, falling back to the map
   only when a timestamp overlap is detected. Removes the map alloc + the `collect()` sort for the
   common case. *Expected:* meaningful GC reduction for metric queries with many series.

### P2 — broadly applicable + config

5. **`signal.DecodeAttributes` — reuse buffers** (7–8% of allocs, all signals). Offer an append-style
   / scratch-reusing decode so attribute decode writes into a caller-provided buffer instead of
   allocating per call. Low risk.

6. **Traces: index-permute instead of `permute[[]uint8]`** (9% of trace allocs) — sort by a
   permutation of indices and apply in place, avoiding the byte-slice copies; stream spansets rather
   than materializing all up front (the `materializeSpans` 33% path). Partly oteldb-side.

7. **Embedder config (cheap, large):** enable the **decode cache** (`WithDecodeCache`) and the
   **backend object read cache** in oteldb — `os.readFileContents` (11%) + `Decompress` (11%) show
   parts are re-read and re-decompressed every query, i.e. the caches are off/ineffective in this
   build. Not a library code change, but the single biggest config lever; flag it to the embedder.

8. **GC tuning** (`GOGC`/soft memory limit) is a band-aid per the findings — the allocation fixes
   above are the real cure. Embedder-owned.

### Out of scope / larger follow-on

- **Resident-heap reduction** (`appendClone`/`cloneBytes`, 65–78% of resident heap): the head clones
  each appended attribute/byte value. Interning repeated attribute *values* in the head (a
  per-head dictionary, like the part dictionary codec) or arena-allocating same-batch bytes would cut
  resident memory. Structural; lower priority than the query-path churn above, but it is the
  resident-heap dominator.

## Suggested sequencing

P0.1 + P0.2 first — safe, no API change, and they target the metric path's 70% allocs + #1 CPU item
together. Then P1.4 (map-free merge, metrics) and P2.5 (attributes) as further safe wins. P1.3
(recordCols pooling) is the biggest record win but needs the release-lifecycle design — do it once
the P0 wins are measured. Re-profile after each step; the golden benchmarks + `efficiency_test.go`
ceilings are the regression gate.
