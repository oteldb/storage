# Performance plan — query-path allocation/GC reduction

**Status:** P0 done; P1.4 is the next lever (the new bench loop now flags it as #1). Source:
`/src/oteldb/benchmark/results/pprof/FINDINGS.md` and the captured profiles (oteldb v0.41.0, `file`
backend). Grounded in the current storage code.

## Done (P0) — measured by `scripts/bench-pprof.sh`

- **P0.2 — peek variable-length prefixes** (`bitstream.Buffered/Peek/Skip`; `readDoD`/`xorRead`):
  **GorillaDecode +26% throughput / −20% time** (benchstat, p=0.002), the float value column that
  dominates real decode CPU. DoD/decimal flat on constant-stride synthetic data.
- **P0.1 — pool part decode buffers** (`engine.decPool`, no-cross-fetch-cache path): **read-path
  B/op −19%** (geomean, p=0.002), `fetch_all` −8% time; `chunk.resize` drops out of the alloc
  profile entirely. Race-clean (merge copies out of the pooled `decodedPart`).
- **Bench loop** — `scripts/bench-pprof.sh {run,cmp,top}` captures benchstat timings+allocs and
  CPU + alloc_space pprof tops per labeled run, into `.bench/` (gitignored).

### What the loop now shows is #1 (do next)

After P0, the read-path **alloc profile is 50% `engine.sampleMerge.add` + 11% `collect`** — the
per-series `map[int64]sfval` (plan item **P1.4**). It is now the single biggest allocator *and* a GC
driver (`mapassign`/scan). Replacing it (single-source fast path, or a k-way merge of the sorted
runs, falling back to the map only on detected timestamp overlap) is the clear next win.

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
