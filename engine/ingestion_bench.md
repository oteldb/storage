# Ingestion contention benchmark

`BenchmarkIngestionContention` exercises native file storage and the real WAL,
with one or eight concurrent writers. IDs and identities are precomputed once;
engine creation, identity seeding and cleanup are outside the measured phase.
Each operation creates a fresh engine and replays a fixed number of rounds, so
calibration does not grow a single retained dataset indefinitely.

The default CPU-only fixture has 10,000 hosts, eight CPU-mode series per host,
and 16 rounds: 1.28 million measured samples per operation. This isolates engine
locks; it does not represent the full node-exporter metric mix or HTTP/decoder
costs. Each writer owns disjoint series and submits at most 4,096 points per
append with increasing 15-second timestamps.

- `resident` seeds identities into the active head and measures WAL/publication
  contention without pressure flushing. Its final active samples are reported;
  final flush/close is outside timing and the phase profiles.
- `pressure` flushes the seed to disk, then enables asynchronous pressure flushes
  with an 8-MiB resident target, 32-MiB builder target and 128-MiB shared build pool.
  The smaller head makes this CPU-series subset exercise the pressure lifecycle.
  Timing/profiles include concurrent appends, pending pressure-job completion,
  and the terminal flush. No active samples remain. There are no background merges.

The cases are different scopes, not equivalent durable-storage throughput tests.
Both use file-backed WAL with sync disabled, matching the latest live benchmark.
`flushes/op`, `parts/op`, `head-samples/op`, `samples/op` and `samples/s` make the
work performed visible. Parts exclude the disk seed; flushes exclude setup.
Small fixtures may exercise only the terminal flush; check `flushes/op` and the
pressure wait stacks before claiming that pressure contention was reproduced.

Use a disk-backed `TMPDIR` for native data, not tmpfs. For repeated measurements:

```sh
mkdir -p "$PWD/.bench-tmp"
TMPDIR="$PWD/.bench-tmp" GOMAXPROCS=8 \
  go test -vet=off ./engine -run '^$' \
  -bench '^BenchmarkIngestionContention$' -benchtime=1x -count=6
```

To collect phase-only profiles, select exactly one leaf in a fresh process and
use `-benchtime=1x -count=1`. Mutex/block profiles are process-wide and cumulative;
the benchmark enables sampling only after setup. Do not combine this mode with
Go's global `-cpuprofile`, `-mutexprofile`, `-blockprofile` or `-trace` flags.

```sh
mkdir -p "$PWD/.bench-tmp" "$PWD/.bench-profiles"
TMPDIR="$PWD/.bench-tmp" GOMAXPROCS=8 \
  OTELDB_BENCH_INGEST_ROUNDS=64 \
  OTELDB_BENCH_INGEST_PROFILE="$PWD/.bench-profiles/ingest" \
  go test -vet=off ./engine -run '^$' \
  -bench '^BenchmarkIngestionContention$/pressure/writers=8$' \
  -benchtime=1x -count=1 -o "$PWD/.bench-profiles/engine.test"

go tool pprof -http=127.0.0.1:8080 .bench-profiles/engine.test \
  .bench-profiles/ingest.pressure.w8.mutex
go tool pprof -http=127.0.0.1:8080 .bench-profiles/engine.test \
  .bench-profiles/ingest.pressure.w8.block
go tool trace .bench-profiles/ingest.pressure.w8.trace
```

The same prefix produces `.cpu`, `.allocs.before`, `.allocs.after` and `.heap.after`.
Allocation deltas use `pprof -alloc_space -base before after`. Mutex delay is
reported at the releasing lock holder; block delay reports waiting call paths,
including pressure-channel waits and the benchmark's worker join. Neither profile
alone measures how long all writers were simultaneously blocked; use the trace.
Profiled timings include sampling overhead and should be kept separate from the
unprofiled measurements.

`OTELDB_BENCH_INGEST_HOSTS` selects 1–80,000 hosts;
`OTELDB_BENCH_INGEST_ROUNDS` selects 1–64 rounds;
`OTELDB_BENCH_INGEST_HEAD_MIB` selects a 1–1,024 MiB pressure target.
File/WAL fixtures are removed after each operation. Keep reports/profiles outside
tracked source and testdata. Full live workload acceptance, queue loss and freshness
still need the scale benchmark after any optimization.

Prepared-ingestion diagnostics report `parallel-samples/op`, `ordered-samples/op`,
`ordered-batches/op`, `pressure-misses/op`, `snapshot-misses/op`, `work-limit/op`
and `retry-success/op`. `wal-appends/op` counts measured WAL record-batch append calls;
fixture seeding is excluded. These separate transient fallback from unavoidable ordered work.
Older benchmark binaries do not expose these counters; compare only counters present in both
revisions or use an instrumented control.
