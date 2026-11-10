# CPU-metric storage benchmarks

The default fixture has 10,000 hosts × eight CPU modes: **80,000 series**. It contains one
`node_cpu_seconds_total` metric, not the full node-exporter dashboard workload. Samples use a
15-second cadence; storage uses the native file backend and a WAL. Disk indexes are opt-in.

| Benchmark | What ns/op, B/op and allocs/op measure |
|---|---|
| `BenchmarkDiskSeriesCPUInputGeneration` | Build identities, hash IDs and generate one generation's sample vectors. No storage. |
| `BenchmarkDiskSeriesCPUStorage/append-cold` | Append one generation into a fresh engine. |
| `BenchmarkDiskSeriesCPUStorage/append-head-warm` | Append the next generation after all series were seeded into the resident head. |
| `BenchmarkDiskSeriesCPUStorage/append-after-flush` | Append after all series were seeded and flushed. In disk-index mode the resident registry is empty and the historical cache is warm. |
| `BenchmarkDiskSeriesCPUStorage/flush` | Flush a prepared head. Appending the input is excluded. |
| `BenchmarkDiskSeriesCPUIngest` | Historical end-to-end fixture: input generation, setup, ingestion and all generation flushes. Its custom append/flush seconds isolate calls, but standard bytes/allocations include generation. |
| `BenchmarkDiskSeriesCPUMerge` | Compact a controlled four-part fixture. Generation, setup and input part writing are excluded. |

The storage-phase benchmark generates immutable inputs, computes IDs, creates the file/WAL
backend, performs any required seeding of the full requested series set and cleans up **outside** its timer/allocation
counters. Its `samples/op` and `series/op` state the work per measured operation. Each iteration
uses a fresh engine, after untimed GC drains setup/seed garbage. Input vectors remain resident
during the operation. Reported MB/s counts logical timestamp/value bytes, not bytes written to disk. `append-head-warm` and `append-after-flush` are different states; neither
implies that a live full node-exporter workload has reached steady state.

Use `-benchtime=1x` for these large fixtures. Environment controls shared with the historical
benchmarks are `OTELDB_BENCH_CPU_HOSTS`, `OTELDB_BENCH_CPU_POINTS`,
`OTELDB_BENCH_DISK_SERIES`, `OTELDB_BENCH_WAL_SYNC` and `OTELDB_BENCH_CPU_READ_CACHE`.
`OTELDB_BENCH_CPU_GENERATIONS` and `OTELDB_BENCH_CPU_REOPEN` describe the historical/query fixtures; each storage-phase
case measures one generation with its documented seed state. Head-warm and flush cases disable
automatic resident-head pressure for their bounded input so seeding cannot silently flush part of
the head; cold and after-flush append cases retain normal pressure handling.

## Run

Use a disk-backed temporary directory. `/tmp` can be tmpfs, which changes WAL/file timing.
Go removes all fixture data after each case; profiles are retained only when requested.

```sh
mkdir -p "$PWD/.bench-tmp"
TMPDIR="$PWD/.bench-tmp" GOMAXPROCS=8 \
  OTELDB_BENCH_DISK_SERIES=true OTELDB_BENCH_CPU_READ_CACHE=true \
  go test ./engine -run '^$' -bench '^BenchmarkDiskSeriesCPUStorage/append-after-flush$' \
    -benchtime=1x -benchmem -count=6
rmdir "$PWD/.bench-tmp"
```

## Profile storage only

Go's `-cpuprofile` profiles the whole test process, including untimed fixture generation and
setup. Stopping the benchmark timer does not stop that profiler. For a storage-only capture,
select a single case and use the existing scoped profile option instead. Create the profile
output directory first; `-o` retains the matching test binary:

```sh
mkdir -p "$PWD/.bench-tmp"
TMPDIR="$PWD/.bench-tmp" GOMAXPROCS=8 \
  OTELDB_BENCH_DISK_SERIES=true OTELDB_BENCH_CPU_READ_CACHE=true \
  OTELDB_BENCH_CPU_PROFILE=/path/to/retained/profiles/ingest \
  go test -o /path/to/retained/profiles/engine.test ./engine -run '^$' -bench '^BenchmarkDiskSeriesCPUStorage/append-after-flush$' \
    -benchtime=1x -count=1
rmdir "$PWD/.bench-tmp"
```

It writes `ingest.append-after-flush.cpu`, `.allocs.before`, `.allocs.after` and `.heap.after`.
Only the first measured operation is profiled; input generation, setup, seeding and cleanup
are outside the capture window. Allocation snapshots include profiler overhead; profile runs
are separate from unprofiled timing comparisons.

```sh
go tool pprof -http=127.0.0.1:8085 /path/to/retained/profiles/engine.test \
  /path/to/retained/profiles/ingest.append-after-flush.cpu
go tool pprof -alloc_space \
  -base=/path/to/retained/profiles/ingest.append-after-flush.allocs.before \
  /path/to/retained/profiles/ingest.append-after-flush.allocs.after
```

The phase benchmark also tags CPU samples with `phase` (`input-generation`, `engine-setup`,
`seed`, `storage`, `cleanup`) and `operation`. These labels help navigate a global CPU profile;
CPU labels do not filter heap/allocation profiles and may omit separately created background
workers. Prefer scoped captures for complete operation-window profiles.
