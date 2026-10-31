# WAL append staging

`BenchmarkAppendWALRegistry` measures one 4,096-point synchronous engine append across
65,536 or 1,048,576 resident series, with the per-series OOO window disabled or set to
1,000. The input identities are one integer-valued `instance` attribute per series.
Registration, initial WAL writes and input construction occur before timing. Timed
calls cycle through disjoint 4,096-series slices of that registry, repeating timestamp
1 and value 1. Existing series must not invoke their identity materializer.

The benchmark uses `RunParallel`, GOMAXPROCS=8 and its default eight worker goroutines.
The WAL is a real file under workspace NVMe, selected with TMPDIR; the Go build cache
keeps its normal location. Per-append sync is disabled and the segment target is 1 GiB
to avoid rotation during timing. Each fixture gets a fresh engine/WAL, six 100ms samples.
There is no conversion, query, maintenance, flush or live resource-quota measurement.

Linux/amd64, AMD Ryzen 9 9950X3D2, Go 1.27.3 (`nodwarf5`), measured 2026-10-31.
Other agents pause builds, tests and benchmarks during each timing set.

| Registry / OOO window | Baseline | Final | Time change |
|---|---:|---:|---:|
| 65,536 / disabled | 649.4 µs | 470.8 µs | -27.50% |
| 65,536 / 1,000 | 763.2 µs | 473.8 µs | -37.92% |
| 1,048,576 / disabled | 1,321.2 µs | 965.6 µs | -26.91% |
| 1,048,576 / 1,000 | 1,361.0 µs | 971.3 µs | -28.63% |

All time comparisons have p=0.002, n=6. The baseline is storage 0439604 plus the
same benchmark source; the final implementation is d683867. `map-cache.txt` records
the intermediate implementation before reader-admitted encoding. The latter preserves
the measured map-lookup benefit; no separate ingestion-speed claim is made for it.

The optimization captures head buffers and prior watermarks while writer ownership
pins them, reducing repeated canonical-ID map probes during admission and apply.
The accumulator grows from 288 to 312 bytes on amd64, per distinct series in the
largest staged append; this scratch persists as before. 4,096 slots imply 96 KiB
extra before capacity rounding. Large direct metric batches can exceed that size;
the library's existing public append is not capped here. Reset clears borrowed series
and cached head references while retaining owned numeric scratch.

Allocation totals are **not** an allocation improvement/regression measurement: the
faster implementation changes adaptive iteration counts and therefore which sample
slice-growth thresholds are crossed. The 65k/OOO-disabled fixture's final B/op is
higher, while its allocations/op are lower. Both 1M fixtures remain about 128 KiB/op
and 8,192 allocations/op, dominated by first expansions of timestamp/value buffers.
Use a fixed sample-buffer growth state for a causal allocation comparison.

The gated encoding regression exercises Append, AppendBatch and ApplyPrimary with
fresh identities and registered identities after flush. Before reader admission all
six cases block; afterward readers return the two already committed points while the
encoder remains blocked. The writer gate still serializes encode/write/publication
against all other writers, seal, sync, checkpoint and Close. Known identities use
immutable views captured under state; no mutable registry map is read off-lock.

Checks: full storage tests, full engine/guard/WAL race tests, staged-vs-direct fuzzing,
failure/replay/ownership and reader ordering regressions, full golangci-lint. No live
10k-capacity improvement is inferred from these native append measurements.

```sh
TMPDIR=/path/on/workspace/nvme GOMAXPROCS=8 go test -p=4 ./engine -run '^$' \
  -bench '^BenchmarkAppendWALRegistry$' -benchtime=100ms -count=6 -cpu=8
benchstat before.txt final.txt
```

Raw selected samples and benchstat are kept beside this document, outside testdata.
