# Sealed-head streaming flush

`BenchmarkFlushSealedHead` compares the former flat-column flush with bounded sorted
identity runs streamed through the common part encoder. The fixture pre-registers
65,536 two-label series, reuses immutable input buffers, writes real file parts with
a 64 MiB raw-row cap, and records a 448 MiB additional-workspace reservation target.
Setup and replacing the fixture's head are outside timing. There is no WAL append,
receiver conversion, concurrent writer/query/merge, or live CPU/memory quota here.

Six alternating baseline/current pairs, Go 1.27.3 (`nodwarf5`), Linux/amd64,
Ryzen 9 9950X3D2, GOMAXPROCS=8:

| Points per series | Before B/op | After B/op | Change | Before / after time | Parts |
|---|---:|---:|---:|---:|---:|
| 1 | 73.32 MiB | 64.79 MiB | -11.63% | 50.71 / 52.11 ms | 1 / 1 |
| 8 | 114.75 MiB | 79.66 MiB | -30.58% | 69.31 / 71.47 ms | 1 / 1 |
| 128 | 830.9 MiB | 321.0 MiB | -61.37% | 349.1 / 327.3 ms | 4 / 4 |

All byte reductions have p=0.002. Only the long flush's time change is significant
(-6.26%, p=0.002); shorter timings are statistically unchanged. Allocation counts
increase by 1.13%, 4.27% and 43.36% respectively. Fewer bytes is not a claim of fewer
objects, improved live GC, or sustainable ingest capacity.

Requested admission is a fixed 448 MiB in all new cases versus approximately
74.7, 186.7 and 2,106.7 MiB previously. This is policy reservation, not measured peak
RSS. Reserving the full unit can admit fewer concurrent small flushes. Frozen input
samples remain in head/in-flight accounting; the workspace pool does not bound them.
Huge individual unordered series or identities can still require oversized exclusive
reservations. Whole-object backends may seal earlier and make more output parts;
completed output handles remain resident until complete-generation publication.

The facade supplies one quarter of the shared maintenance pool as the metric flush
target, leaving the other three quarters available to merges. A deterministic
native-file regression holds that merge share and flushes 524,288 ordered points:
it fails on the former implementation and passes with bounded encoding and the
facade wiring. No timeout or real-time sleep drives that assertion.

Raw before/after samples and benchstat are committed separately beside this file.
The full-storage suite, targeted race checks, and repository lint pass. Native API
publication/reopen checks also exercise a 4,097-point series across encoding chunks.
