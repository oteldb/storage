#!/usr/bin/env bash
#
# bench-pprof.sh — a pprof-guided CPU/MEM benchmark loop for the storage read path.
#
# It runs a curated, deterministic set of read/decode benchmarks (the hot paths the pprof
# FINDINGS flagged: fetch → chunk decode → bitstream), capturing for each labeled run:
#   - <label>.bench.txt   benchstat-comparable timings + allocs (B/op, allocs/op) for all benches
#   - <label>.cpu.prof    CPU profile of the realistic read path (BenchmarkGolden/read)
#   - <label>.mem.prof    allocation profile of the same
#   - <label>.cpu.top.txt / <label>.mem.top.txt   `go tool pprof -top` of each (what to fix next)
#
# Usage:
#   scripts/bench-pprof.sh run  <label>          # benchmark + profile, save under .bench/<label>.*
#   scripts/bench-pprof.sh cmp  <base> <head>    # benchstat .bench/<base> vs .bench/<head>
#   scripts/bench-pprof.sh top  <label> [cpu|mem]# reprint the saved pprof top
#
# Typical loop:
#   scripts/bench-pprof.sh run baseline          # before a change
#   ... edit hot path ...
#   scripts/bench-pprof.sh run change            # after
#   scripts/bench-pprof.sh cmp baseline change    # did it help? regress?
#
# Env knobs: COUNT (default 8, for benchstat significance), BENCHTIME (default 2s).
set -euo pipefail

cd "$(dirname "$0")/.."

OUT=.bench
COUNT="${COUNT:-8}"
BENCHTIME="${BENCHTIME:-2s}"

# The benchstat set: the realistic read path (root) plus the decode micro-benchmarks it bottoms out
# in. -run '^$' skips tests; -benchmem reports B/op and allocs/op (the MEM axis).
bench_set() {
  go test -run '^$' -bench '^BenchmarkGolden$/read' -benchmem -count="$COUNT" -benchtime="$BENCHTIME" .
  # The release-aware read (realistic embedder pattern) — measures Batch.Release buffer pooling.
  go test -run '^$' -bench '^(BenchmarkDoDDecode|BenchmarkGorillaDecode|BenchmarkDecimalDecode|BenchmarkT64Decode|BenchmarkU128Decode)$' \
    -benchmem -count="$COUNT" -benchtime="$BENCHTIME" ./encoding/chunk
  go test -run '^$' -bench '^(BenchmarkReadBit|BenchmarkReadBits|BenchmarkReadBitsUnaligned)$' \
    -benchmem -count="$COUNT" -benchtime="$BENCHTIME" ./encoding/bitstream
}

cmd_run() {
  local label="$1"
  mkdir -p "$OUT"

  echo ">> benchstat set → $OUT/$label.bench.txt"
  bench_set | tee "$OUT/$label.bench.txt"

  # Profile only the realistic read path so the profile is the decode/merge/GC the FINDINGS care
  # about (not the write benches). Profiling needs the compiled test binary kept (-o).
  echo ">> profiling read path → $OUT/$label.{cpu,mem}.prof"
  go test -run '^$' -bench '^BenchmarkGolden$/read' -benchtime="$BENCHTIME" \
    -cpuprofile "$OUT/$label.cpu.prof" -memprofile "$OUT/$label.mem.prof" \
    -o "$OUT/$label.test" . >/dev/null

  go tool pprof -top -nodecount=25 "$OUT/$label.cpu.prof" >"$OUT/$label.cpu.top.txt" 2>/dev/null || true
  go tool pprof -top -nodecount=25 -sample_index=alloc_space "$OUT/$label.mem.prof" >"$OUT/$label.mem.top.txt" 2>/dev/null || true

  echo ">> CPU top:"; sed -n '1,18p' "$OUT/$label.cpu.top.txt"
  echo ">> MEM (alloc_space) top:"; sed -n '1,18p' "$OUT/$label.mem.top.txt"
}

cmd_cmp() {
  local base="$1" head="$2"
  benchstat "$OUT/$base.bench.txt" "$OUT/$head.bench.txt"
}

cmd_top() {
  local label="$1" which="${2:-cpu}"
  cat "$OUT/$label.$which.top.txt"
}

case "${1:-}" in
  run) shift; cmd_run "$@";;
  cmp) shift; cmd_cmp "$@";;
  top) shift; cmd_top "$@";;
  *) sed -n '2,30p' "$0"; exit 1;;
esac
