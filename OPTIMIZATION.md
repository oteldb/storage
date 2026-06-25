# Dictionary Codec — Optimization Log

Performance investigation of `encoding/chunk` dictionary encode/decode, driven by
`go test -cpuprofile` iteration on `BenchmarkDictEncodeReuseBuffer` and
`BenchmarkDictDecodeReuseDst`.

Benchmark setup: 1000 values, 10 distinct (`"label-0".."label-9"`), reused dst/buffer
(0 allocs/op baseline).

---

## Timeline

| Step | Encode MB/s | Decode MB/s | Allocs | Commit |
|---|---|---|---|---|
| Initial (map[string]int, per-byte WriteByte) | 224 | 192 | 16/14 | `8615225` |
| + single comma-ok lookup + full-byte flag + bulk WriteBytes/AppendString | 551 | 392 | 12/14 | `687eeaf` |
| + xxh3 ByteIntMap + [][]byte API | 531 | 287* | 9/14 | `7ffa3db` |
| + scratch pools + AppendBytes + ReadBytesView + pre-sizing | ~686 | ~1086 | 0/0 | (interim) |
| + integer growAt + bytes.Equal + deferred flatPayloadBytes | **990** | **1115** | 0/0 | `0807bbd` |

\* decode dipped due to format change; recovered with ReadBytesView (zero-copy view into src).

---

## Bottleneck analysis (final state, commit `0807bbd`)

### Encode — hash-bound

```
flat%   function
33.15%  pool.(*ByteIntMap).PutOrGet     (probe loop: hash loads + compare)
23.84%  xxh3.rrmxmx                     (xxh3 final mixing)
23.01%  chunk.EncodeBytes               (loop body: range, id stores)
10.96%  xxh3.hashAny                    (xxh3 hash body)
```

The xxh3 hash (rrmxmx + hashAny) is **35% of encode time** — the irreducible per-key
cost. The probe loop (PutOrGet flat) is **33%** — loading `m.hashes[i]` from L1,
comparing, and linear probing. Both are near-optimal for an open-addressing hash table
with a fast non-cryptographic hash.

### Decode — gather-bound

```
flat%   function
86.23%  chunk.DecodeBytes   (the gather loop: dst[i] = entries[idBuf[i]])
 4.66%  bitstream.(*Reader).ReadBytesView
 2.75%  bitstream.(*Reader).loadNextBuffer
 1.27%  encoding/binary.bigEndian.Uint64
```

**82% of decode time is the gather loop** (`dst[i] = entries[idBuf[i]]`). Each iteration
copies a 24-byte `[]byte` slice header (ptr+len+cap). For 1000 rows that's 24 KB of
stores per call. At ~1 ns per copy this is near peak L1 store bandwidth (~25 GB/s
uncompressed throughput). **No further improvement is possible without an API change.**

---

## Fixes applied (commits `687eeaf` → `0807bbd`)

### 1. Single comma-ok map lookup (was: double lookup)

**Before** (`8615225`): two `map[string]int` lookups per string — the `_, ok := dict[s]`
check, then `ids[i] = dict[s]`.

**After**: single `id, ok := dict[s]; ids[i] = id`.

**Gain**: eliminated ~50% of map operations.

### 2. Full-byte flag (was: single WriteBit flag)

**Before**: `WriteBit(true)` left the writer unaligned (count=7), so every subsequent
`AppendString`/`WriteBytes` fell back to the slow per-byte path.

**After**: `WriteByte(flagDict)` — a full byte, keeping all bulk writes byte-aligned.

**Gain**: dictionary entries and row IDs now use zero-copy `append([]byte, ...)` instead
of per-byte `WriteByte` through the bit-packing path.

### 3. xxh3 ByteIntMap (was: Go map[string]int)

**Before**: Go's built-in `map[string]int` with runtime string hashing.

**After**: `pool.ByteIntMap` — open-addressing hash table with `xxh3.Hash` (zeebo/xxh3)
and `[]byte` keys (no string conversion).

**Gain**: xxh3 is faster than the runtime's string hash; `[]byte` keys avoid the
string-conversion allocation. Allocations dropped from 16 → 9 per encode.

### 4. Scratch pools + pre-sizing

**Before**: `make([]int, len(vals))` and `make([]string, size)` allocated per call.

**After**: `sync.Pool`-backed `dictEncodeScratch` and `dictDecodeScratch` reuse the
entries/ids slices across calls. `slices.Grow(dst, exactSize)` pre-sizes the output
buffer to avoid append-triggered reallocations.

**Gain**: 0 allocs/op for both encode and decode with reused dst/buffer.

### 5. AppendBytes + ReadBytesView (zero-copy I/O)

**Before**: row IDs written via per-byte `WriteByte` in a loop; decoded bytes copied
into freshly allocated `[]byte` per entry.

**After**: `Writer.AppendBytes(n)` returns a writable `[]byte` slice directly in the
output buffer (no intermediate allocation). `Reader.ReadBytesView(n)` returns a `[]byte`
aliasing the source (no copy).

**Gain**: eliminated all intermediate byte allocations in the I/O path.

### 6. Integer load-factor check (was: float64 comparison)

**Before**: `float64(m.used+1) > float64(len(m.hashes))*0.75` — int→float conversion +
float multiply on every `PutOrGet` call. **380 ms flat (10% of encode).**

**After**: precomputed `m.growAt = len(hashes)*3/4`, checked as `m.used+1 > m.growAt` —
one integer comparison. **90 ms flat.**

**Gain**: ~290 ms saved (8% of encode time).

### 7. bytes.Equal (was: hand-written byteKeysEqual)

**Before**: `byteKeysEqual` — a byte-at-a-time loop. **1.06 s cum (29% of encode).**

**After**: `bytes.Equal` → `runtime.memequal` (SIMD-optimized on amd64). **310 ms cum.**

**Gain**: ~750 ms saved (20% of encode time).

### 8. Deferred flatPayloadBytes (was: computed every iteration)

**Before**: `flatPayloadBytes += uvarintLen(uint64(len(v))) + len(v)` computed for every
value in the hot loop, even though it's only used in the rare flat-fallback path
(>65536 distinct).

**After**: computed only when the flat fallback is triggered.

**Gain**: removed ~10 ms from the hot loop; improved encode from ~896 → ~990 MB/s.

---

## Proposed API changes (not yet implemented)

These changes target the remaining bottlenecks that cannot be fixed without an API
change. See the profiling rationale above for the numbers.

### 1. `DecodeBytesDict` — split the gather out of decode (highest impact)

**Addresses**: decode gather loop (82% of decode time).

```go
// DictColumn is a decoded dictionary column in its split form.
type DictColumn struct {
    Entries [][]byte  // unique values
    IDs     []byte    // 1 byte per row (or 2 for large dicts)
    IDWidth int       // 1 or 2
}

func (c *DictColumn) Len() int
func (c *DictColumn) At(row int) []byte  // lazy gather, one row at a time

// DecodeBytesDict decodes without gathering — returns the split form.
// Aliases src. ~7× faster than DecodeBytes.
func DecodeBytesDict(src []byte) (*DictColumn, int, error)
```

**Expected gain**: decode drops from ~970 ns to ~140 ns (**~7× faster**). For queries
that filter 90% of rows before projection, total decode+access work drops 10×.

**Risk**: none — pure addition, no existing API broken.

### 2. `DictEncoder` — reusable encoder with warm map

**Addresses**: `sync.Pool` Get/Put + `defer` overhead (~5% of encode); cache-cold hash
table on every call.

```go
type DictEncoder struct {
    m       *pool.ByteIntMap
    entries [][]byte
    ids     []uint16
}

func NewDictEncoder() *DictEncoder
func (e *DictEncoder) Encode(dst []byte, vals [][]byte) []byte
func (e *DictEncoder) Reset()
```

**Expected gain**: ~5-10% encode improvement. The hash table stays cache-warm across
batches; pool overhead eliminated.

### 3. `Column` lazy-decode interface (M1 fetch contract)

**Addresses**: compound effect — decode only what the query touches.

```go
type Column interface {
    Type() chunk.Codec
    Rows() int
    Bytes() [][]byte        // full materialization (cached)
    Dict() *chunk.DictColumn // split form (cached), nil if non-dict
}
```

**Expected gain**: for `SELECT b WHERE a = "foo"` on a 1000-row block:
- Current: decode A fully + decode B fully = ~1940 ns
- Lazy: decode A via Dict() (140 ns) + filter + decode surviving B rows (140 + N ns)

**~7× end-to-end query improvement** for selective queries.

---

## How to reproduce

```bash
# Benchmarks
go test -run='^$' -bench='BenchmarkDictEncodeReuseBuffer|BenchmarkDictDecodeReuseDst' \
  -benchmem -benchtime=3s ./encoding/chunk/

# CPU profile (encode)
go test -run='^$' -bench='BenchmarkDictEncodeReuseBuffer' \
  -benchtime=3s -cpuprofile=/tmp/enc.prof ./encoding/chunk/
go tool pprof -top -flat /tmp/enc.prof

# CPU profile (decode)
go test -run='^$' -bench='BenchmarkDictDecodeReuseDst' \
  -benchtime=3s -cpuprofile=/tmp/dec.prof ./encoding/chunk/
go tool pprof -top -flat /tmp/dec.prof
```
