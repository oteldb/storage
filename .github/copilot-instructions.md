# Rules for Large Language Models (LLMs)

[github](https://docs.github.com/en/copilot/how-tos/configure-custom-instructions/add-repository-instructions?tool=jetbrains#about-repository-custom-instructions-for-copilot-3)

`CLAUDE.md` and `AGENTS.md` are symlinks to this file. Edit this file to change them all.

## What this is

`github.com/oteldb/storage` — a low-level, distributed, OpenTelemetry-centric storage
**library** (not a binary) for signals: metrics, logs, traces, profiles. Greenfield, Go 1.26.

It is built as the storage tier for **oteldb** (`github.com/go-faster/oteldb`), an OpenTelemetry
observability backend by the same authors (today ClickHouse-backed). oteldb is the primary
consumer — it must embed this library as a native Go storage engine — so its data model, query
needs (PromQL/LogQL/TraceQL), and house conventions (go-faster, ch-go) are the concrete target.
See `_ref/docs/oteldb.md` for the embedder analysis.

Read these before designing or implementing:
- **`PROMPT.md`** — the requirements (goals, performance targets, features). The source of truth
  for *what* to build.
- **`DESIGN.md`** — the architecture of record: layers, package layout, the fetch contract, write/
  read paths, the milestone plan (M0–M7). The source of truth for *how* (forward-looking).
- **`ARCHITECTURE.md`** — the architecture *as currently built* (no roadmap/TODOs). The source of
  truth for *what exists today*. **Keep it current:** any architectural change — a new package or
  layer, a new public type/interface, a new or changed on-disk/wire format or codec framing, a moved
  layer boundary, or a new cross-cutting invariant — must update `ARCHITECTURE.md` in the same change.
- **`_ref/docs/`** — analyses of 13 reference systems + two synthesis docs
  (`storage-engine.md`, `query-languages.md`), indexed by `_ref/docs/README.md`. The prior art;
  cite it when justifying a design choice. The `_ref/` source trees are reference only — never
  edit, build, or import from them.

When `PROMPT.md`, `DESIGN.md`, and the code disagree, surface it rather than silently picking one.

**`PROMPT.md`, `DESIGN.md`, and `_ref/` are gitignored — not committed to the repo.** Use them as
local guidance, but **never cite them from committed code** (no `DESIGN.md §6` / `_ref/docs/…` in
source comments): those references dangle for anyone who clones the repo. Write self-contained
comments instead, and reference `ARCHITECTURE.md` (which is committed) when a pointer is warranted.

## Working agreement

- **Library, not a binary.** No `main`, no server/HTTP/gRPC, no CLI, no auth, no scraping. The
  embedder (e.g. the parent `go-faster/oteldb`) owns the process and transport. Public surface is
  the small `Storage` facade in `DESIGN.md` §5; keep everything else internal.
- **Follow the milestone order** in `DESIGN.md` §14. Don't build a layer before the one it
  depends on exists and is tested. Current target: M0 (encoding foundations), metrics vertical first.
- **Match the surrounding code** — its naming, comment density, and idioms. Don't introduce a new
  style or dependency without reason.

## Go conventions

- **Errors:** use `github.com/go-faster/errors` (the `go-faster-errors` skill governs this). Key
  rule: `errors.Wrap(nil, …)` returns non-nil — wrap only inside a non-nil check.
- **JSON / wire decode:** use `github.com/go-faster/jx` (the `jx` skill governs buffer safety /
  pooling) where hand-written encode/decode is needed.
- **Dependencies are minimal and deliberate:** go-faster libs, an etcd client, an S3 SDK, and the
  OTel pdata types at the API boundary. Discuss before adding anything else.
- **Zero-alloc hot paths** (this is a hard requirement, not a nicety):
  - append-style APIs: `func(dst []byte, …) []byte`; callers own/reuse buffers.
  - **`[]byte`, not `string`, for keys/values/identity in the data model** (e.g. `signal.Value`,
    `KeyValue.Key`): OTLP decode buffers and the interning map (`pool.ByteIntMap`) are byte-based, so
    `[]byte` avoids the string-conversion allocation. Convert to `string` only at display boundaries.
  - `sync.Pool` + `Reset()` for parsers, blocks, iterators, result rows; arenas for same-lifetime
    batches; intern label strings and compare by id.
  - lazy column decode; never materialize columns a query doesn't reference.
  - `unsafe` string↔[]byte only where the lifetime is provably bounded — and fuzz it.
- **Identity is normalized out of value streams** (SeriesID + interned symbols); value columns hold
  only ids/timestamps/values.
- **Generate repetitive low-level code** (`DESIGN.md` §10a): per-type columnar read/write,
  specialized codecs, and SIMD/assembly kernels (via `avo`, always with a pure-Go fallback behind
  arch build tags). ch-go is the precedent (`_ref/docs/ch-go.md`). Generators live in
  `internal/cmd/gen*/`, wired with `//go:generate`; `go generate ./...` regenerates. Generated
  files are `*_gen.go`, committed, carry `// Code generated … DO NOT EDIT.`, and are **never
  hand-edited** — change the template/generator. CI fails if `go generate` leaves the tree dirty.

## Testing (non-negotiable, from PROMPT.md)

- **≥90% coverage from the start.** New packages ship with tests in the same change.
- **Fuzz** every codec/parser/format: `encode∘decode == identity`, WAL framing, manifest parsing,
  matcher/PromQL parsers. Build and fuzz `encoding/bitstream` before anything depends on it.
- **Property-based tests** for invariants: codec round-trips, merge associativity/idempotence,
  postings set-ops vs a naive reference, fetch returns a superset of a brute-force scan,
  lossless exp-histogram downsampling.
- **Golden tests** for on-disk formats (parts, WAL) to catch accidental format breaks.
- **Benchmarks on hot paths** (bit-stream, merge, postings intersection, fetch); use benchmarks +
  pprof as the performance feedback loop — measure before and after hot-path changes.
- **Assertions:** use `github.com/stretchr/testify` (`require` for must-stop checks — errors,
  preconditions; `assert` for independent value checks). Prefer it for new and converted tests over
  hand-written `if got != want { t.Fatalf(...) }`. Fuzz/benchmark bodies stay testify-free where it
  would add overhead to the hot loop.
- Run `go test ./...` and `go vet ./...`; keep the tree green and formatted (`gofmt`/`goimports`).

## Architecture invariants to preserve

- **One physical engine, many front-ends.** Query languages and signals are thin layers over the
  shared columnar engine and the **dual-shape fetch contract** (label matchers ∪ column conditions
  → lazy iterator, `DESIGN.md` §7). Condition extraction lives in each language package, never in
  storage. Don't leak a language's concepts into the storage layer.
- **Immutable parts + one merge engine.** Compaction, retention, downsampling, and recompression
  are the same background-merge code. Don't add a parallel subsystem for any of them.
- **Backends are interchangeable** behind `backend.Backend` (memory/file/s3 share the code path).
  The **in-memory engine** (`backend.Memory()` + `Durability=Ephemeral`, exposed as
  `storage.InMemory(...)`) is first-class: a full ingest+query path with WAL/flush disabled, the
  reference backend, and the default in tests. Every layer must work in-memory with no disk/S3.
- **Coordination is external/minimal:** etcd for ring/leases/rebalance, backend CAS for manifest
  commits. No homegrown Raft. Single-node mode must work with the cluster layer absent.
- **Policy via callbacks:** multi-tenancy, retention, downsampling, and limits resolve through
  consumer-supplied callbacks keyed by tenant id. Two specifics:
  - **Tenant id is derived, not passed:** a `func(Resource, Scope) → TenantID` callback routes each
    ingested record to a tenant from its resource/scope (one batch may span tenants); `tenant.Resolver`
    maps that id → policy. The `Write*` facade methods take no tenant argument.
  - **Matchers are callbacks, not operator enums:** at every layer (`index/postings` and the `fetch`
    contract) a label matcher carries a `Match(value) bool` predicate over the typed value — never an
    `=/!=/=~/!~` enum. Languages supply the predicate and compose negation/absent; storage and the
    fetch seam stay operator-free.
