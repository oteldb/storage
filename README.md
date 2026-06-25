# storage [![Go Reference](https://pkg.go.dev/badge/github.com/oteldb/storage#section-documentation.svg)](https://pkg.go.dev/github.com/oteldb/storage#section-documentation) [![codecov](https://img.shields.io/codecov/c/github/oteldb/storage?label=cover)](https://codecov.io/gh/oteldb/storage) [![experimental](https://img.shields.io/badge/-experimental-blueviolet)](https://go-faster.org/docs/projects/status#experimental)

Low-level, distributed, OpenTelemetry-centric storage **library** (Go) for signals:
- Metrics
- Logs
- Traces
- Profiles

Built as the storage tier for [oteldb](https://github.com/go-faster/oteldb), an OpenTelemetry
observability backend — embeddable as a native Go storage engine for all signals.

Currently in WIP and PoC. See [`DESIGN.md`](DESIGN.md) for the architecture and `PROMPT.md` for
the requirements.