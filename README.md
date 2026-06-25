# storage

Low-level, distributed, OpenTelemetry-centric storage **library** (Go) for signals:
- Metrics
- Logs
- Traces
- Profiles

Built as the storage tier for [oteldb](https://github.com/go-faster/oteldb), an OpenTelemetry
observability backend — embeddable as a native Go storage engine for all signals.

Currently in WIP and PoC. See [`DESIGN.md`](DESIGN.md) for the architecture and `PROMPT.md` for
the requirements.