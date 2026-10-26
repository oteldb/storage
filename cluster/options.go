package cluster

import (
	"go.opentelemetry.io/otel/trace"

	"github.com/oteldb/storage/internal/obs"
	"github.com/oteldb/storage/query/fetch"
)

// Option configures this package's read/RPC surface — the handlers ([ReadHandler], [SeriesHandler],
// [KeysHandler], [SideHandler], [AggregateHandler], [AggregateWindowHandler]) and remote clients
// ([NewRemoteFetcher], [NewRemoteAggregator], [FetchSeries], [FetchKeys], [FetchSide]). It is
// distinct from [Config] (the cluster membership configuration): Option configures a constructed
// handler/client, not the node's cluster membership.
//
// It exists so an external embedder — who cannot import this module's internal/obs package and so
// cannot name or construct an [*obs.Obs] — has a way to wire a tracer in anyway. Every constructor
// takes Option as a trailing variadic parameter, so existing calls without one keep compiling.
type Option func(*rpcOptions)

// rpcOptions holds the already-resolved observability handle, not a bare provider: [WithTracerProvider]
// resolves it once when the Option is built, so a caller that constructs it once (e.g. at node or
// router startup) and reuses it across many RPCs does not re-resolve a tracer per call.
type rpcOptions struct {
	obs            *obs.Obs
	stringMatchers []fetch.StringSetMatcher
}

// WithTracerProvider reports the handler's or client's spans through tp. Unset (or a nil tp) keeps
// the default no-op tracer, so an embedder who does not configure tracing pays nothing.
func WithTracerProvider(tp trace.TracerProvider) Option {
	o, err := obs.New(obs.Config{TracerProvider: tp})
	if err != nil { // a TracerProvider-only Config never errors; guard rather than propagate a panic
		o = obs.NewNop()
	}

	return func(ro *rpcOptions) { ro.obs = o }
}

// resolveOpts applies opts over the no-op default and returns the resulting observability handle.
// [Option] is exported, so a caller may hand over a slice built elsewhere; a nil entry in it is
// skipped rather than dereferenced.
func resolveOpts(opts []Option) *obs.Obs {
	return resolveRPCOptions(opts).obs
}

func resolveRPCOptions(opts []Option) rpcOptions {
	ro := rpcOptions{obs: obs.NewNop()}

	for _, opt := range opts {
		if opt != nil {
			opt(&ro)
		}
	}

	return ro
}

// WithStringMatchers sends conservative string hints on series and aggregate RPCs.
// Hints never replace the caller's exact matcher recheck and are ignored by other RPCs.
func WithStringMatchers(matchers []fetch.StringSetMatcher) Option {
	return func(ro *rpcOptions) { ro.stringMatchers = matchers }
}
