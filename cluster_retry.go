package storage

import (
	"context"
	"net"
	"net/http"
	"time"

	"github.com/go-faster/errors"
	"go.uber.org/zap"

	"github.com/oteldb/storage/internal/retry"
	"github.com/oteldb/storage/query/fetch"
	"github.com/oteldb/storage/reliability"
)

// RPC op labels for retry/hedge metrics and logs.
const (
	rpcOpRead   = "read"
	rpcOpWrite  = "write"
	rpcOpSeries = "series" // record-signal series enumeration
	rpcOpSide   = "side"   // profile symbol-store fetch
	rpcOpKeys   = "keys"   // record-signal attribute-key enumeration
)

// newClusterHTTPClient builds the node-to-node HTTP client. It sets connection-level timeouts so a
// dead peer fails fast instead of hanging, but leaves the overall request unbounded: per-attempt
// deadlines are applied by the retry/hedge layer via context, which composes with hedging (an
// http.Client.Timeout would abort a request the hedge layer still wants to race).
func newClusterHTTPClient(c reliability.RetryConfig) *http.Client {
	dialTimeout := c.PerTryTimeout
	if dialTimeout <= 0 || dialTimeout > 5*time.Second {
		dialTimeout = 5 * time.Second
	}

	return &http.Client{
		Transport: &http.Transport{
			Proxy:                 http.ProxyFromEnvironment,
			DialContext:           (&net.Dialer{Timeout: dialTimeout, KeepAlive: 30 * time.Second}).DialContext,
			ForceAttemptHTTP2:     true,
			MaxIdleConns:          100,
			MaxIdleConnsPerHost:   8,
			IdleConnTimeout:       90 * time.Second,
			TLSHandshakeTimeout:   dialTimeout,
			ExpectContinueTimeout: time.Second,
			ResponseHeaderTimeout: c.PerTryTimeout, // 0 ⇒ unbounded; per-try ctx still bounds it
		},
	}
}

// readPolicy is the hedged-read profile for idempotent cluster fetches: per-attempt timeout, retry
// on transient transport errors, and opportunistic concurrent attempts across replicas. The hooks
// are bound to ctx so retries/hedges are logged trace-correlated and metered.
func (s *Storage) readPolicy(ctx context.Context, op string) retry.Policy {
	c := s.cluster.retry
	p := retry.Policy{
		MaxAttempts:   c.MaxAttempts,
		PerTryTimeout: c.PerTryTimeout,
		BaseBackoff:   c.BaseBackoff,
		MaxBackoff:    c.MaxBackoff,
		HedgeDelay:    c.HedgeDelay,
		Retryable:     retry.Transient,
	}
	s.bindPolicyObs(ctx, op, &p)

	return p
}

// writePolicy is the conservative profile for non-idempotent cluster writes: a per-attempt timeout
// so a stuck primary is abandoned, retries only when the request provably never reached the server
// (so a write is never double-applied), and no hedging (concurrent writes are unsafe).
func (s *Storage) writePolicy(ctx context.Context, op string) retry.Policy {
	c := s.cluster.retry
	p := retry.Policy{
		MaxAttempts:   c.MaxAttempts,
		PerTryTimeout: c.PerTryTimeout,
		BaseBackoff:   c.BaseBackoff,
		MaxBackoff:    c.MaxBackoff,
		Retryable:     retry.ConnFailure,
	}
	s.bindPolicyObs(ctx, op, &p)

	return p
}

// bindPolicyObs attaches the trace-correlated log + metric hooks to a policy for the given op.
func (s *Storage) bindPolicyObs(ctx context.Context, op string, p *retry.Policy) {
	p.OnAttempt = func(int) { s.obs.RPC.Attempt(ctx, op) }
	p.OnRetry = func(attempt int, err error, wait time.Duration) {
		s.obs.RPC.Retry(ctx, op)
		s.obs.Logger(ctx).Debug("rpc retry",
			zap.String("op", op), zap.Int("attempt", attempt), zap.Duration("wait", wait), zap.Error(err))
	}
	p.OnHedge = func(attempt int) {
		s.obs.RPC.Hedge(ctx, op)
		s.obs.Logger(ctx).Debug("rpc hedge", zap.String("op", op), zap.Int("attempt", attempt))
	}
}

// hedgedFetcher races a request across a shard's remote owners under a hedged read policy: the first
// owner is tried immediately, and a second is raced once the first passes the hedge delay or fails —
// first success wins, the rest are canceled. Each owner's copy is complete (replicas), so any single
// success is authoritative. It subsumes a plain sequential failover (HedgeDelay 0).
type hedgedFetcher struct {
	store   *Storage
	op      string
	remotes []fetch.Fetcher
}

func (h hedgedFetcher) Fetch(ctx context.Context, r fetch.Request) (fetch.Iterator, error) {
	if len(h.remotes) == 0 {
		return nil, errors.New("cluster: no reachable owners for tenant")
	}

	if len(h.remotes) == 1 { // single owner: nothing to hedge against, just a bounded retry
		f := h.remotes[0]

		return retry.Do(ctx, h.store.readPolicy(ctx, h.op), func(c context.Context) (fetch.Iterator, error) {
			return f.Fetch(c, r)
		})
	}

	thunks := make([]func(context.Context) (fetch.Iterator, error), len(h.remotes))
	for i := range h.remotes {
		f := h.remotes[i]
		thunks[i] = func(c context.Context) (fetch.Iterator, error) { return f.Fetch(c, r) }
	}

	return retry.Hedge(ctx, h.store.readPolicy(ctx, h.op), thunks)
}
