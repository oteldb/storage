package storage

import (
	"context"
	"net"
	"net/http"
	"sync/atomic"
	"time"

	"github.com/go-faster/errors"
	"go.uber.org/zap"

	"github.com/oteldb/storage/cluster"
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
	rpcOpValues = "values" // record-signal distinct column-value enumeration
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
//
// An owner answering [cluster.ErrShardAbsent] (the ring points at it, but it holds no data for the
// shard) is a failover, not a result: only when every owner disclaims the shard does the fetch yield
// an empty iterator.
type hedgedFetcher struct {
	store   *Storage
	op      string
	remotes []fetch.Fetcher

	// absentShard, when set, is the shard this node owns per the ring but holds no data for — the
	// reason the fan-out exists at all. Reported here, where a request context finally exists.
	absentShard *absentShard
}

func (h hedgedFetcher) Fetch(ctx context.Context, r fetch.Request) (fetch.Iterator, error) {
	if len(h.remotes) == 0 {
		return nil, errors.New("cluster: no reachable owners for tenant")
	}

	if a := h.absentShard; a != nil {
		h.store.reportAbsentShard(ctx, h.op, a.sig, a.shard, len(h.remotes))
	}

	var absent atomic.Int64

	attempt := func(f fetch.Fetcher) func(context.Context) (fetch.Iterator, error) {
		return func(c context.Context) (fetch.Iterator, error) {
			it, err := f.Fetch(c, r)
			if errors.Is(err, cluster.ErrShardAbsent) {
				absent.Add(1)
			}

			return it, err
		}
	}

	var (
		it  fetch.Iterator
		err error
	)

	if len(h.remotes) == 1 { // single owner: nothing to hedge against, just a bounded retry
		p := h.store.readPolicy(ctx, h.op)
		// The lone owner's "I don't hold this shard" is final — retrying it only burns the budget.
		p.Retryable = func(err error) bool {
			return !errors.Is(err, cluster.ErrShardAbsent) && retry.Transient(err)
		}

		it, err = retry.Do(ctx, p, attempt(h.remotes[0]))
	} else {
		thunks := make([]func(context.Context) (fetch.Iterator, error), len(h.remotes))
		for i := range h.remotes {
			thunks[i] = attempt(h.remotes[i])
		}

		it, err = retry.Hedge(ctx, h.store.readPolicy(ctx, h.op), thunks)
	}

	if err != nil && int(absent.Load()) >= len(h.remotes) {
		h.store.obs.RPC.ShardAbsent(ctx, h.op)

		return fetch.NewSliceIterator(nil), nil
	}

	return it, err
}
