// Package partsync replicates flushed, immutable parts between nodes whose backends are
// per-node private (shared-nothing cluster mode). Head replication (cluster/replica) protects
// only the unflushed window; over a shared object store the flushed parts need no replication
// at all — but with a local-disk backend a peer cannot see them, so a replica instead
// *mirrors* the owner's backend objects over HTTP: it picks the newest peer copy of the
// engine's bucket index, copies the part objects it lacks, and installs the index last. The
// engine then reconciles via its ordinary LoadParts/RefreshReplica path — partsync moves
// backend objects, never engine state.
//
// Ordering makes a crashed sync harmless: within a part the manifest is copied after the
// part's other objects, and the bucket index is written after every part, so the local index
// only ever references fully-copied parts (the same commit-point discipline flush uses). A
// half-copied part is an unreferenced orphan retried on the next pass.
//
// Objects are content-immutable except the bucket index and the record engines' whole-set stream
// identity object (streams.bin, plus the metrics series.bin a prefix written before identity became
// part-scoped still has), so a plain presence diff drives the copy; the mutable objects are
// re-fetched whenever the index changed. Every fetched object is verified against the
// sender's checksum. Local objects the peer no longer has are pruned only after being absent
// for two consecutive passes, giving in-flight readers a full maintenance cycle to drain
// (quarantine-by-delay rather than immediate delete).
package partsync

import (
	"bytes"
	"context"
	"encoding/binary"
	"io"
	"net/http"
	"net/url"
	"path"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/go-faster/errors"
	"github.com/go-faster/sdk/zctx"
	"github.com/zeebo/xxh3"
	"go.uber.org/zap"

	"github.com/oteldb/storage/backend"
	"github.com/oteldb/storage/backend/bucketindex"
	"github.com/oteldb/storage/internal/obs"
)

const (
	httpScheme = "http"

	// ListPath is the HTTP path serving a node's backend key listing under a prefix.
	ListPath = "/internal/parts/list"
	// ObjectPath is the HTTP path serving one backend object verbatim.
	ObjectPath = "/internal/parts/object"
	// NotifyPath is the HTTP path an owner POSTs to after a flush/merge so a secondary mirrors
	// immediately instead of waiting for its next maintenance tick. Advisory and best-effort —
	// the periodic pull remains the anti-entropy source of truth.
	NotifyPath = "/internal/parts/notify"

	// checksumHeader carries the xxh3 hash of the object body, verified by the client.
	checksumHeader = "X-Checksum-Xxh3"

	// pruneAfterMisses is how many consecutive sync passes a local object must be absent from
	// the peer before it is deleted.
	pruneAfterMisses = 2

	// errBodyLimit bounds how much of a peer's error response is read into an error message, so
	// a peer answering with a large body cannot blow up the caller's log line.
	errBodyLimit = 4 << 10
)

// ValidKey reports whether a remotely-supplied key or prefix is safe to hand to a backend:
// relative, slash-delimited, and free of traversal or NUL. Backends validate again (the file
// backend keeps every path under its root); this check is defense-in-depth at every network
// boundary — the serving handlers reject hostile request parameters, and the syncer rejects
// hostile key names a compromised peer could return.
func ValidKey(k string) bool {
	return !strings.Contains(k, "..") && !strings.HasPrefix(k, "/") &&
		!strings.ContainsAny(k, "\\\x00")
}

// ListHandler serves the backend keys under the "prefix" query parameter, framed as a uvarint
// count followed by uvarint-length-prefixed keys.
func ListHandler(be backend.Backend) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		prefix := req.URL.Query().Get("prefix")
		if !ValidKey(prefix) {
			http.Error(w, "invalid prefix", http.StatusBadRequest)

			return
		}

		keys, err := be.List(req.Context(), prefix)
		if err != nil {
			zctx.From(req.Context()).Error("partsync list failed",
				zap.String("prefix", prefix), zap.Error(err))
			http.Error(w, "list: "+err.Error(), http.StatusInternalServerError)

			return
		}

		buf := binary.AppendUvarint(nil, uint64(len(keys)))
		for _, k := range keys {
			buf = binary.AppendUvarint(buf, uint64(len(k)))
			buf = append(buf, k...)
		}

		w.Header().Set("Content-Type", "application/octet-stream")
		_, _ = w.Write(buf) //nolint:gosec // G705: binary framing on an internal octet-stream endpoint, no HTML sink
	})
}

// ObjectHandler serves one backend object (the "key" query parameter) verbatim, with its xxh3
// checksum in a response header. A missing key is a 404.
func ObjectHandler(be backend.Backend) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		key := req.URL.Query().Get("key")
		if key == "" || !ValidKey(key) {
			http.Error(w, "invalid key", http.StatusBadRequest)

			return
		}

		data, err := backend.ReadView(req.Context(), be, key)
		if err != nil {
			if errors.Is(err, backend.ErrNotExist) {
				http.Error(w, "no such object", http.StatusNotFound)

				return
			}

			zctx.From(req.Context()).Error("partsync object read failed",
				zap.String("key", key), zap.Error(err))
			http.Error(w, "read: "+err.Error(), http.StatusInternalServerError)

			return
		}

		w.Header().Set("Content-Type", "application/octet-stream")
		w.Header().Set(checksumHeader, strconv.FormatUint(xxh3.Hash(data), 16))
		_, _ = w.Write(data) //nolint:gosec // G705: raw object bytes on an internal octet-stream endpoint, no HTML sink
	})
}

// ErrNotExist is returned by [Client.Fetch] for a key the peer does not have.
var ErrNotExist = errors.New("partsync: object does not exist on peer")

// Client fetches backend listings and objects from a peer's partsync endpoints.
type Client struct {
	// HTTP is the client used for peer requests; nil uses [http.DefaultClient]. Pass one with
	// timeouts in production (the cluster's tuned client).
	HTTP *http.Client
}

// List returns the peer's backend keys under prefix.
func (c *Client) List(ctx context.Context, addr, prefix string) ([]string, error) {
	resp, err := c.get(ctx, addr, ListPath, url.Values{"prefix": []string{prefix}})
	if err != nil {
		return nil, err
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK {
		return nil, peerError(addr, "list", resp)
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, errors.Wrap(err, "read list body")
	}

	keys, err := decodeKeyList(body)
	if err != nil {
		return nil, errors.Wrapf(err, "decode list from %q", addr)
	}

	return keys, nil
}

// Fetch returns one object from the peer, verified against the sender's checksum.
// A key the peer lacks returns [ErrNotExist].
func (c *Client) Fetch(ctx context.Context, addr, key string) ([]byte, error) {
	resp, err := c.get(ctx, addr, ObjectPath, url.Values{"key": []string{key}})
	if err != nil {
		return nil, err
	}
	defer func() { _ = resp.Body.Close() }()

	switch resp.StatusCode {
	case http.StatusOK:
	case http.StatusNotFound:
		return nil, errors.Wrapf(ErrNotExist, "%q on %q", key, addr)
	default:
		return nil, peerError(addr, "fetch "+strconv.Quote(key), resp)
	}

	data, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, errors.Wrapf(err, "read object %q", key)
	}

	if want := resp.Header.Get(checksumHeader); want != "" {
		if got := strconv.FormatUint(xxh3.Hash(data), 16); got != want {
			return nil, errors.Errorf("object %q from %q: checksum mismatch (got %s want %s)", key, addr, got, want)
		}
	}

	return data, nil
}

// Notify tells the peer at addr that enginePrefix has new flushed parts, so it can mirror
// immediately. Fire-and-forget semantics: an error just means the peer will catch up on its
// next maintenance tick.
func (c *Client) Notify(ctx context.Context, addr, enginePrefix string) error {
	u := (&url.URL{Scheme: httpScheme, Host: addr}).JoinPath(NotifyPath)
	u.RawQuery = url.Values{"prefix": []string{enginePrefix}}.Encode()

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, u.String(), http.NoBody)
	if err != nil {
		return errors.Wrap(err, "build request")
	}

	obs.InjectHTTP(ctx, req.Header)

	resp, err := c.http().Do(req)
	if err != nil {
		return errors.Wrapf(err, "notify %q", addr)
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusAccepted {
		return peerError(addr, "notify", resp)
	}

	return nil
}

// peerError turns a peer's non-2xx response into an error carrying a bounded prefix of the
// response body, which is where the peer's handler put its own failure reason.
func peerError(addr, what string, resp *http.Response) error {
	body, _ := io.ReadAll(io.LimitReader(resp.Body, errBodyLimit))

	if body = bytes.TrimSpace(body); len(body) == 0 {
		return errors.Errorf("%s: %q returned %d", what, addr, resp.StatusCode)
	}

	return errors.Errorf("%s: %q returned %d: %s", what, addr, resp.StatusCode, body)
}

func (c *Client) http() *http.Client {
	if c.HTTP != nil {
		return c.HTTP
	}

	return http.DefaultClient
}

func (c *Client) get(ctx context.Context, addr, p string, q url.Values) (*http.Response, error) {
	u := (&url.URL{Scheme: httpScheme, Host: addr}).JoinPath(p)
	u.RawQuery = q.Encode()

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u.String(), http.NoBody)
	if err != nil {
		return nil, errors.Wrap(err, "build request")
	}

	obs.InjectHTTP(ctx, req.Header)

	resp, err := c.http().Do(req)
	if err != nil {
		return nil, errors.Wrapf(err, "get %q from %q", p, addr)
	}

	return resp, nil
}

// decodeKeyList parses the ListHandler framing, defensively against truncated input.
func decodeKeyList(data []byte) ([]string, error) {
	count, n := binary.Uvarint(data)
	if n <= 0 {
		return nil, errors.New("key count")
	}
	data = data[n:]

	// Cap the allocation by what the payload could actually hold (1 byte per key minimum).
	keys := make([]string, 0, int(min(count, uint64(len(data)))))

	for range count {
		l, n := binary.Uvarint(data)
		if n <= 0 || l > uint64(len(data)-n) {
			return nil, errors.New("key length")
		}
		data = data[n:]

		keys = append(keys, string(data[:l]))
		data = data[l:]
	}

	return keys, nil
}

// KeepFunc decides whether a peer-listed object key should be mirrored into this node. It lets
// the caller narrow a pull to a subset of a part's objects — erasure coding passes one that
// keeps only this node's own shard slot (plus every non-shard object), so a replica stores one
// shard per part instead of the whole k+m set. A nil KeepFunc keeps everything.
type KeepFunc func(key string) bool

// keepAll is the default: mirror every object.
func keepAll(string) bool { return true }

// Stats reports what one [Syncer.Sync] pass did.
type Stats struct {
	// Synced is true when a newer peer copy was found and mirrored (Copied may still be zero
	// if only the mutable objects changed).
	Synced bool
	// Copied is the number of objects fetched from the peer.
	Copied int
	// CopiedBytes is the total size of the fetched objects.
	CopiedBytes int64
	// Pruned is the number of stale local objects deleted.
	Pruned int
	// Withheld is the number of local objects a prune declined to delete because nothing
	// authorized it: their part is absent from the peer's index and the peer did not say it
	// removed one. That is a peer missing data it should hold, so a non-zero value is a repair
	// signal, not a tuning knob.
	Withheld int
}

// Totals is a Syncer's cumulative activity across every prefix and pass, for the operator
// stats surface (storage.StoreStats). Counters only — reading it does no I/O.
type Totals struct {
	// Passes is every Sync attempt, including no-ops (no usable peer index, nothing newer) —
	// the "is the sync loop running?" liveness probe.
	Passes int64
	// Mirrored is the passes that installed a newer peer copy.
	Mirrored int64
	// Copied is the objects fetched from peers, CopiedBytes their total size.
	Copied      int64
	CopiedBytes int64
	// Pruned is the stale local objects deleted (after the quarantine delay).
	Pruned int64
	// Withheld is the local objects a prune declined to delete because no peer claimed to have
	// removed their part — a peer missing data it should hold. It is a repair signal: steady
	// state is zero, and a value that keeps climbing names a damaged owner.
	Withheld int64
	// Errors is the passes that failed part-way (retried by the next maintenance tick).
	Errors int64
	// LastSyncUnixNano is the wall-clock completion time of the most recent mirroring pass
	// (zero until one succeeds) — the "is replication current?" staleness probe.
	LastSyncUnixNano int64
}

// Syncer mirrors engine prefixes of a per-node private backend from cluster peers. Safe for
// concurrent use across distinct prefixes; per-prefix passes are expected to be serial (the
// maintenance loop runs one task per engine).
type Syncer struct {
	local  backend.Backend
	client *Client

	mu sync.Mutex
	// state is the per-engine-prefix prune bookkeeping.
	state map[string]*prefixState
	// totals is the cumulative activity across every prefix and pass.
	totals Totals
}

// Totals returns a snapshot of the Syncer's cumulative activity.
func (s *Syncer) Totals() Totals {
	s.mu.Lock()
	defer s.mu.Unlock()

	return s.totals
}

// prefixState is the per-engine-prefix sync state: a pass-serialization lock (a notify-driven
// sync may race the maintenance-loop sync on the same prefix; racing installs could put an
// older index over a newer one), the peer's key set from the last pass, and — per local key —
// how many consecutive passes it has been absent from the peer.
type prefixState struct {
	pass   sync.Mutex // serializes Sync passes for this prefix
	remote map[string]struct{}
	miss   map[string]int
}

// New returns a Syncer mirroring into local via client.
func New(local backend.Backend, client *Client) *Syncer {
	return &Syncer{local: local, client: client, state: make(map[string]*prefixState)}
}

// Sync mirrors one engine prefix (e.g. "default/metrics") from the newest of peers into the
// local backend. In strict mode (an owner backfilling before it compacts) the peer copy must be
// strictly newer than the local one; otherwise (a replica mirroring its owner) any differing
// peer copy at least as new is installed. Unreachable peers are skipped; having no usable peer
// index is a no-op, not an error.
func (s *Syncer) Sync(ctx context.Context, enginePrefix string, peers []string, strict bool, keep KeepFunc) (Stats, error) {
	if enginePrefix == "" || !ValidKey(enginePrefix) {
		return Stats{}, errors.Errorf("invalid engine prefix %q", enginePrefix)
	}

	s.mu.Lock()
	ps := s.stateFor(enginePrefix)
	s.mu.Unlock()

	// One pass at a time per prefix: concurrent passes (a flush notify racing the maintenance
	// tick) could install an older peer index over a newer one.
	ps.pass.Lock()
	defer ps.pass.Unlock()

	st, err := s.sync(ctx, enginePrefix, peers, strict, keep)
	s.account(st, err)

	return st, err
}

// account folds one pass's outcome into the cumulative totals.
func (s *Syncer) account(st Stats, err error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.totals.Passes++
	s.totals.Copied += int64(st.Copied)
	s.totals.CopiedBytes += st.CopiedBytes
	s.totals.Pruned += int64(st.Pruned)
	s.totals.Withheld += int64(st.Withheld)

	switch {
	case err != nil:
		s.totals.Errors++
	case st.Synced:
		s.totals.Mirrored++
		s.totals.LastSyncUnixNano = time.Now().UnixNano()
	}
}

// sync is one uncounted mirroring pass; see [Syncer.Sync].
func (s *Syncer) sync(ctx context.Context, enginePrefix string, peers []string, strict bool, keep KeepFunc) (Stats, error) {
	// An object filter (EC slot filtering) means the pull reconciles by object *presence*, not
	// just by index generation: erasure-coding a part rewrites its objects (full copies →
	// shards) without changing the bucket index, so an index-only gate would never re-mirror the
	// new layout. The reconcile is idempotent — once converged it copies and prunes nothing.
	forced := keep != nil
	if keep == nil {
		keep = keepAll
	}

	indexKey := enginePrefix + "/" + bucketindex.Object

	addr, peerIndexRaw, peerIndex := s.newestPeer(ctx, indexKey, peers)
	if peerIndex == nil {
		return Stats{}, nil
	}

	localRaw, err := backend.ReadView(ctx, s.local, indexKey)
	if err != nil && !errors.Is(err, backend.ErrNotExist) {
		return Stats{}, errors.Wrap(err, "read local index")
	}

	localIndex := &bucketindex.Index{}
	if localRaw != nil {
		if ix, err := bucketindex.Decode(localRaw); err == nil {
			localIndex = ix
		} // a corrupt local index is treated as empty and overwritten by the mirror
	}

	cmp := compareIndexes(peerIndex, localIndex)
	newer := cmp > 0 || (cmp == 0 && !strict && !bytes.Equal(peerIndexRaw, localRaw))

	// Whether the peer's index may *authorize a deletion*, which is a stronger claim than whether
	// it is worth mirroring. Only a peer that provably supersedes may. An index that merely
	// differs says nothing about what is missing from it: an owner that silently lost an older
	// part — bit rot, a partial rm, a restore from a stale snapshot — still holds the newest, so
	// nothing but the commit generation separates that from a deliberate shrink. Taking its word
	// there would leave a replica's own good copies unprotected, and prune would delete them two
	// passes later: the damage propagating from the broken node to the healthy ones.
	//
	// This is the one place the design inverts ClickHouse, where deletion is always an explicit
	// DROP_RANGE and a replica that finds a part missing fetches it rather than dropping its own.
	supersedes := cmp > 0

	if !newer && !forced {
		return Stats{}, nil // peer is older, or not newer enough, and no object-level reconcile
	}

	st := Stats{}

	unbacked, err := s.copyMissing(ctx, &st, addr, enginePrefix, indexKey, keep, peerIndex.Entries)
	if err != nil {
		return st, err
	}

	// The index was read before the peer's key listing, so an owner merge landing in between hands
	// us an index naming a part whose objects the peer has already dropped — a part the copy loop
	// then cannot have copied, either because the listing no longer names it or because its fetch
	// 404ed. Installing it would publish an entry resolving to objects that are not here. Racing a
	// merge stays tolerated; adopting an index the pass could not back is what does not.
	if len(unbacked) > 0 {
		zctx.From(ctx).Debug("partsync: peer index outran its objects, deferring install",
			zap.String("prefix", enginePrefix), zap.String("peer", addr),
			zap.Int("parts", len(unbacked)))

		supersedes = false
	}

	// Adopting the peer's index is what makes its absences ours, so it is gated on the same
	// claim pruning is: a non-superseding index is not installed, and the local one goes on
	// naming what this node holds. Copying stays unconditional — it is additive, and a peer that
	// has an object we lack is worth taking whichever way the indexes order.
	if supersedes && !bytes.Equal(peerIndexRaw, localRaw) {
		// Installed last (the commit point) — it only ever references parts whose objects are
		// already local.
		if err := s.local.Write(ctx, indexKey, peerIndexRaw); err != nil {
			return st, errors.Wrap(err, "install index")
		}

		st.Copied++
		st.CopiedBytes += int64(len(peerIndexRaw))
	}

	del := deletion{
		live:    livePartSet(peerIndex, localIndex, supersedes),
		removed: peerIndex.Removals(),
		stated:  peerIndex.RecordsRemovals(),
	}
	if err := s.prune(ctx, &st, enginePrefix, keep, del); err != nil {
		return st, err
	}

	st.Synced = newer || st.Copied > 0 || st.Pruned > 0

	return st, nil
}

// deletion is what a pass is allowed to delete, which is a narrower question than what it is
// worth mirroring.
//
// live are the parts to protect outright. removed are the parts the peer *said* it removed; a
// part absent from both is one the peer no longer has and never claimed to have dropped, which is
// a peer missing data rather than an instruction. stated reports whether the peer records removals
// at all — an index predating the format states none, and there absence is all there is to go on,
// which is the pre-tombstone behavior kept for the transition.
type deletion struct {
	live    map[string]struct{}
	removed map[string]struct{}
	stated  bool
}

// authorizes reports whether key may be considered for deletion at all.
func (d deletion) authorizes(key, enginePrefix string) bool {
	part := partOf(key, enginePrefix)
	if part == "" {
		// Not a part object — the bucket index, an engine sidecar. Those are reconciled by the
		// peer's listing alone, as they always were: they have no part to be tombstoned.
		return true
	}

	if _, ok := d.live[part]; ok {
		return false
	}

	if !d.stated {
		return true
	}

	_, ok := d.removed[part]

	return ok
}

// partOf returns the part prefix key belongs to, or "" for an object that belongs to no part.
func partOf(key, enginePrefix string) string {
	if part, _, ok := strings.Cut(key, shardMarker); ok {
		return part
	}

	rest, ok := strings.CutPrefix(key, enginePrefix+"/")
	if !ok {
		return ""
	}

	seg, _, ok := strings.Cut(rest, "/")
	if !ok {
		return "" // a direct child of the engine prefix is a sidecar, not a part.
	}

	return enginePrefix + "/" + seg
}

// livePartSet is the set of part prefixes prune must protect: the peer's index when that index
// supersedes the local one, and the union of the two otherwise.
//
// The union is the second half of not taking a non-superseding peer's word for a deletion. Not
// adopting its index is what makes the protection last — the local index goes on naming the part,
// so every later pass unions it back in — but the pass that discovered the shrink still has to
// protect it, and an object-level reconcile (EC slot filtering) prunes on a tie by design and
// would otherwise quarantine it on the way past.
//
// Where the two indexes agree, which is every ordinary pass, the union is the peer's set.
func livePartSet(peerIndex, localIndex *bucketindex.Index, supersedes bool) map[string]struct{} {
	live := make(map[string]struct{}, len(peerIndex.Entries))
	for _, e := range peerIndex.Entries {
		live[e.Prefix] = struct{}{}
	}

	if supersedes {
		return live
	}

	for _, e := range localIndex.Entries {
		live[e.Prefix] = struct{}{}
	}

	return live
}

// stateFor returns (creating if needed) the prune bookkeeping for enginePrefix. Caller holds s.mu.
func (s *Syncer) stateFor(enginePrefix string) *prefixState {
	st := s.state[enginePrefix]
	if st == nil {
		st = &prefixState{miss: make(map[string]int)}
		s.state[enginePrefix] = st
	}

	return st
}

// newestPeer fetches every peer's bucket index for indexKey and returns the newest one (by
// part sequence, then flushed epoch). Unreachable peers and missing indexes are skipped.
func (s *Syncer) newestPeer(ctx context.Context, indexKey string, peers []string) (addr string, raw []byte, ix *bucketindex.Index) {
	for _, p := range peers {
		data, err := s.client.Fetch(ctx, p, indexKey)
		if err != nil {
			continue // unreachable or has no index: not a candidate
		}

		cand, err := bucketindex.Decode(data)
		if err != nil {
			continue // corrupt copy: not a candidate
		}

		if ix == nil || compareIndexes(cand, ix) > 0 {
			addr, raw, ix = p, data, cand
		}
	}

	return addr, raw, ix
}

// copyMissing fetches from addr every object under enginePrefix the local backend lacks,
// ordering manifests after their part's other objects and re-fetching the mutable identity
// objects; the bucket index itself is excluded (installed by the caller, last).
func (s *Syncer) copyMissing(
	ctx context.Context, st *Stats, addr, enginePrefix, indexKey string, keep KeepFunc,
	indexed []bucketindex.Entry,
) (map[string]struct{}, error) {
	listed, err := s.client.List(ctx, addr, enginePrefix)
	if err != nil {
		return nil, errors.Wrap(err, "list peer")
	}

	// Measured against the unfiltered listing: an object filter narrows what this node stores, not
	// what the peer holds, so filtering here would report a part as lost the moment its objects
	// became someone else's shards.
	unbacked := unbackedParts(indexed, listed, enginePrefix)

	// Apply the caller's object filter (EC slot filtering) up front, so both the copy set and
	// the prune bookkeeping (remote set below) see only the objects this node should hold — a
	// filtered-out shard the node still has locally is then pruned as "absent from remote".
	remote := listed[:0]
	for _, k := range listed {
		if keep(k) {
			remote = append(remote, k)
		}
	}

	local, err := s.local.List(ctx, enginePrefix)
	if err != nil {
		return nil, errors.Wrap(err, "list local")
	}

	have := make(map[string]struct{}, len(local))
	for _, k := range local {
		have[k] = struct{}{}
	}

	immutable, manifests, mutable := classifyFetch(remote, enginePrefix, indexKey, have)

	for _, group := range [][]string{immutable, manifests, mutable} {
		for _, k := range group {
			data, err := s.client.Fetch(ctx, addr, k)
			if err != nil {
				if errors.Is(err, ErrNotExist) {
					// Raced a merge on the peer: the object went away with its part. Tolerated,
					// but the part is no longer one this pass can vouch for.
					if part := partOf(k, enginePrefix); part != "" {
						unbacked[part] = struct{}{}
					}

					continue
				}

				return nil, err
			}

			if err := s.local.Write(ctx, k, data); err != nil {
				return nil, errors.Wrapf(err, "write %q", k)
			}

			st.Copied++
			st.CopiedBytes += int64(len(data))
		}
	}

	// Remember the peer's key set for prune bookkeeping.
	s.mu.Lock()
	s.stateFor(enginePrefix).remote = keySet(remote)
	s.mu.Unlock()

	return unbacked, nil
}

// unbackedParts is the set of index entries the peer's own listing no longer backs with any
// object: parts an owner merge dropped after the index was read, which no copy can bring over.
func unbackedParts(indexed []bucketindex.Entry, listed []string, enginePrefix string) map[string]struct{} {
	backed := make(map[string]struct{}, len(listed))

	for _, k := range listed {
		if part := partOf(k, enginePrefix); part != "" {
			backed[part] = struct{}{}
		}
	}

	unbacked := make(map[string]struct{})

	for i := range indexed {
		if _, ok := backed[indexed[i].Prefix]; !ok {
			unbacked[indexed[i].Prefix] = struct{}{}
		}
	}

	return unbacked
}

// classifyFetch splits a peer's (already slot-filtered) key listing into the objects to fetch,
// ordered so the manifest lands after its part's other objects: immutable objects the node
// lacks, manifests, and the mutable identity objects (always re-fetched). The bucket index is
// excluded (the caller installs it last). Keys outside the prefix or malformed are dropped —
// a correct peer never produces them.
func classifyFetch(remote []string, enginePrefix, indexKey string, have map[string]struct{}) (immutable, manifests, mutable []string) {
	for _, k := range remote {
		if !ValidKey(k) || !strings.HasPrefix(k, enginePrefix+"/") {
			continue
		}

		switch {
		case k == indexKey:
			// installed by the caller, last
		case isMutableAux(k):
			mutable = append(mutable, k)
		default:
			if _, ok := have[k]; ok {
				continue // immutable and already local
			}

			if path.Base(k) == "manifest" {
				manifests = append(manifests, k)
			} else {
				immutable = append(immutable, k)
			}
		}
	}

	return immutable, manifests, mutable
}

// prune deletes local objects the peer no longer has, but only after they have been absent for
// [pruneAfterMisses] consecutive passes — an in-flight reader gets a full maintenance cycle to
// drain before a superseded part's objects go away.
func (s *Syncer) prune(ctx context.Context, st *Stats, enginePrefix string, keep KeepFunc, del deletion) error {
	local, err := s.local.List(ctx, enginePrefix)
	if err != nil {
		return errors.Wrap(err, "list local for prune")
	}

	s.mu.Lock()
	ps := s.stateFor(enginePrefix)
	remote, counts := ps.remote, ps.miss

	var (
		doomed   []string
		withheld int
	)

	seen := make(map[string]struct{}, len(local))

	for _, k := range local {
		seen[k] = struct{}{}

		// A live part's protected objects must never be pruned by source absence:
		//   - objects this node should hold (keep) — its own shard slot, full copies — are
		//     authoritative locally even when the source legitimately dropped them;
		//   - ANY erasure-coded shard, own slot or not: a membership change renumbers slots, so
		//     a "foreign" shard here may be one of the part's last surviving copies that repair
		//     still needs to gather. Live-part shards are deleted only by the owner-prune path,
		//     which confirms the slot's owner holds it first. The cost is a stale-shard residue
		//     after churn (bounded by part turnover), reclaimed when the part is superseded.
		// Superseded-part objects (part gone from the index) always fall through to the
		// remote-absence quarantine below.
		if livePart(k, del.live) && (keep(k) || strings.Contains(k, shardMarker)) {
			delete(counts, k)

			continue
		}

		// A superseded part's objects fall through to the absence quarantine below only if the
		// peer actually said it removed the part. Otherwise the peer is missing data it should
		// hold, and deleting the local copy on that basis is how one node's loss becomes the
		// cluster's.
		if !del.authorizes(k, enginePrefix) {
			delete(counts, k)
			withheld++

			continue
		}

		if _, ok := remote[k]; ok {
			delete(counts, k) // present again: reset

			continue
		}

		counts[k]++
		if counts[k] >= pruneAfterMisses {
			doomed = append(doomed, k)
			delete(counts, k)
		}
	}

	// Forget counters for keys that no longer exist locally.
	for k := range counts {
		if _, ok := seen[k]; !ok {
			delete(counts, k)
		}
	}
	s.mu.Unlock()

	st.Withheld += withheld

	for _, k := range doomed {
		if err := s.local.Delete(ctx, k); err != nil && !errors.Is(err, backend.ErrNotExist) {
			return errors.Wrapf(err, "prune %q", k)
		}

		st.Pruned++
	}

	return nil
}

// livePart reports whether key belongs to a part still listed in the index. A shard key
// (`{part}/ecshard/{slot}/{obj}`) is checked by its part prefix; any other key under a live
// part prefix also qualifies. A key belonging to no live part (superseded by a merge) is not
// protected and falls through to pruning.
// shardMarker separates a part prefix from a shard slot in an erasure-coded shard key (kept in
// sync with the cluster/ec layout).
const shardMarker = "/ecshard/"

func livePart(key string, liveParts map[string]struct{}) bool {
	if part, _, ok := strings.Cut(key, shardMarker); ok {
		_, live := liveParts[part]

		return live
	}

	for part := range liveParts {
		if strings.HasPrefix(key, part+"/") {
			return true
		}
	}

	return false
}

// compareIndexes orders two bucket indexes by recency: the lexicographically higher max part
// prefix wins, then the higher flushed epoch. Zero means indistinguishable (same generation).
func compareIndexes(a, b *bucketindex.Index) int {
	// The commit generation is the only thing that orders index *states*: it advances on a
	// rewrite that merely removes parts, which is precisely the rewrite the fallbacks below
	// cannot see. An index that has one is therefore ranked by it alone.
	ag, bg := a.Generation, b.Generation
	if !ag.Zero() || !bg.Zero() {
		// One side predating the format is older by construction — a writer that stamps a
		// generation has, by definition, written since the other one did.
		return ag.Compare(bg)
	}

	// Neither side carries one: both were written before format v3, so fall back to what those
	// writers did express. This is the pre-#278 ranking, kept only for the transition, and it
	// cannot tell a shrunk index from a damaged one — which is why the caller treats a
	// non-superseding index as unable to authorize a deletion.
	as, bs := maxPartPrefix(a), maxPartPrefix(b)

	switch {
	case as != bs:
		if as > bs {
			return 1
		}

		return -1
	case a.FlushedEpoch != b.FlushedEpoch:
		if a.FlushedEpoch > b.FlushedEpoch {
			return 1
		}

		return -1
	default:
		return 0
	}
}

// maxPartPrefix is the highest part id in ix. Part ids sort lexicographically in creation order, so
// the highest one names the index's newest part.
func maxPartPrefix(ix *bucketindex.Index) string {
	var m string

	for _, e := range ix.Entries {
		if n := path.Base(e.Prefix); n > m {
			m = n
		}
	}

	return m
}

// isMutableAux reports whether key is one of the engine's mutable (rewritten-on-flush) auxiliary
// objects: the record engines' streams.bin, and the metrics series.bin that only a prefix written
// before identity became part-scoped still carries — it must keep being mirrored until the owner
// deletes it, which the ordinary absence quarantine then propagates.
func isMutableAux(key string) bool {
	switch path.Base(key) {
	case "series.bin", "streams.bin":
		return true
	}

	return strings.HasSuffix(key, "/"+bucketindex.Object)
}

// keySet builds a set from keys.
func keySet(keys []string) map[string]struct{} {
	m := make(map[string]struct{}, len(keys))
	for _, k := range keys {
		m[k] = struct{}{}
	}

	return m
}
