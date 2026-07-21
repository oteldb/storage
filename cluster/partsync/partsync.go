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
// Objects are content-immutable except the bucket index and the head-identity objects
// (series.bin / streams.bin), so a plain presence diff drives the copy; the mutable objects
// are re-fetched whenever the index changed. Every fetched object is verified against the
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
	"github.com/zeebo/xxh3"

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
		return nil, errors.Errorf("list: %q returned %d", addr, resp.StatusCode)
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
		return nil, errors.Errorf("fetch %q: %q returned %d", key, addr, resp.StatusCode)
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
		return errors.Errorf("notify: %q returned %d", addr, resp.StatusCode)
	}

	return nil
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

	if !newer && !forced {
		return Stats{}, nil // peer is older, or not newer enough, and no object-level reconcile
	}

	st := Stats{}

	if err := s.copyMissing(ctx, &st, addr, enginePrefix, indexKey, keep); err != nil {
		return st, err
	}

	// Install the index last (the commit point) when it actually differs — it only ever
	// references parts whose objects are already local.
	if !bytes.Equal(peerIndexRaw, localRaw) {
		if err := s.local.Write(ctx, indexKey, peerIndexRaw); err != nil {
			return st, errors.Wrap(err, "install index")
		}

		st.Copied++
		st.CopiedBytes += int64(len(peerIndexRaw))
	}

	if err := s.prune(ctx, &st, enginePrefix); err != nil {
		return st, err
	}

	st.Synced = newer || st.Copied > 0 || st.Pruned > 0

	return st, nil
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
func (s *Syncer) copyMissing(ctx context.Context, st *Stats, addr, enginePrefix, indexKey string, keep KeepFunc) error {
	listed, err := s.client.List(ctx, addr, enginePrefix)
	if err != nil {
		return errors.Wrap(err, "list peer")
	}

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
		return errors.Wrap(err, "list local")
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
					continue // raced a merge on the peer: the object went away with its part
				}

				return err
			}

			if err := s.local.Write(ctx, k, data); err != nil {
				return errors.Wrapf(err, "write %q", k)
			}

			st.Copied++
			st.CopiedBytes += int64(len(data))
		}
	}

	// Remember the peer's key set for prune bookkeeping.
	s.mu.Lock()
	s.stateFor(enginePrefix).remote = keySet(remote)
	s.mu.Unlock()

	return nil
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
func (s *Syncer) prune(ctx context.Context, st *Stats, enginePrefix string) error {
	local, err := s.local.List(ctx, enginePrefix)
	if err != nil {
		return errors.Wrap(err, "list local for prune")
	}

	s.mu.Lock()
	ps := s.stateFor(enginePrefix)
	remote, counts := ps.remote, ps.miss

	var doomed []string

	seen := make(map[string]struct{}, len(local))

	for _, k := range local {
		seen[k] = struct{}{}

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

	for _, k := range doomed {
		if err := s.local.Delete(ctx, k); err != nil && !errors.Is(err, backend.ErrNotExist) {
			return errors.Wrapf(err, "prune %q", k)
		}

		st.Pruned++
	}

	return nil
}

// compareIndexes orders two bucket indexes by recency: the higher max part sequence wins, then
// the higher flushed epoch. Zero means indistinguishable (same generation).
func compareIndexes(a, b *bucketindex.Index) int {
	as, bs := maxSeq(a), maxSeq(b)

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

// maxSeq is the highest trailing part-sequence number referenced by ix, or -1 for none.
func maxSeq(ix *bucketindex.Index) int {
	m := -1

	for _, e := range ix.Entries {
		if n, err := strconv.Atoi(path.Base(e.Prefix)); err == nil && n > m {
			m = n
		}
	}

	return m
}

// isMutableAux reports whether key is one of the engine's mutable (rewritten-on-flush)
// auxiliary objects: the head-identity sets series.bin (metrics) / streams.bin (records).
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
