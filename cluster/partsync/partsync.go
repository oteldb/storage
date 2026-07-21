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

	"github.com/go-faster/errors"
	"github.com/zeebo/xxh3"

	"github.com/oteldb/storage/backend"
	"github.com/oteldb/storage/backend/bucketindex"
	"github.com/oteldb/storage/internal/obs"
)

const (
	// ListPath is the HTTP path serving a node's backend key listing under a prefix.
	ListPath = "/internal/parts/list"
	// ObjectPath is the HTTP path serving one backend object verbatim.
	ObjectPath = "/internal/parts/object"

	// checksumHeader carries the xxh3 hash of the object body, verified by the client.
	checksumHeader = "X-Checksum-Xxh3"

	// pruneAfterMisses is how many consecutive sync passes a local object must be absent from
	// the peer before it is deleted.
	pruneAfterMisses = 2
)

// ListHandler serves the backend keys under the "prefix" query parameter, framed as a uvarint
// count followed by uvarint-length-prefixed keys.
func ListHandler(be backend.Backend) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		keys, err := be.List(req.Context(), req.URL.Query().Get("prefix"))
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
		data, err := backend.ReadView(req.Context(), be, req.URL.Query().Get("key"))
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

func (c *Client) http() *http.Client {
	if c.HTTP != nil {
		return c.HTTP
	}

	return http.DefaultClient
}

func (c *Client) get(ctx context.Context, addr, p string, q url.Values) (*http.Response, error) {
	u := "http://" + addr + p + "?" + q.Encode()

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u, http.NoBody)
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

// Syncer mirrors engine prefixes of a per-node private backend from cluster peers. Safe for
// concurrent use across distinct prefixes; per-prefix passes are expected to be serial (the
// maintenance loop runs one task per engine).
type Syncer struct {
	local  backend.Backend
	client *Client

	mu sync.Mutex
	// state is the per-engine-prefix prune bookkeeping.
	state map[string]*prefixState
}

// prefixState is the prune bookkeeping for one engine prefix: the peer's key set from the last
// pass and, per local key, how many consecutive passes it has been absent from the peer.
type prefixState struct {
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
func (s *Syncer) Sync(ctx context.Context, enginePrefix string, peers []string, strict bool) (Stats, error) {
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

	if cmp := compareIndexes(peerIndex, localIndex); cmp < 0 || (cmp == 0 && (strict || bytes.Equal(peerIndexRaw, localRaw))) {
		return Stats{}, nil // peer is older, or not newer enough for this mode
	}

	st := Stats{Synced: true}

	if err := s.copyMissing(ctx, &st, addr, enginePrefix, indexKey); err != nil {
		return st, err
	}

	// Install the index last: it only ever references parts whose objects are already local.
	if err := s.local.Write(ctx, indexKey, peerIndexRaw); err != nil {
		return st, errors.Wrap(err, "install index")
	}
	st.Copied++
	st.CopiedBytes += int64(len(peerIndexRaw))

	if err := s.prune(ctx, &st, enginePrefix); err != nil {
		return st, err
	}

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
func (s *Syncer) copyMissing(ctx context.Context, st *Stats, addr, enginePrefix, indexKey string) error {
	remote, err := s.client.List(ctx, addr, enginePrefix)
	if err != nil {
		return errors.Wrap(err, "list peer")
	}

	local, err := s.local.List(ctx, enginePrefix)
	if err != nil {
		return errors.Wrap(err, "list local")
	}

	have := make(map[string]struct{}, len(local))
	for _, k := range local {
		have[k] = struct{}{}
	}

	var immutable, manifests, mutable []string

	for _, k := range remote {
		switch {
		case k == indexKey:
			// installed by the caller, last
		case isMutableAux(k):
			mutable = append(mutable, k) // re-fetch: content changes across flushes
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
