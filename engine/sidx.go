package engine

import (
	"context"
	"encoding/binary"
	"hash/crc32"
	"sync/atomic"

	"github.com/go-faster/errors"

	"github.com/oteldb/storage/backend"
	"github.com/oteldb/storage/encoding/chunk"
	"github.com/oteldb/storage/signal"
)

// The series-index sidecar ({prefix}/sidx) is the on-disk, binary-searchable form of a part's
// SeriesID → row-range index: the sorted distinct series ids with their run-start rows, as
// fixed-width entries so a lookup binary-searches the raw bytes in place — no per-part decode and
// no per-series Go objects. A part with a sidecar is opened without reading the series column at
// all, and its index bytes live in the (byte-bounded, evictable) backend read cache instead of
// being pinned in the heap for the part's lifetime; the engine holds a view only while a fetch is
// actually reading the part (dropped when the part's refs reach zero, see [pagedIndex]).
//
// Layout: [u32 magic "OTSI"] [u8 version=1] [uvarint numSeries] [uvarint totalRows]
// [numSeries × 20-byte entries: u64be id.Hi ‖ u64be id.Lo ‖ u32be startRow] [u32 CRC32C of all
// preceding bytes]. Entries are sorted by id; entry k's row range is
// [start(k), start(k+1)) with start(numSeries) = totalRows (runs partition the part).
//
// The sidecar is a derived structure — absent or corrupt, openPart falls back to rebuilding the
// resident index from the series column — so it carries no format-migration burden; it is still
// golden-tested against accidental byte drift.
const (
	sidxMagic   uint32 = 0x4F545349 // "OTSI"
	sidxVersion byte   = 1
	sidxEntryW         = 20 // 16-byte id + 4-byte start row
)

var sidxCRC = crc32.MakeTable(crc32.Castagnoli)

// errSidxCorrupt marks an unreadable series-index sidecar; the caller falls back to the resident
// index built from the series column.
var errSidxCorrupt = errors.New("engine: corrupt series-index sidecar")

// sidxKey is the backend key of a part's series-index sidecar (deleted with the part, since
// deletePart lists and removes everything under the prefix).
func sidxKey(prefix string) string { return prefix + "/sidx" }

// encodeSeriesIndex serializes the series-index sidecar from a part's sorted series column (each
// distinct id repeated for its contiguous run) — the same single pass as buildPartIndex, emitted
// as fixed-width entries instead of resident slices.
func encodeSeriesIndex(ids []chunk.U128) []byte {
	distinct := 0
	if len(ids) > 0 {
		distinct = 1
		for i := 1; i < len(ids); i++ {
			if ids[i] != ids[i-1] {
				distinct++
			}
		}
	}

	buf := make([]byte, 0, 5+2*binary.MaxVarintLen64+distinct*sidxEntryW+4)
	buf = binary.BigEndian.AppendUint32(buf, sidxMagic)
	buf = append(buf, sidxVersion)
	buf = binary.AppendUvarint(buf, uint64(distinct))
	buf = binary.AppendUvarint(buf, uint64(len(ids)))

	for i := 0; i < len(ids); {
		j := i + 1
		for j < len(ids) && ids[j] == ids[i] {
			j++
		}

		buf = binary.BigEndian.AppendUint64(buf, ids[i].Hi)
		buf = binary.BigEndian.AppendUint64(buf, ids[i].Lo)
		buf = binary.BigEndian.AppendUint32(buf, uint32(i))
		i = j
	}

	return binary.BigEndian.AppendUint32(buf, crc32.Checksum(buf, sidxCRC))
}

// parseSeriesIndex validates a sidecar and returns its entries region (a view into data) and
// header counts. checkCRC selects full-object integrity (paid once, at part open); a reload of an
// already-validated object passes false and gets the O(1) structural checks only — every
// subsequent entry access is bounds-safe against the returned view regardless. Never panics on
// malformed input.
func parseSeriesIndex(data []byte, checkCRC bool) (entries []byte, numSeries, totalRows int, err error) {
	if len(data) < 5+2+4 { // magic+version, two ≥1-byte uvarints, crc
		return nil, 0, 0, errors.Wrap(errSidxCorrupt, "short")
	}

	body := data[:len(data)-4]
	if checkCRC && crc32.Checksum(body, sidxCRC) != binary.BigEndian.Uint32(data[len(data)-4:]) {
		return nil, 0, 0, errors.Wrap(errSidxCorrupt, "crc")
	}

	if binary.BigEndian.Uint32(body) != sidxMagic {
		return nil, 0, 0, errors.Wrap(errSidxCorrupt, "magic")
	}

	if body[4] != sidxVersion {
		return nil, 0, 0, errors.Wrap(errSidxCorrupt, "version")
	}

	rest := body[5:]

	n, m := binary.Uvarint(rest)
	if m <= 0 {
		return nil, 0, 0, errors.Wrap(errSidxCorrupt, "series count")
	}

	rest = rest[m:]

	total, m := binary.Uvarint(rest)
	if m <= 0 {
		return nil, 0, 0, errors.Wrap(errSidxCorrupt, "row count")
	}

	rest = rest[m:]

	if n > uint64(len(rest))/sidxEntryW || total > uint64(maxInt) {
		return nil, 0, 0, errors.Wrap(errSidxCorrupt, "entry count")
	}

	if uint64(len(rest)) != n*sidxEntryW {
		return nil, 0, 0, errors.Wrap(errSidxCorrupt, "entry region size")
	}

	return rest, int(n), int(total), nil
}

const maxInt = int(^uint(0) >> 1)

// sidxEntryID returns entry k's series id from a validated entries view.
func sidxEntryID(entries []byte, k int) signal.SeriesID {
	e := entries[k*sidxEntryW:]

	return signal.SeriesID{Hi: binary.BigEndian.Uint64(e), Lo: binary.BigEndian.Uint64(e[8:])}
}

// sidxEntryStart returns entry k's run-start row from a validated entries view.
func sidxEntryStart(entries []byte, k int) int {
	return int(binary.BigEndian.Uint32(entries[k*sidxEntryW+16:]))
}

// pagedIndex is the paged form of a part's series index: lookups binary-search the sidecar's raw
// entry bytes, fetched through the backend on demand and held only while at least one fetch is
// reading the part. Between fetches nothing stays pinned in the heap — the bytes live (evictably)
// in the backend read cache, so resident index memory is governed by the cache's byte budget
// rather than the part's series count.
//
// keep marks a backend without the zero-copy [backend.Viewer] capability (a bare cold-tier backend
// with the read cache disabled): re-reading the sidecar per fetch would pay a full object read, so
// the view is kept for the part's lifetime instead — the footprint of the old resident index, and
// no cold-latency regression. With a Viewer (memory backend, or any backend behind
// [backend.Cached]) a reload is a cache-hit view, so dropping is nearly free.
type pagedIndex struct {
	be    backend.Backend
	key   string
	n     int // distinct series
	total int // rows
	keep  bool

	view atomic.Pointer[[]byte] // validated entries region; nil ⇒ load on next use
}

// entries returns the sidecar's entry bytes, loading (and structurally re-validating) them through
// the backend when dropped. Concurrent loads race benignly (identical immutable content).
func (p *pagedIndex) entries(ctx context.Context) ([]byte, error) {
	if v := p.view.Load(); v != nil {
		return *v, nil
	}

	data, err := backend.ReadView(ctx, p.be, p.key)
	if err != nil {
		return nil, errors.Wrap(err, "reload series index")
	}

	// Full CRC ran at part open; a reload of the immutable object needs only the O(1) structural
	// checks (all entry accesses are bounds-safe against the returned view either way).
	ents, n, total, err := parseSeriesIndex(data, false)
	if err != nil {
		return nil, err
	}

	if n != p.n || total != p.total {
		return nil, errors.Wrap(errSidxCorrupt, "reload mismatch")
	}

	p.view.Store(&ents)

	return ents, nil
}

// drop releases the held entries view (unless keep pins it for the part's life). Called when the
// part's last in-flight fetch releases it; a lookup racing the drop simply reloads.
func (p *pagedIndex) drop() {
	if !p.keep {
		p.view.Store(nil)
	}
}

// search returns the entry index of id, or (0, false).
func (p *pagedIndex) search(entries []byte, id signal.SeriesID) (int, bool) {
	lo, hi := 0, p.n
	for lo < hi {
		mid := int(uint(lo+hi) >> 1)

		switch c := sidxEntryID(entries, mid).Compare(id); {
		case c < 0:
			lo = mid + 1
		case c > 0:
			hi = mid
		default:
			return mid, true
		}
	}

	return 0, false
}

// rangeAt returns entry k's row range: [start(k), start(k+1)), with the last run ending at the
// part's row total.
func (p *pagedIndex) rangeAt(entries []byte, k int) rowRange {
	end := p.total
	if k+1 < p.n {
		end = sidxEntryStart(entries, k+1)
	}

	return rowRange{start: sidxEntryStart(entries, k), end: end}
}

// validSidxEntries checks the invariants every derived row range relies on — ids strictly
// ascending (binary search) and run starts strictly increasing below the row total (so
// [start(k), start(k+1)) never slices out of a decoded column) — rejecting a CRC-valid but
// wrongly-written sidecar rather than trusting it on the read path. One linear pass at part open.
func validSidxEntries(entries []byte, n, total int) bool {
	if n == 0 {
		return total == 0
	}

	if sidxEntryStart(entries, 0) != 0 {
		return false
	}

	for k := 1; k < n; k++ {
		if sidxEntryID(entries, k-1).Compare(sidxEntryID(entries, k)) >= 0 {
			return false
		}

		if sidxEntryStart(entries, k-1) >= sidxEntryStart(entries, k) {
			return false
		}
	}

	return sidxEntryStart(entries, n-1) < total
}

// openPagedIndex reads and fully validates the part's series-index sidecar, returning ok=false
// (absent or corrupt sidecar, or a row total disagreeing with the manifest) to send the caller to
// the resident fallback. The validated view is retained — the part is being opened to be read, so
// the first fetch starts warm; it drops at the part's first refs==0 release.
func openPagedIndex(ctx context.Context, b backend.Backend, prefix string, manifestRows int) (*pagedIndex, bool) {
	data, err := backend.ReadView(ctx, b, sidxKey(prefix))
	if err != nil {
		return nil, false // absent (or unreadable) ⇒ resident fallback
	}

	ents, n, total, err := parseSeriesIndex(data, true)
	if err != nil || total != manifestRows || !validSidxEntries(ents, n, total) {
		return nil, false
	}

	_, viewer := b.(backend.Viewer)

	p := &pagedIndex{be: b, key: sidxKey(prefix), n: n, total: total, keep: !viewer}
	p.view.Store(&ents)

	return p, true
}
