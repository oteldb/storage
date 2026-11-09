package engine

import (
	"context"
	"encoding/binary"
	"math"
	"slices"

	"github.com/go-faster/errors"

	"github.com/oteldb/storage/backend"
	"github.com/oteldb/storage/index/identity"
	"github.com/oteldb/storage/index/postings"
	"github.com/oteldb/storage/internal/seriesindex"
	"github.com/oteldb/storage/internal/watermark"
	"github.com/oteldb/storage/query/fetch"
	"github.com/oteldb/storage/readbudget"
	"github.com/oteldb/storage/signal"
)

func (p *part) diskSeriesWorkspace(ctx context.Context, nMatchers int) (int64, error) {
	idx, err := p.seriesIndex(ctx)
	if err != nil {
		return 0, err
	}
	if idx != nil {
		return int64(max(nMatchers, 1)+3)*idx.MaxPageBytes() + int64(max(nMatchers, 1))*((int64(idx.Count())+63)/64)*8, nil
	}
	key, err := p.diskIdentityKey(ctx)
	if err != nil {
		return 0, err
	}
	var size int64
	for _, k := range []string{key, watermark.Key(p.prefix), statsKey(p.prefix)} {
		n, known, err := backend.NativeSizeOf(ctx, p.be, k)
		if errors.Is(err, backend.ErrNotExist) && k != key {
			continue
		}
		if err != nil {
			return 0, err
		}
		if !known {
			return 0, errors.Wrap(readbudget.ErrExceeded, "cannot bound legacy identity object without native sizes")
		}
		if n < 0 || n > (math.MaxInt64-size)/(16) {
			return 0, errors.New("legacy identity workspace overflow")
		}
		size += 16 * n
	}
	column, shared, err := wholeMergeColumnBytes(p, colTs)
	if err != nil {
		return 0, err
	}
	if column > math.MaxInt64-shared || column+shared > math.MaxInt64-size-512<<10 {
		return 0, errors.New("legacy timestamp workspace overflow")
	}
	return size + column + shared + 512<<10, nil
}

func diskObjectPresent(ctx context.Context, b backend.Backend, key string) (bool, error) {
	_, known, err := backend.NativeSizeOf(ctx, b, key)
	if errors.Is(err, backend.ErrNotExist) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	if known {
		return true, nil
	}
	keys, err := b.List(ctx, key)
	if err != nil {
		return false, err
	}
	return slices.Contains(keys, key), nil
}

func (p *part) diskIdentityKey(ctx context.Context) (string, error) {
	p.metadataMu.Lock()
	legacyKey := p.legacyIdentityKey
	p.metadataMu.Unlock()
	if legacyKey != "" {
		return legacyKey, nil
	}
	key := identityKey(p.prefix)
	present, err := diskObjectPresent(ctx, p.be, key)
	if err != nil {
		return "", err
	}
	if !present {
		return "", errors.Wrap(backend.ErrNotExist, "part identity")
	}
	return key, nil
}

func (p *part) visitDiskSeries(
	ctx context.Context, matchers []fetch.Matcher,
	visit func(signal.SeriesID, []byte, seriesindex.Metadata) error,
) error {
	ctx = p.reader.ReaderContext(ctx)
	idx, err := p.seriesIndex(ctx)
	if err != nil {
		return err
	}
	if idx != nil {
		refs, err := idx.Resolve(ctx, matchers)
		if err != nil {
			return err
		}
		return idx.Visit(ctx, refs, visit)
	}
	return p.visitLegacyDiskSeries(ctx, matchers, visit)
}

// Legacy objects are decoded once per operation; the admission estimate includes their input
// and decoded symbol, watermark and aggregate storage.
func (p *part) visitLegacyDiskSeries(
	ctx context.Context, matchers []fetch.Matcher,
	visit func(signal.SeriesID, []byte, seriesindex.Metadata) error,
) error {
	ctx = p.reader.ReaderContext(ctx)
	key, err := p.diskIdentityKey(ctx)
	if err != nil {
		return err
	}
	data, err := backend.ReadUncached(ctx, p.be, key)
	if err != nil {
		return errors.Wrap(err, "read legacy identity")
	}
	times, ok := p.sidecarWatermarks(ctx)
	if !ok {
		times, err = p.seriesMaxTimes(ctx)
		if err != nil {
			return err
		}
	}
	var stats map[signal.SeriesID]SeriesAgg
	if !p.hasSF {
		if raw, err := backend.ReadUncached(ctx, p.be, statsKey(p.prefix)); err == nil {
			stats, _ = decodeSeriesStats(raw)
		}
	}
	count := 0
	seen := make(map[signal.SeriesID]struct{})
	var raw []byte
	emit := func(id signal.SeriesID, s signal.Series) error {
		if err := ctx.Err(); err != nil {
			return err
		}
		present, err := p.index.has(ctx, id)
		if err != nil {
			return err
		}
		if !present {
			if key == identityKey(p.prefix) {
				return errors.Wrap(seriesindex.ErrCorrupt, "identity absent from part")
			}
			return nil
		}
		if _, duplicate := seen[id]; duplicate {
			return errors.Wrap(seriesindex.ErrCorrupt, "duplicate legacy identity")
		}
		seen[id] = struct{}{}
		count++
		raw = s.AppendHashInput(raw[:0])
		if signal.HashBytes(raw) != id {
			return errors.Wrap(seriesindex.ErrCorrupt, "legacy identity hash mismatch")
		}
		if !seriesindex.MatchesSeries(s, matchers) {
			return nil
		}
		meta, err := legacySeriesMetadata(times, stats, id)
		if err != nil {
			return err
		}
		return visit(id, raw, meta)
	}
	if key == identityKey(p.prefix) {
		err = identity.Decode(data, emit)
	} else {
		err = decodeSeriesSetVisit(data, func(s signal.Series) error { return emit(s.Hash(), s) })
	}
	if err != nil {
		return err
	}
	if count != p.index.seriesCount() {
		return errors.Wrap(seriesindex.ErrCorrupt, "legacy identities do not cover part")
	}
	return nil
}

func legacySeriesMetadata(times []watermark.Entry, stats map[signal.SeriesID]SeriesAgg, id signal.SeriesID) (seriesindex.Metadata, error) {
	lo, hi := 0, len(times)
	for lo < hi {
		mid := int(uint(lo+hi) >> 1)
		if times[mid].ID.Compare(id) < 0 {
			lo = mid + 1
		} else {
			hi = mid
		}
	}
	if lo == len(times) || times[lo].ID != id {
		return seriesindex.Metadata{}, errors.Wrap(seriesindex.ErrCorrupt, "legacy watermark missing identity")
	}
	meta := seriesindex.Metadata{Covered: times[lo].Max}
	if stat, ok := stats[id]; ok {
		meta.HasStats = true
		meta.Stats = seriesindex.Stats{Count: stat.Rows, Sum: stat.Sum, Min: stat.Min, Max: stat.Max}
	}
	return meta, nil
}

func (p *part) lookupDiskSeries(ctx context.Context, id signal.SeriesID) (signal.Series, seriesindex.Metadata, bool, error) {
	ctx = p.reader.ReaderContext(ctx)
	idx, err := p.seriesIndex(ctx)
	if err != nil {
		return signal.Series{}, seriesindex.Metadata{}, false, err
	}
	if idx != nil {
		return idx.Lookup(ctx, id)
	}
	present, err := p.index.has(ctx, id)
	if err != nil || !present {
		return signal.Series{}, seriesindex.Metadata{}, false, err
	}
	var s signal.Series
	var meta seriesindex.Metadata
	found := false
	err = p.visitLegacyDiskSeries(ctx, nil, func(got signal.SeriesID, raw []byte, m seriesindex.Metadata) error {
		if got == id {
			var err error
			s, _, err = signal.DecodeSeries(append([]byte(nil), raw...))
			if err != nil {
				return err
			}
			meta, found = m, true
		}
		return nil
	})
	return s, meta, found, err
}

func (p *part) diskSeriesMetadata(ctx context.Context, id signal.SeriesID) (seriesindex.Metadata, bool, error) {
	ctx = p.reader.ReaderContext(ctx)
	idx, err := p.seriesIndex(ctx)
	if err != nil {
		return seriesindex.Metadata{}, false, err
	}
	if idx != nil {
		return idx.Metadata(ctx, id)
	}
	present, err := p.index.has(ctx, id)
	if err != nil || !present {
		return seriesindex.Metadata{}, false, err
	}
	if covered, ok, err := p.legacyDiskWatermark(ctx, id); err != nil {
		return seriesindex.Metadata{}, false, err
	} else if ok {
		return seriesindex.Metadata{Covered: covered}, true, nil
	}
	times, ok := p.sidecarWatermarks(ctx)
	if !ok {
		times, err = p.seriesMaxTimes(ctx)
		if err != nil {
			return seriesindex.Metadata{}, false, err
		}
	}
	for _, t := range times {
		if t.ID == id {
			return seriesindex.Metadata{Covered: t.Max}, true, nil
		}
	}
	return seriesindex.Metadata{}, false, errors.Wrap(seriesindex.ErrCorrupt, "legacy watermark missing identity")
}

func (p *part) diskSeriesIDs(ctx context.Context) (postings.Postings, error) {
	ctx = p.reader.ReaderContext(ctx)
	idx, err := p.seriesIndex(ctx)
	if err != nil {
		return nil, err
	}
	if idx != nil {
		return &verifiedDiskIDs{indexed: idx.IDs(ctx), rows: p.diskRowIDs(ctx)}, nil
	}
	return p.diskRowIDs(ctx), nil
}

func (p *part) validateDiskSeriesIDs(ctx context.Context) error {
	p.diskIDsMu.Lock()
	defer p.diskIDsMu.Unlock()
	if p.diskIDsChecked {
		return nil
	}
	ids, err := p.diskSeriesIDs(ctx)
	if err != nil {
		return err
	}
	for ids.Next() {
	}
	if err := ids.Err(); err != nil {
		return err
	}
	p.diskIDsChecked = true
	return nil
}

func (p *part) diskRowIDs(ctx context.Context) postings.Postings {
	if p.index.paged == nil {
		return postings.FromSlice(p.index.ids)
	}
	return &diskPartIDs{ctx: p.reader.ReaderContext(ctx), index: p.index.paged, pos: -1}
}

type verifiedDiskIDs struct {
	indexed, rows postings.Postings
	err           error
	done          bool
	started       bool
}

func (p *verifiedDiskIDs) Next() bool {
	if p.err != nil || p.done {
		return false
	}
	p.started = true
	have, want := p.indexed.Next(), p.rows.Next()
	if err := p.indexed.Err(); err != nil {
		p.err = err
		return false
	}
	if err := p.rows.Err(); err != nil {
		p.err = err
		return false
	}
	if have != want || (have && p.indexed.At() != p.rows.At()) {
		p.err = errors.Wrap(seriesindex.ErrCorrupt, "series index IDs differ from part rows")
		return false
	}
	p.done = !have
	return have
}
func (p *verifiedDiskIDs) Seek(id signal.SeriesID) bool {
	if p.err != nil || p.done {
		return false
	}
	// Seek walks the validated prefix too, so a skipped mismatch cannot become a false absence.
	if p.started && p.At().Compare(id) >= 0 {
		return true
	}
	for p.Next() {
		if p.At().Compare(id) >= 0 {
			return true
		}
	}
	return false
}
func (p *verifiedDiskIDs) At() signal.SeriesID { return p.indexed.At() }
func (p *verifiedDiskIDs) Err() error          { return p.err }

type diskPartIDs struct {
	ctx       context.Context //nolint:containedctx // Postings has no context parameter; the iterator owns its read lifetime.
	index     *pagedIndex
	pos, base int
	data      []byte
	id        signal.SeriesID
	err       error
}

func (p *diskPartIDs) Next() bool {
	if p.err != nil || p.pos+1 >= p.index.n {
		p.pos = p.index.n
		return false
	}
	p.pos++
	if p.data == nil || p.pos >= p.base+len(p.data)/sidxEntryW {
		p.base = p.pos
		n := min(indexPageEntries, p.index.n-p.pos)
		offset := p.index.offset + int64(p.pos*sidxEntryW)
		p.data, p.err = backend.ReadAtAppend(p.ctx, p.index.be, p.data[:0], p.index.key, offset, int64(n*sidxEntryW))
		if p.err != nil {
			return false
		}
		if len(p.data) != n*sidxEntryW {
			p.err = errors.Wrap(errSidxCorrupt, "short identity ID page")
			return false
		}
	}
	off := (p.pos - p.base) * sidxEntryW
	p.id = signal.SeriesID{Hi: binary.BigEndian.Uint64(p.data[off:]), Lo: binary.BigEndian.Uint64(p.data[off+8:])}
	return true
}
func (p *diskPartIDs) Seek(id signal.SeriesID) bool {
	if p.pos >= 0 && p.pos < p.index.n && p.id.Compare(id) >= 0 {
		return p.err == nil
	}
	for p.Next() {
		if p.id.Compare(id) >= 0 {
			return true
		}
	}
	return false
}
func (p *diskPartIDs) At() signal.SeriesID { return p.id }
func (p *diskPartIDs) Err() error          { return p.err }
