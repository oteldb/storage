package engine

import (
	"context"
	"math/bits"
	"slices"

	"github.com/go-faster/errors"

	"github.com/oteldb/storage/backend/bucketindex"
	"github.com/oteldb/storage/index/postings"
	"github.com/oteldb/storage/index/series"
	"github.com/oteldb/storage/internal/readerpool"
	"github.com/oteldb/storage/internal/seriesindex"
	"github.com/oteldb/storage/internal/watermark"
	"github.com/oteldb/storage/signal"
	"github.com/oteldb/storage/wal"
)

const (
	defaultResidentHeadBytes = 128 << 20
	diskHeadBloomWords       = 1 << 17
	diskHeadBloomMaxWords    = 32 * diskHeadBloomWords
)

// ErrReplicaHeadFull asks replication to retry after the primary publishes more parts.
var ErrReplicaHeadFull = errors.New("engine: replica resident head is full; retry after primary part publication")

type diskHeadEntry struct {
	newest int64
	exists bool
}

type diskHeadState struct {
	complete bool
	count    int64
	bloom    diskHeadBloom
	cache    *diskSeriesCache
	pending  map[signal.SeriesID]diskHeadEntry
}

func newDiskHeadState() *diskHeadState {
	return newDiskHeadStateWithCache(defaultSeriesCacheBytes)
}

func newDiskHeadStateWithCache(budget int64) *diskHeadState {
	return &diskHeadState{
		complete: true,
		bloom:    newDiskHeadBloom(diskHeadBloomWords, diskHeadBloomMaxWords),
		cache:    newDiskSeriesCache(budget),
	}
}

type diskBloomSegment struct {
	words    []uint64
	inserted uint64
}

type diskHeadBloom struct {
	segments               []diskBloomSegment
	initialWords, maxWords int
}

func newDiskHeadBloom(initialWords, maxWords int) diskHeadBloom {
	return diskHeadBloom{initialWords: initialWords, maxWords: maxWords}
}

func (b *diskHeadBloom) add(id signal.SeriesID) {
	if len(b.segments) == 0 {
		b.segments = append(b.segments, diskBloomSegment{words: make([]uint64, b.initialWords)})
	}
	last := &b.segments[len(b.segments)-1]
	if last.inserted >= uint64(len(last.words)*64/10) && len(last.words) < b.maxWords {
		b.segments = append(b.segments, diskBloomSegment{words: make([]uint64, min(2*len(last.words), b.maxWords))})
		last = &b.segments[len(b.segments)-1]
	}
	h1, h2 := diskBloomHashes(id)
	mask := uint64(len(last.words)*64 - 1)
	for i := range uint64(4) {
		bit := (h1 + i*h2) & mask
		last.words[bit/64] |= uint64(1) << (bit % 64)
	}
	last.inserted++
}

func (b *diskHeadBloom) contains(id signal.SeriesID) bool {
	h1, h2 := diskBloomHashes(id)
	for _, segment := range b.segments {
		mask := uint64(len(segment.words)*64 - 1)
		found := true
		for i := range uint64(4) {
			bit := (h1 + i*h2) & mask
			if segment.words[bit/64]&(uint64(1)<<(bit%64)) == 0 {
				found = false
				break
			}
		}
		if found {
			return true
		}
	}
	return false
}

func (b *diskHeadBloom) sizeBytes() int64 {
	n := int64(cap(b.segments)) * 32
	for _, segment := range b.segments {
		n += int64(cap(segment.words)) * 8
	}
	return n
}

func diskBloomHashes(id signal.SeriesID) (uint64, uint64) {
	return mixDiskBloom(id.Lo ^ bits.RotateLeft64(id.Hi, 17)), mixDiskBloom(id.Hi^bits.RotateLeft64(id.Lo, 31)^0x9e3779b97f4a7c15) | 1
}

func mixDiskBloom(x uint64) uint64 {
	x ^= x >> 30
	x *= 0xbf58476d1ce4e5b9
	x ^= x >> 27
	x *= 0x94d049bb133111eb
	return x ^ (x >> 31)
}

func (s *diskHeadState) note(id signal.SeriesID)            { s.bloom.add(id) }
func (s *diskHeadState) mayContain(id signal.SeriesID) bool { return s.bloom.contains(id) }

func (s *diskHeadState) remember(id signal.SeriesID, newest int64) {
	s.cache.put(id, newest)
}

func (h *head) cardinality() int64 {
	if h.disk != nil {
		return h.disk.count
	}
	return int64(h.series.Len())
}

func (e *Engine) diskSeriesCountLocked() int64 { return e.head.cardinality() }

// rebuildDiskStateLocked recounts the streaming union after a catalog lifecycle change.
// The caller excludes writers and owns state; disk reads admit readers.
func (e *Engine) rebuildDiskStateLocked(ctx context.Context) (err error) {
	if !e.cfg.DiskSeries {
		return nil
	}
	defer func() {
		if err != nil {
			e.head.disk.complete = false
		}
	}()
	if e.diskHistoryGapLocked() != nil {
		e.head.disk.complete = false
		return nil
	}
	next := newDiskHeadStateWithCache(e.diskSeriesCacheBytes())
	headIDs := make([]signal.SeriesID, e.head.series.Len())
	view := e.head.series.View()
	for i := range headIDs {
		headIDs[i] = view.ID(i)
	}
	slices.SortFunc(headIDs, signal.SeriesID.Compare)
	parts := e.readablePartsLocked()
	var watermarks []diskHeadEntry
	if err := e.mu.WithReaders(func() error {
		its := []postings.Postings{postings.FromSlice(headIDs)}
		for _, p := range parts {
			it, err := p.diskSeriesIDs(ctx)
			if err != nil {
				return err
			}
			its = append(its, it)
		}
		it := postings.Merge(its...)
		for it.Next() {
			next.count++
			next.note(it.At())
		}
		if err := it.Err(); err != nil {
			return err
		}
		if len(headIDs) > 0 {
			var err error
			watermarks, err = e.diskWatermarksForHead(ctx, parts, headIDs)
			return err
		}
		return nil
	}); err != nil {
		return err
	}
	for i, entry := range watermarks {
		if entry.exists {
			ref, _ := e.head.series.Ref(headIDs[i])
			e.head.slots.set(e.head.stateRefs[ref], entry.newest)
			e.head.newest = max(e.head.newest, entry.newest)
		}
	}
	e.head.disk = next
	return nil
}

// preloadDiskSeriesLocked resolves historical membership before staged admission.
func (e *Engine) preloadDiskSeriesLocked(ids []signal.SeriesID) error {
	if !e.cfg.DiskSeries {
		return nil
	}
	e.head.disk.pending = nil
	if err := e.preloadDiskSeriesMoreLocked(ids); err != nil {
		e.head.disk.pending = nil
		return err
	}
	return nil
}

func (e *Engine) preloadOneDiskSeriesLocked(id signal.SeriesID) error {
	return e.preloadDiskSeriesMoreLocked([]signal.SeriesID{id})
}

func (e *Engine) preloadDiskSeriesMoreLocked(ids []signal.SeriesID) error {
	if !e.cfg.DiskSeries {
		return nil
	}
	return e.mu.WithReaders(func() error { return e.preloadDiskSeriesOffState(ids) })
}

func (e *Engine) preloadDiskSeriesOffState(ids []signal.SeriesID) error {
	s := e.head.disk
	if err := e.checkDiskAdmissionLocked(); err != nil {
		return err
	}
	if s.pending == nil {
		s.pending = make(map[signal.SeriesID]diskHeadEntry)
	}
	var historical []signal.SeriesID
	for _, id := range ids {
		if e.head.series.Has(id) {
			continue
		}
		if _, ok := s.pending[id]; ok {
			continue
		}
		if hit, ok := s.cache.get(id); ok {
			s.pending[id] = hit
			continue
		}
		s.pending[id] = diskHeadEntry{}
		if s.mayContain(id) {
			historical = append(historical, id)
		}
	}
	slices.SortFunc(historical, signal.SeriesID.Compare)
	return e.preloadHistoricalBatch(historical)
}

//nolint:contextcheck,nolintlint // Append has no context; scoped reader I/O stays off state.
func (e *Engine) preloadHistoricalBatch(ids []signal.SeriesID) error {
	if len(ids) == 0 {
		return nil
	}
	s := e.head.disk
	if err := lookupDiskSeries(e.readablePartsLocked(), ids, s.pending); err != nil {
		return err
	}
	for _, id := range ids {
		if entry := s.pending[id]; entry.exists {
			s.remember(id, entry.newest)
		}
	}
	return nil
}

//nolint:contextcheck,nolintlint // Append has no context; scoped reader I/O stays off state.
func lookupDiskSeries(parts []*part, ids []signal.SeriesID, pending map[signal.SeriesID]diskHeadEntry) (err error) {
	ctx, batch := readerpool.WithBatch(context.Background())
	defer func() {
		if closeErr := batch.Close(); err == nil {
			err = closeErr
		}
	}()
	var reader seriesindex.MetadataReader
	var coverage watermark.Cursor
	for _, p := range parts {
		if err := batch.Reset(); err != nil {
			return err
		}
		if err := p.visitDiskMetadataWindow(ctx, ids, &reader, &coverage, func(id signal.SeriesID, meta seriesindex.Metadata) {
			entry := pending[id]
			if !entry.exists || meta.Covered > entry.newest {
				entry.newest = meta.Covered
			}
			entry.exists = true
			pending[id] = entry
		}); err != nil {
			return errors.Wrap(err, "lookup historical series batch")
		}
	}
	return nil
}

func (e *Engine) clearDiskPendingLocked() {
	if e.head.disk != nil {
		e.head.disk.pending = nil
	}
}

// compactDiskHeadLocked drops metadata once its samples have immutable part ownership.
// The caller owns writer/state; disk-mode merges retain no head references.
func (e *Engine) compactDiskHeadLocked() {
	h := e.head
	if h.disk == nil || e.flushing != nil || len(h.samples) == h.series.Len() {
		return
	}
	next := newHeadWithContexts(e.cfg.SeriesContexts, e.cfg.SeriesTemplates)
	view := h.series.View()
	var attrs signal.Attributes
	for i := range view.Len() {
		id := view.ID(i)
		newest, seen := h.watermark(id)
		if _, ok := h.samples[id]; !ok {
			if seen {
				h.disk.remember(id, newest)
			}
			continue
		}
		ident := view.At(i, attrs)
		attrs = ident.Attributes
		next.register(series.Entry{ID: id, Series: ident})
		if seen {
			ref, _ := next.series.Ref(id)
			next.slots.set(next.stateRefs[ref], newest)
		}
	}
	next.disk = h.disk
	h.sym, h.series, h.post = next.sym, next.series, next.post
	h.nonStringLabels, h.stateRefs, h.slots = next.nonStringLabels, next.stateRefs, next.slots
	h.valueScratch = next.valueScratch
	h.resetBuffers()
	e.identityOrder.reset()
}

func (e *Engine) flushResidentHead() error {
	if !e.cfg.DiskSeries {
		return nil
	}
	e.mu.RLock()
	over := e.residentHeadOverBudgetLocked()
	e.mu.RUnlock()
	if over {
		return e.relieveResidentHeadPressure()
	}
	return nil
}

// flushResidentHeadLocked applies backpressure while preserving writer lock ownership.
func (e *Engine) flushResidentHeadLocked() error {
	if !e.cfg.DiskSeries {
		return nil
	}
	for e.residentHeadOverBudgetLocked() {
		e.mu.UnlockWriter()
		err := e.relieveResidentHeadPressure()
		e.mu.LockWriter()
		if err != nil {
			return err
		}
	}
	return nil
}

func (e *Engine) residentHeadOverBudgetLocked() bool {
	return e.head.bytes > 0 && e.head.residentIdentityBytes()+e.head.inFlightBytes() >= e.cfg.ResidentHeadBytes
}

//nolint:contextcheck,nolintlint // The context analyzer consumes this intentional API boundary before reporting.
func (e *Engine) relieveResidentHeadPressure() error {
	// A zero cluster term means this node has no claim and cannot publish a catalog.
	if e.cfg.Term == nil || e.term() != 0 {
		return e.Flush(context.Background())
	}
	if err := e.RefreshReplica(context.Background()); err != nil {
		return err
	}
	e.mu.RLock()
	over := e.residentHeadOverBudgetLocked()
	e.mu.RUnlock()
	if over {
		return ErrReplicaHeadFull
	}
	return nil
}

func (e *Engine) replayResidentGuard(samples int) error {
	if e.cfg.DiskSeries && e.head.residentIdentityBytes()+e.head.inFlightBytes()+int64(samples)*SampleBytes > e.cfg.ResidentHeadBytes {
		return errors.New("WAL replay exceeds resident head budget; journal preserved; increase ResidentHeadBytes to recover")
	}
	return nil
}

func (e *Engine) applyDiskReplicatedLocked(data []byte) error {
	defer e.walB.reset()
	byID := make(map[signal.SeriesID]signal.Series)
	admit := func(id signal.SeriesID, ts []int64, values, sf []float64) {
		if _, ok := byID[id]; !ok && !e.head.series.Has(id) {
			return
		}
		for i := range ts {
			weight := 1.0
			if sf != nil {
				weight = sf[i]
			}
			e.walB.admit(e.head, id, ts[i], values[i], weight, 0, AppendLimits{}, func() signal.Series { return byID[id] })
		}
	}
	if err := wal.Replay(data, wal.Handlers{
		OnSeries: func(id signal.SeriesID, s signal.Series) error {
			e.preloadAuthoritativeSeriesLocked(id)
			byID[id] = s
			return nil
		},
		OnSamples: func(id signal.SeriesID, ts []int64, values []float64) error { admit(id, ts, values, nil); return nil },
		OnSamplesSF: func(id signal.SeriesID, ts []int64, values, sf []float64) error {
			admit(id, ts, values, sf)
			return nil
		},
	}); err != nil {
		return err
	}
	e.walB.apply(e.head)
	return nil
}

func (e *Engine) diskOverflowLimits(limits AppendLimits, b *walBatch, stateHeld bool) AppendLimits {
	if !e.cfg.DiskSeries || limits.Overflow == nil {
		return limits
	}
	overflow := limits.Overflow
	limits.Overflow = func(s signal.Series) signal.Series {
		ov := overflow(s)
		var err error
		if stateHeld {
			err = e.preloadOneDiskSeriesLocked(ov.Hash())
		} else {
			err = e.preloadDiskSeriesOffState([]signal.SeriesID{ov.Hash()})
		}
		if err != nil {
			b.lookupErr = err
		}
		return ov
	}
	return limits
}

func (e *Engine) diskHistoryGapLocked() error {
	if e.hasWantsLocked() {
		return errors.Wrap(bucketindex.ErrFenced, "historical series metadata is unavailable while parts need repair")
	}
	for i := range e.foreign {
		entry := &e.foreign[i]
		if e.foreignParts[entry.Prefix] == nil {
			return errors.Wrapf(bucketindex.ErrFenced, "historical series metadata is unavailable for adopted part %q", entry.Prefix)
		}
	}
	return nil
}

func (e *Engine) diskSeriesCompleteLocked() bool {
	if !e.cfg.DiskSeries {
		return true
	}
	if !e.head.disk.complete || e.loadErr != nil || e.hasWantsLocked() {
		return false
	}
	for i := range e.foreign {
		entry := &e.foreign[i]
		if e.foreignParts[entry.Prefix] == nil {
			return false
		}
	}
	return true
}

func (e *Engine) checkDiskAdmissionLocked() error {
	if e.cfg.DiskSeries && !e.diskSeriesCompleteLocked() {
		return errors.Wrap(bucketindex.ErrFenced, "historical series state is incomplete")
	}
	return nil
}

// preloadAuthoritativeSeriesLocked preserves accepted writes even while history needs repair.
func (e *Engine) preloadAuthoritativeSeriesLocked(id signal.SeriesID) {
	if !e.cfg.DiskSeries {
		return
	}
	if !e.diskSeriesCompleteLocked() {
		e.head.disk.complete = false
		return
	}
	if err := e.preloadOneDiskSeriesLocked(id); err != nil {
		e.clearDiskPendingLocked()
		e.head.disk.complete = false
	}
}

func (e *Engine) diskWatermarksForHead(ctx context.Context, parts []*part, ids []signal.SeriesID) ([]diskHeadEntry, error) {
	bounds := make([]diskHeadEntry, len(ids))
	for i, id := range ids {
		newest, seen := e.head.watermark(id)
		bounds[i] = diskHeadEntry{newest: newest, exists: seen}
	}
	var reader seriesindex.MetadataReader
	for _, p := range parts {
		if err := p.visitDiskMetadata(ctx, ids, &reader, func(id signal.SeriesID, meta seriesindex.Metadata) {
			i, found := slices.BinarySearchFunc(ids, id, signal.SeriesID.Compare)
			if !found {
				return
			}
			entry := &bounds[i]
			if !entry.exists || meta.Covered > entry.newest {
				entry.newest = meta.Covered
			}
			entry.exists = true
		}); err != nil {
			return nil, err
		}
	}
	return bounds, nil
}

func (e *Engine) diskSeriesCacheBytes() int64 {
	if e.cfg.SeparateSeries {
		return e.cfg.SeriesCacheBytes - e.cfg.SeriesCacheBytes/8
	}
	return e.cfg.SeriesCacheBytes
}
