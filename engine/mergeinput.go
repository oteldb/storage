package engine

import (
	"context"
	"encoding/binary"
	"math"

	"github.com/go-faster/errors"

	"github.com/oteldb/storage/backend"
	"github.com/oteldb/storage/block"
	"github.com/oteldb/storage/encoding/compress"
	"github.com/oteldb/storage/internal/buildbudget"
	"github.com/oteldb/storage/internal/memsize"
	"github.com/oteldb/storage/internal/watermark"
	"github.com/oteldb/storage/signal"
)

var errMergeInputMemory = errors.New("engine: merge inputs exceed memory budget")

type mergeInputBound struct {
	resident, transient int64
}

func (b *mergeInputBound) bytes() int64 { return buildbudget.Add(b.resident, b.transient) }

type boundedMergeSelection struct {
	parts              []*part
	tiers, conflicting []DownsampleTier
	inputBytes         int64
	allowance          int64
}

func (e *Engine) selectBoundedMerge(ctx context.Context, cohorts []mergeCohort, readable []*part,
	opts MergeOptions, capBytes int64, idle int, turn uint64,
) (boundedMergeSelection, error) {
	return e.selectBoundedMergeWithAllowance(ctx, cohorts, readable, opts, capBytes, idle, turn, e.mergeMemoryBudgetBytes())
}

func (e *Engine) selectBoundedMergeWithAllowance(ctx context.Context, cohorts []mergeCohort, readable []*part,
	opts MergeOptions, capBytes int64, idle int, turn uint64, limit int64,
) (boundedMergeSelection, error) {
	return e.selectMergeInputsWithWatermarks(ctx, cohorts, readable, opts, capBytes, idle, turn, limit, nil)
}

func (e *Engine) selectMergeInputsWithWatermarks(ctx context.Context, cohorts []mergeCohort, readable []*part,
	opts MergeOptions, capBytes int64, idle int, turn uint64, limit int64, fallback map[*part]bool,
) (boundedMergeSelection, error) {
	maxInputs := maxMergeParts
	start := minInt64
	if opts.RetainFrom > 0 {
		start = opts.RetainFrom
	}
	for {
		parts, tiers, conflicting := cohortRunLimited(cohorts, readable, opts, capBytes, idle, turn, maxInputs)
		selection := boundedMergeSelection{parts: parts, tiers: tiers, conflicting: conflicting, allowance: limit}
		if len(parts) == 0 {
			if maxInputs < maxMergeParts {
				return boundedMergeSelection{}, errMergeInputMemory
			}
			return selection, nil
		}
		if limit == math.MaxInt64 {
			return selection, nil
		}
		whole := len(parts) == 1 && !splitsOutput(parts, start)
		bound, err := e.mergeInputBoundWithWatermarks(ctx, parts, whole, fallback)
		if err != nil {
			return boundedMergeSelection{}, err
		}
		bound.resident = buildbudget.Add(bound.resident, mergeSeriesScratchBound(parts, tiers))
		floor := e.minimumMergeOutput(parts, capBytes, opts, tiers)
		if whole && buildbudget.Add(bound.bytes(), floor) <= limit {
			floor = buildbudget.Add(floor, e.wholeMergeOutputBound(parts[0], capBytes, opts, tiers))
		}
		if buildbudget.Add(bound.bytes(), floor) <= limit && (len(parts) <= minMergeParts || bound.bytes() <= limit/2) {
			selection.inputBytes = bound.bytes()
			return selection, nil
		}
		if len(parts) <= minMergeParts {
			return boundedMergeSelection{}, errors.Wrapf(errMergeInputMemory,
				"%d inputs need %d bytes plus %d output bytes; limit %d", len(parts), bound.bytes(), floor, limit)
		}
		maxInputs = max(minMergeParts, len(parts)/2)
	}
}

func (e *Engine) wholeMergeOutputBound(p *part, capBytes int64, opts MergeOptions, tiers []DownsampleTier) int64 {
	profile, _, _ := mergeEncoding([]*part{p}, capBytes, opts, tiers)
	codec := compress.NewCompressor(profile.Algorithm, profile.Level)
	e.mu.RLock()
	view := e.head.series.View()
	e.mu.RUnlock()
	var encoded, materialized int64
	for i := range view.Len() {
		encoded = max(encoded, view.EncodingBytes(i))
		materialized = max(materialized, view.MaterializationBytes(i))
	}
	n := buildbudget.Add(buildbudget.Mul(int64(p.rows()), 128), buildbudget.Mul(codec.EncodeWorkspace(), 4))
	identities := int64(p.index.seriesCount())
	n = buildbudget.Add(n, buildbudget.Mul(identities,
		buildbudget.Add(1024+memsize.MapEntry[signal.SeriesID, uint32](), buildbudget.Mul(encoded, 4))))
	return buildbudget.Add(n, buildbudget.Add(materialized, view.EncodingWorkspace()))
}

func (e *Engine) minimumMergeOutput(src []*part, capBytes int64, opts MergeOptions, tiers []DownsampleTier) int64 {
	profile, _, _ := mergeEncoding(src, capBytes, opts, tiers)
	coders := newMetricMergeCoders(src, profile)
	rows := e.cfg.MetricBlockRows
	if rows <= 0 {
		rows = DefaultMetricBlockRows
	}
	// Four typed columns have at most one bounded frame and pending granule each; codec pools are separate.
	return buildbudget.Add(buildbudget.Add(4<<20, buildbudget.Mul(int64(rows), 64)),
		coders.workspace())
}

func (e *Engine) mergeInputBound(ctx context.Context, src []*part, whole bool) (mergeInputBound, error) {
	return e.mergeInputBoundWithWatermarks(ctx, src, whole, nil)
}

func (e *Engine) mergeInputBoundWithWatermarks(
	ctx context.Context, src []*part, whole bool, fallback map[*part]bool,
) (mergeInputBound, error) {
	var bound mergeInputBound
	for _, p := range src {
		one, err := e.partMergeInputBound(ctx, p, whole, fallback[p])
		if err != nil {
			return mergeInputBound{}, errors.Wrapf(err, "price merge input %q", p.prefix)
		}
		bound.resident = buildbudget.Add(bound.resident, one.resident)
		bound.transient = max(bound.transient, one.transient)
	}
	return bound, nil
}

func (e *Engine) partMergeInputBound(ctx context.Context, p *part, whole, fallback bool) (mergeInputBound, error) {
	bound := mergeInputBound{resident: buildbudget.Add(4<<10, p.reader.ResidentBytes())}
	index, err := mergeIndexBound(ctx, p)
	if err != nil {
		return mergeInputBound{}, err
	}
	bound.include(index)
	watermarks, err := mergeWatermarkBoundWithFallback(ctx, p, fallback)
	if err != nil {
		return mergeInputBound{}, err
	}
	bound.include(watermarks)
	columns, err := e.mergeColumnBound(ctx, p, whole)
	if err != nil {
		return mergeInputBound{}, err
	}
	bound.include(columns)
	if whole {
		bound.resident = buildbudget.Add(bound.resident, buildbudget.Mul(int64(p.rows()), 64))
	}
	return bound, nil
}

func (b *mergeInputBound) include(other mergeInputBound) {
	b.resident = buildbudget.Add(b.resident, other.resident)
	b.transient = max(b.transient, other.transient)
}

func mergeIndexBound(ctx context.Context, p *part) (mergeInputBound, error) {
	index := p.index.paged
	if index == nil {
		return mergeInputBound{resident: memsize.Slice(p.index.ids) + memsize.Slice(p.index.starts)}, nil
	}
	bytes := buildbudget.Add(32, buildbudget.Mul(int64(index.n), sidxEntryW))
	var transient int64
	if view := index.view.Load(); view != nil {
		bytes = max(bytes, memsize.Slice(*view))
	} else {
		size, err := mergeSidecarBytes(ctx, p.be, index.key, bytes)
		if err != nil {
			return mergeInputBound{}, err
		}
		bytes = max(bytes, size)
		transient = buildbudget.Mul(bytes, 2)
	}
	return mergeInputBound{resident: buildbudget.Add(bytes, memsize.Of[pagedIndex]()+memsize.Slice(index.fences)),
		transient: transient}, nil
}

func mergeWatermarkBound(ctx context.Context, p *part) (mergeInputBound, error) {
	return mergeWatermarkBoundWithFallback(ctx, p, false)
}

func mergeWatermarkBoundWithFallback(ctx context.Context, p *part, fallback bool) (mergeInputBound, error) {
	p.seriesMaxMu.Lock()
	cached := p.seriesMax != nil
	bytes := memsize.Slice(p.seriesMax)
	p.seriesMaxMu.Unlock()
	if cached {
		return mergeInputBound{resident: bytes}, nil
	}
	bytes = buildbudget.Mul(int64(p.index.seriesCount()), memsize.Of[watermark.Entry]())
	size, err := mergeSidecarBytes(ctx, p.be, watermark.Key(p.prefix), buildbudget.Add(8+binary.MaxVarintLen64, bytes))
	if err != nil {
		return mergeInputBound{}, err
	}
	transient := buildbudget.Mul(size, 3)
	if fallback {
		whole, _, err := wholeMergeColumnBytes(p, colTs)
		if err != nil {
			return mergeInputBound{}, err
		}
		transient = max(transient, whole)
	}
	// A concurrent immutable cache fill and this local preflight can coexist until acceptance.
	return mergeInputBound{resident: buildbudget.Mul(bytes, 2), transient: transient}, nil
}

func (e *Engine) mergeColumnBound(ctx context.Context, p *part, whole bool) (mergeInputBound, error) {
	var bound mergeInputBound
	var sourceWide int64
	for _, name := range []string{colTs, colValue, colSF} {
		if name == colSF && !p.hasSF {
			continue
		}
		desc, ok := p.reader.ColumnDescByName(name)
		if !ok {
			return mergeInputBound{}, errors.Wrapf(errMergeInputMemory, "missing column %q", name)
		}
		if !whole && desc.Const {
			continue
		}
		if !whole {
			scan, supported, err := e.scanMergeColumnBound(ctx, p, name, desc)
			if err != nil {
				return mergeInputBound{}, err
			}
			if supported {
				bound.include(scan)
				continue
			}
		}
		bytes, shared, err := wholeMergeColumnBytes(p, name)
		if err != nil {
			return mergeInputBound{}, err
		}
		if shared > 0 && bytes < math.MaxInt64 {
			bytes = buildbudget.Add(bytes-shared, max(shared-sourceWide, 0))
			sourceWide = max(sourceWide, shared)
		}
		bound.resident = buildbudget.Add(bound.resident, bytes)
	}
	return bound, nil
}

func (e *Engine) scanMergeColumnBound(ctx context.Context, p *part, name string, desc block.ColumnDesc) (mergeInputBound, bool, error) {
	steady, open, ok := p.reader.ScanBound(ctx, name, e.mergeReadWindow)
	if !ok {
		return mergeInputBound{}, false, nil
	}
	if steady < 0 || open < 0 {
		return mergeInputBound{}, false, errMergeInputMemory
	}
	if desc.Compress != compress.AlgorithmNone {
		open = buildbudget.Add(open, compress.DecodeWorkspace)
	}
	return mergeInputBound{resident: steady, transient: open}, true, nil
}

func wholeMergeColumnBytes(p *part, name string) (bytes, shared int64, err error) {
	in, err := p.reader.ColumnInputSize(name)
	if err != nil {
		return 0, 0, errors.Wrap(err, "price merge column")
	}
	desc, ok := p.reader.ColumnDescByName(name)
	if !ok {
		return 0, 0, errMergeInputMemory
	}
	if desc.Const {
		return buildbudget.Mul(int64(p.rows()), 16), 0, nil
	}
	if in.WholeResident == math.MaxInt64 {
		return 0, 0, errors.Wrapf(errMergeInputMemory, "cannot bound whole column %q", name)
	}
	n := buildbudget.Add(in.WholeResident, buildbudget.Mul(max(desc.Bytes, 0), 3))
	if desc.Compress != compress.AlgorithmNone {
		n = buildbudget.Add(n, compress.DecodeWorkspace)
	}
	if in.SourceWide {
		shared = in.WholeResident
	}
	return n, shared, nil
}

func mergeSidecarBytes(ctx context.Context, b backend.Backend, key string, canonical int64) (int64, error) {
	n, known, err := backend.NativeSizeOf(ctx, b, key)
	if errors.Is(err, backend.ErrNotExist) {
		return 0, nil
	}
	if err != nil {
		return 0, errors.Wrap(err, "size merge sidecar")
	}
	if !known {
		return canonical, nil
	}
	if n < 0 {
		return 0, errMergeInputMemory
	}
	return n, nil
}
