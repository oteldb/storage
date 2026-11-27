package engine

import (
	"bytes"
	"context"

	"github.com/go-faster/errors"

	"github.com/oteldb/storage/backend/bucketindex"
	"github.com/oteldb/storage/index/postings"
	"github.com/oteldb/storage/internal/buildbudget"
	"github.com/oteldb/storage/internal/partid"
	"github.com/oteldb/storage/internal/seriesindex"
	"github.com/oteldb/storage/internal/seriestable"
	"github.com/oteldb/storage/signal"
)

type descriptorOrdinal struct {
	id     signal.SeriesID
	ref    uint32
	source uint8
}

type rawDescriptorMerge struct {
	e         *Engine
	readers   []*seriestable.Reader
	indexes   []*seriesindex.Index
	postings  *seriesindex.PostingMerge
	winners   []descriptorOrdinal
	used      int64
	largest   int64
	allowance int64
	roots     []*seriesRootHandle
}

func (e *Engine) buildRawSeriesMerge(ctx context.Context, sources []*seriesRootHandle, allowance int64) (_ []*seriesRootHandle, err error) {
	m := rawDescriptorMerge{
		e: e, readers: make([]*seriestable.Reader, len(sources)),
		indexes: make([]*seriesindex.Index, len(sources)), allowance: allowance,
	}
	ids := make([]postings.Postings, len(sources))
	positions := make([]uint32, len(sources))
	live := make([]bool, len(sources))
	defer func() {
		for _, r := range m.readers {
			_ = r.Close()
		}
		if err != nil {
			e.discardSeriesOutputs(ctx, m.roots)
		}
	}()
	for i, source := range sources {
		m.indexes[i] = source.run.PostingIndex()
		m.readers[i], err = source.run.NewReader(ctx)
		if err != nil {
			return nil, err
		}
		m.readers[i].StreamPages()
		ids[i] = m.readers[i].IDs(ctx)
		live[i] = ids[i].Next()
	}
	m.reset(ctx)
	for {
		selected := -1
		for i, ok := range live {
			if ok && (selected < 0 || ids[i].At().Compare(ids[selected].At()) < 0) {
				selected = i
			}
		}
		if selected < 0 {
			break
		}
		id, ref := ids[selected].At(), positions[selected]
		if err := m.append(ctx, id, selected, ref, sources[selected].run.MaxCanonicalBytes()); err != nil {
			return nil, err
		}
		positions[selected]++
		live[selected] = ids[selected].Next()
	}
	for _, cursor := range ids {
		if err := cursor.Err(); err != nil {
			return nil, err
		}
	}

	if err := m.flush(ctx); err != nil {
		return nil, err
	}
	return m.roots, nil
}

func (m *rawDescriptorMerge) reset(ctx context.Context) {
	m.winners = m.winners[:0]
	m.postings = seriesindex.NewPostingMerge(ctx, m.indexes)
	m.used, m.largest = 0, 0
}

func (m *rawDescriptorMerge) append(ctx context.Context, id signal.SeriesID, source int, ref uint32, maxCanonical int64) error {
	n := len(m.winners)
	if n != 0 && m.winners[n-1].id == id {
		entry := m.winners[n-1]
		winner, err := m.readers[entry.source].LookupOrdinal(id, entry.ref)
		if err != nil {
			return err
		}
		candidate, err := m.readers[source].LookupOrdinal(id, ref)
		if err != nil {
			return err
		}
		if !bytes.Equal(winner, candidate) {
			return errors.New("engine: conflicting compacted series identity")
		}
		// Equal descriptors have identical label membership. The winner's mapping suffices.
		return nil
	}

	// Price references, ID-column finishing, retained posting names, and directory
	// keys conservatively. Page and codec scratch are paid once per output.
	cost := buildbudget.Add(512, buildbudget.Mul(maxCanonical, 4))
	largest := max(m.largest, maxCanonical)
	need := buildbudget.Add(buildbudget.Add(m.used, cost), buildbudget.Mul(max(64<<10, largest), 32))
	if need > m.allowance {
		if err := m.flush(ctx); err != nil {
			return err
		}
		largest = maxCanonical
		if buildbudget.Add(cost, buildbudget.Mul(max(64<<10, largest), 32)) > m.allowance {
			return errors.New("engine: series identity exceeds merge allowance")
		}
	}
	if err := m.postings.Append(source, ref, uint32(len(m.winners))); err != nil {
		return err
	}
	m.winners = append(m.winners, descriptorOrdinal{id: id, ref: ref, source: uint8(source)})
	m.used = buildbudget.Add(m.used, cost)
	m.largest = largest
	return nil
}

func (m *rawDescriptorMerge) flush(ctx context.Context) error {
	if len(m.winners) == 0 {
		return nil
	}
	prefix := m.e.cfg.Prefix + "/s-" + partid.New().String()
	run, err := seriestable.WriteOrdinalMerge(ctx, m.e.cfg.Backend, prefix, len(m.winners), m.postings,
		func(i int) signal.SeriesID { return m.winners[i].id },
		func(i int) ([]byte, error) {
			entry := m.winners[i]
			return m.readers[entry.source].LookupOrdinal(entry.id, entry.ref)
		}, m.e.seriesIDs)
	if err != nil {
		m.e.deletePrivateSeriesRun(ctx, prefix)
		return err
	}
	h := &seriesRootHandle{binding: bucketindex.SeriesRoot{Prefix: prefix, ManifestDigest: run.Digest()}, run: run}
	h.acquire()
	m.roots = append(m.roots, h)
	m.reset(ctx)
	return nil
}
