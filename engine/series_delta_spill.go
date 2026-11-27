package engine

import (
	"context"
	"strconv"
	"time"

	"github.com/go-faster/errors"

	"github.com/oteldb/storage/backend"
	"github.com/oteldb/storage/internal/buildbudget"
	"github.com/oteldb/storage/internal/partid"
	"github.com/oteldb/storage/internal/seriesindex"
	"github.com/oteldb/storage/internal/seriestable"
)

const seriesSpillFanIn = 8

type seriesDeltaSpill struct {
	key            string
	index          *seriesindex.Index
	lo, hi         int
	directoryBytes int64
}

type seriesDeltaSpills struct {
	be      backend.Backend
	prefix  string
	entries identitySnapshot
	reserve func(int64) error
	base    int64
	scratch int64
	serial  int
	active  map[string]struct{}
}

func (s *seriesDeltaSpills) key() string {
	s.serial++
	return s.prefix + "/" + strconv.Itoa(s.serial)
}

func (s *seriesDeltaSpills) cleanup(ctx context.Context) {
	ctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
	defer cancel()
	for key := range s.active {
		_ = s.be.Delete(ctx, key)
	}
}

func (s *seriesDeltaSpills) write(ctx context.Context, b *seriestable.Builder, lo, hi int) (seriesDeltaSpill, error) {
	dir := b.StreamingDirectoryBytes()
	if err := s.reserve(buildbudget.Add(s.base, buildbudget.Add(s.scratch, buildbudget.Add(b.Bytes(), dir)))); err != nil {
		return seriesDeltaSpill{}, err
	}
	key := s.key()
	s.active[key] = struct{}{}
	if err := b.WriteIndex(ctx, s.be, key); err != nil {
		return seriesDeltaSpill{}, err
	}
	index, err := seriesindex.Open(ctx, s.be, key)
	return seriesDeltaSpill{key: key, index: index, lo: lo, hi: hi, directoryBytes: dir}, err
}

func (s *seriesDeltaSpills) group(spills []seriesDeltaSpill, final bool) ([]*seriesindex.Index, int64, error) {
	indexes := make([]*seriesindex.Index, len(spills))
	var directory, inputs int64
	for i, spill := range spills {
		indexes[i] = spill.index
		directory = buildbudget.Add(directory, spill.directoryBytes)
		inputs = buildbudget.Add(inputs, buildbudget.Add(64<<10, buildbudget.Mul(spill.index.MaxPageBytes(), 8)))
	}
	needed := buildbudget.Add(s.base, buildbudget.Add(s.scratch, buildbudget.Add(directory, inputs)))
	if final {
		// The unframed ID column is built separately from the streamed index.
		needed = buildbudget.Add(needed, buildbudget.Mul(int64(len(s.entries.refs)), 128))
	}
	return indexes, directory, s.reserve(needed)
}

func (s *seriesDeltaSpills) reduce(ctx context.Context, spills []seriesDeltaSpill) ([]seriesDeltaSpill, error) {
	for len(spills) > seriesSpillFanIn {
		var next []seriesDeltaSpill
		for lo := 0; lo < len(spills); lo += seriesSpillFanIn {
			group := spills[lo:min(lo+seriesSpillFanIn, len(spills))]
			if len(group) == 1 {
				next = append(next, group[0])
				continue
			}
			indexes, directory, err := s.group(group, false)
			if err != nil {
				return nil, err
			}
			start, end := group[0].lo, group[len(group)-1].hi
			key := s.key()
			s.active[key] = struct{}{}
			if _, err := seriesindex.WriteContiguous(ctx, s.be, key, [32]byte{}, s.entries.view, s.entries.refs[start:end], indexes); err != nil {
				return nil, err
			}
			index, err := seriesindex.Open(ctx, s.be, key)
			if err != nil {
				return nil, err
			}
			next = append(next, seriesDeltaSpill{key: key, index: index, lo: start, hi: end, directoryBytes: directory})
			for _, consumed := range group {
				if err := s.be.Delete(ctx, consumed.key); err != nil {
					return nil, err
				}
				delete(s.active, consumed.key)
			}
		}
		spills = next
	}
	return spills, nil
}

func (e *Engine) writeNativeSeriesDelta(ctx context.Context, entries identitySnapshot, known []bool,
	snapshot *seriesSnapshot, base int64, reserve func(int64) error,
) error {
	if reserve == nil {
		reserve = func(int64) error { return ctx.Err() }
	}
	var missing int
	for _, exists := range known {
		if !exists {
			missing++
		}
	}
	if missing == 0 {
		return nil
	}
	base = buildbudget.Add(base, buildbudget.Mul(int64(missing), 4))
	if err := reserve(base); err != nil {
		return err
	}
	delta := identitySnapshot{view: entries.view, refs: make([]uint32, 0, missing)}
	var largest int64
	for i, ref := range entries.refs {
		if !known[i] {
			delta.refs = append(delta.refs, ref)
			single := buildbudget.Add(entries.view.IdentityWorkspace(int(ref)), entries.view.EncodingBytes(int(ref)))
			largest = max(largest, single)
		}
	}
	prefix := e.cfg.Prefix + "/s-" + partid.New().String()
	writerBytes, priced := backend.WriterMemory(e.cfg.Backend, prefix+"/series-index")
	if !priced {
		return errors.New("engine: separate series requires bounded writer memory")
	}
	s := seriesDeltaSpills{
		be: e.cfg.Backend, prefix: prefix + "/.postings", entries: delta, reserve: reserve, base: base,
		scratch: buildbudget.Add(buildbudget.Mul(writerBytes, 4), buildbudget.Mul(max(64<<10, largest), 160)),
		active:  make(map[string]struct{}),
	}
	defer s.cleanup(ctx)
	target := min(e.flushMemoryBytes(), 96<<20)
	b := seriestable.NewBorrowedBuilder(delta.view)
	var spills []seriesDeltaSpill
	lo := 0
	for i, ref := range delta.refs {
		single := buildbudget.Add(delta.view.IdentityWorkspace(int(ref)), delta.view.EncodingBytes(int(ref)))
		next := buildbudget.Add(b.Bytes(), buildbudget.Add(b.StreamingDirectoryBytes(), buildbudget.Mul(single, 32)))
		if b.Count() != 0 && next > target {
			spill, err := s.write(ctx, &b, lo, i)
			if err != nil {
				return err
			}
			spills = append(spills, spill)
			lo = i
			b = seriestable.NewBorrowedBuilder(delta.view)
			next = buildbudget.Add(b.StreamingDirectoryBytes(), buildbudget.Mul(single, 32))
		}
		if err := reserve(buildbudget.Add(base, buildbudget.Add(s.scratch, next))); err != nil {
			return err
		}
		if err := b.AddBorrowed(int(ref)); err != nil {
			return err
		}
	}
	if len(spills) == 0 {
		needed := buildbudget.Add(base, buildbudget.Add(s.scratch, buildbudget.Add(b.Bytes(), b.StreamingDirectoryBytes())))
		if err := reserve(needed); err != nil {
			return err
		}
		return e.installSeriesDelta(entries, snapshot, known, prefix, func() (*seriestable.Run, error) {
			return seriestable.WriteWithIDCache(ctx, e.cfg.Backend, prefix, &b, e.seriesIDs)
		})
	}
	spill, err := s.write(ctx, &b, lo, len(delta.refs))
	if err != nil {
		return err
	}
	b = seriestable.Builder{}
	spills, err = s.reduce(ctx, append(spills, spill))
	if err != nil {
		return err
	}
	indexes, _, err := s.group(spills, true)
	if err != nil {
		return err
	}
	return e.installSeriesDelta(entries, snapshot, known, prefix, func() (*seriestable.Run, error) {
		return seriestable.WriteContiguous(ctx, e.cfg.Backend, prefix, delta.view, delta.refs, indexes, e.seriesIDs)
	})
}
