package engine

import (
	"bufio"
	"context"
	"encoding/binary"
	"hash/crc32"
	"math"
	"time"

	"github.com/go-faster/errors"

	"github.com/oteldb/storage/backend"
)

const (
	seriesMetadataMemoryLimit = 32 << 10
	metadataSpoolBuffer       = 64 << 10
	metadataSpoolWidth        = 44
)

var metadataSpoolCRC = crc32.MakeTable(crc32.Castagnoli)

type seriesMetadataSpool struct {
	key       string
	writer    backend.ObjectWriter
	buf       *bufio.Writer
	committed bool
	scratch   [metadataSpoolWidth]byte
}

func (p *partStreamWriter) nextSeriesMetadata() error {
	if p.ordinal != nil {
		return p.appendOrdinalMetadata()
	}
	if p.metaWorkspace == 0 {
		return nil
	}
	if p.meta == nil {
		if len(p.covered) < seriesMetadataMemoryLimit {
			return nil
		}
		key := p.prefix + "/.series-meta"
		w, err := backend.CreateObjectDeferred(p.metaCtx, p.e.cfg.Backend, key)
		if err != nil {
			return err
		}
		p.meta = &seriesMetadataSpool{key: key, writer: w, buf: bufio.NewWriterSize(w, metadataSpoolBuffer)}
	}
	for i, covered := range p.covered {
		var agg SeriesAgg
		if p.withStats {
			agg = p.stats[i]
		}
		if err := p.meta.append(covered, agg); err != nil {
			return err
		}
	}
	if cap(p.covered) > 1 {
		p.covered = nil
		p.stats = nil
	} else {
		p.covered = p.covered[:0]
		p.stats = p.stats[:0]
	}
	return nil
}

func (s *seriesMetadataSpool) append(covered int64, agg SeriesAgg) error {
	raw := &s.scratch
	binary.BigEndian.PutUint64(raw[0:], uint64(covered))
	binary.BigEndian.PutUint64(raw[8:], math.Float64bits(agg.Count))
	binary.BigEndian.PutUint64(raw[16:], math.Float64bits(agg.Sum))
	binary.BigEndian.PutUint64(raw[24:], math.Float64bits(agg.Min))
	binary.BigEndian.PutUint64(raw[32:], math.Float64bits(agg.Max))
	binary.BigEndian.PutUint32(raw[40:], crc32.Checksum(raw[:40], metadataSpoolCRC))
	_, err := s.buf.Write(raw[:])
	return err
}

func (p *partStreamWriter) openSeriesMetadata(ctx context.Context) (backend.ObjectReader, error) {
	if err := p.nextSeriesMetadata(); err != nil {
		return nil, err
	}
	s := p.meta
	if err := s.buf.Flush(); err != nil {
		return nil, err
	}
	s.buf = nil
	// A failed commit can be ambiguous. The temporary key is ours in either case.
	s.committed = true
	if err := s.writer.Commit(ctx); err != nil {
		return nil, err
	}
	r, supported, err := backend.OpenReader(ctx, p.e.cfg.Backend, s.key)
	if err != nil {
		return nil, err
	}
	if !supported {
		return nil, errors.New("engine: metadata spool lost seekable reader")
	}
	if r.Size() != int64(len(p.runs))*metadataSpoolWidth {
		_ = r.Close()
		return nil, errors.New("engine: metadata spool length mismatch")
	}
	return r, nil
}

func (p *partStreamWriter) dropSeriesMetadata(ctx context.Context) {
	if m := p.ordinal; m != nil {
		p.ordinal = nil
		if m.covered != nil {
			m.covered.Abort()
		}
		if m.stats != nil {
			m.stats.Abort()
		}
	}
	if s := p.meta; s != nil {
		p.meta = nil
		s.writer.Abort()
		if s.committed {
			ctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
			defer cancel()
			_ = p.e.cfg.Backend.Delete(ctx, s.key)
		}
	}
}

func readSeriesMetadata(r *bufio.Reader) (int64, SeriesAgg, error) {
	raw, err := r.Peek(metadataSpoolWidth)
	if err != nil {
		return 0, SeriesAgg{}, err
	}
	if crc32.Checksum(raw[:40], metadataSpoolCRC) != binary.BigEndian.Uint32(raw[40:]) {
		return 0, SeriesAgg{}, errors.New("engine: metadata spool checksum mismatch")
	}
	covered := int64(binary.BigEndian.Uint64(raw[0:]))
	agg := SeriesAgg{
		Count: math.Float64frombits(binary.BigEndian.Uint64(raw[8:])),
		Sum:   math.Float64frombits(binary.BigEndian.Uint64(raw[16:])),
		Min:   math.Float64frombits(binary.BigEndian.Uint64(raw[24:])),
		Max:   math.Float64frombits(binary.BigEndian.Uint64(raw[32:])),
	}
	_, err = r.Discard(metadataSpoolWidth)
	return covered, agg, err
}
