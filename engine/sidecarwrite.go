package engine

import (
	"context"
	"encoding/binary"
	"math"

	"github.com/go-faster/errors"

	"github.com/oteldb/storage/backend"
	"github.com/oteldb/storage/encoding/chunk"
	"github.com/oteldb/storage/internal/buildbudget"
	"github.com/oteldb/storage/internal/sidecar"
	"github.com/oteldb/storage/internal/watermark"
)

func (p *partStreamWriter) sidecarWorkspace() (int64, bool) {
	if !backend.StreamsWrites(p.e.cfg.Backend) {
		return 0, false
	}
	var largest int64
	for _, key := range []string{sidxKey(p.prefix), watermark.Key(p.prefix), statsKey(p.prefix)} {
		n, known := backend.WriterMemory(p.e.cfg.Backend, key)
		if !known {
			return 0, false
		}
		largest = max(largest, n)
	}
	return buildbudget.Add(largest, sidecar.ScratchBytes+256), true
}

func (p *partStreamWriter) writeSeriesSidecars(ctx context.Context) error {
	if p.sidecarWriterBytes == 0 {
		if err := backend.WriteDeferred(ctx, p.e.cfg.Backend, sidxKey(p.prefix), encodeSeriesIndexRuns(p.runs)); err != nil {
			return err
		}
		marks := watermark.EncodeEntries(nil, len(p.runs), p.watermarkAt)
		if err := backend.WriteDeferred(ctx, p.e.cfg.Backend, watermark.Key(p.prefix), marks); err != nil {
			return err
		}
		if p.withStats && !p.sampled {
			return backend.WriteDeferred(ctx, p.e.cfg.Backend, statsKey(p.prefix), encodeSeriesStatsEntries(len(p.runs), p.runIDAt, p.stats))
		}
		return nil
	}
	if needed, known := p.sidecarWorkspace(); !known || needed > p.sidecarWriterBytes {
		return errors.New("engine: sidecar writer workspace changed")
	}
	var scratch [32]byte
	header := binary.BigEndian.AppendUint32(scratch[:0], sidxMagic)
	header = append(header, sidxVersion)
	header = binary.AppendUvarint(header, uint64(len(p.runs)))
	header = binary.AppendUvarint(header, uint64(p.rows))
	start := 0
	if err := sidecar.Write(ctx, p.e.cfg.Backend, sidxKey(p.prefix), header, len(p.runs), sidxEntryW, func(dst []byte, i int) []byte {
		r := p.runs[i]
		dst = binary.BigEndian.AppendUint64(dst, r.Value.Hi)
		dst = binary.BigEndian.AppendUint64(dst, r.Value.Lo)
		dst = binary.BigEndian.AppendUint32(dst, uint32(start))
		start += r.Count
		return dst
	}); err != nil {
		return err
	}
	if err := watermark.WriteEntries(ctx, p.e.cfg.Backend, watermark.Key(p.prefix), len(p.runs), p.watermarkAt); err != nil {
		return err
	}
	if p.withStats && !p.sampled {
		header = binary.BigEndian.AppendUint32(scratch[:0], statsMagic)
		header = binary.AppendUvarint(header, uint64(len(p.runs)))
		return sidecar.Write(ctx, p.e.cfg.Backend, statsKey(p.prefix), header, len(p.runs),
			16+binary.MaxVarintLen64+3*8, func(dst []byte, i int) []byte {
				r, a := p.runs[i], p.stats[i]
				dst = binary.BigEndian.AppendUint64(dst, r.Value.Hi)
				dst = binary.BigEndian.AppendUint64(dst, r.Value.Lo)
				dst = binary.AppendVarint(dst, int64(a.Count))
				dst = binary.BigEndian.AppendUint64(dst, math.Float64bits(a.Sum))
				dst = binary.BigEndian.AppendUint64(dst, math.Float64bits(a.Min))
				return binary.BigEndian.AppendUint64(dst, math.Float64bits(a.Max))
			})
	}
	return nil
}

func (p *partStreamWriter) watermarkAt(i int) watermark.Entry {
	return watermark.Entry{ID: u128ToID(p.runs[i].Value), Max: p.covered[i]}
}
func (p *partStreamWriter) runIDAt(i int) chunk.U128 { return p.runs[i].Value }
