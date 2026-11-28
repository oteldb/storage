package engine

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"hash/crc32"
	"math"

	"github.com/go-faster/errors"

	"github.com/oteldb/storage/backend"
	"github.com/oteldb/storage/block"
	"github.com/oteldb/storage/internal/buildbudget"
	"github.com/oteldb/storage/internal/sidecar"
	"github.com/oteldb/storage/internal/watermark"
	"github.com/oteldb/storage/signal"
)

const statsOrdinalMagic = 0x5354414f // STAO.

type ordinalMetadata struct {
	covered *sidecar.OrdinalWriter
	stats   *sidecar.OrdinalWriter
}

func (p *partStreamWriter) ordinalWorkspace() (n int64, known bool) {
	keys := []string{watermark.Key(p.prefix)}
	if p.withStats {
		keys = append(keys, statsKey(p.prefix))
	}
	for _, key := range keys {
		bytes, known := backend.WriterMemory(p.e.cfg.Backend, key)
		if !known {
			return 0, false
		}
		n = buildbudget.Add(n, buildbudget.Add(bytes, sidecar.ScratchBytes))
	}
	return n, true
}

func (p *partStreamWriter) appendOrdinalMetadata() error {
	if len(p.covered) == 0 {
		return nil
	}
	ctx, b, m := p.metaCtx, p.e.cfg.Backend, p.ordinal
	var err error
	if m.covered == nil {
		m.covered, err = sidecar.OpenOrdinalWriter(ctx, b, watermark.Key(p.prefix), watermark.OrdinalMagic)
		if err != nil {
			return err
		}
	}
	var raw [binary.MaxVarintLen64 + 24]byte
	if err := m.covered.Append(ctx, binary.BigEndian.AppendUint64(raw[:0], uint64(p.covered[0]))); err != nil {
		return err
	}
	if p.withStats && !p.sampled {
		if m.stats == nil {
			m.stats, err = sidecar.OpenOrdinalWriter(ctx, b, statsKey(p.prefix), statsOrdinalMagic)
			if err != nil {
				return err
			}
		}
		if err := m.stats.Append(ctx, appendStatsValues(raw[:0], p.stats[0])); err != nil {
			return err
		}
	}
	p.covered, p.stats = p.covered[:0], p.stats[:0]
	return nil
}

func (p *partStreamWriter) finishOrdinalMetadata(ctx context.Context) error {
	if err := p.appendOrdinalMetadata(); err != nil {
		return err
	}
	binding, err := block.BindManifest(ctx, p.e.cfg.Backend, p.prefix)
	if err != nil {
		return err
	}
	crc, err := p.writtenSeriesIndexCRC(ctx)
	if err != nil {
		return err
	}
	p.indexCRC = crc
	binding = bindOrdinalMetadata(binding, crc)
	if err := p.ordinal.covered.Commit(ctx, binding, len(p.runs)); err != nil {
		return err
	}
	if p.ordinal.stats != nil && !p.sampled {
		return p.ordinal.stats.Commit(ctx, binding, len(p.runs))
	}
	return nil
}

func (p *partStreamWriter) writtenSeriesIndexCRC(ctx context.Context) ([4]byte, error) {
	var header [2 * binary.MaxVarintLen64]byte
	raw := binary.AppendUvarint(header[:0], uint64(len(p.runs)))
	raw = binary.AppendUvarint(raw, uint64(p.rows))
	_, width := p.outputSeriesIndexShape()
	offset := int64(5+len(raw)) + int64(len(p.runs))*int64(width)
	crc, err := backend.ReadAt(ctx, p.e.cfg.Backend, sidxKey(p.prefix), offset, 4)
	if err != nil {
		return [4]byte{}, err
	}
	if len(crc) != 4 {
		return [4]byte{}, errors.Wrap(errSidxCorrupt, "missing index binding")
	}
	return [4]byte(crc), nil
}

func (p *part) decodePartStats(ctx context.Context, raw []byte) (map[signal.SeriesID]SeriesAgg, error) {
	if len(raw) < 4 || binary.BigEndian.Uint32(raw) != statsOrdinalMagic {
		return decodeSeriesStats(raw)
	}
	n := p.index.seriesCount()
	body, err := sidecar.OrdinalBody(raw, p.ordinalMetadataBinding(), n)
	if err != nil || n > len(body)/25 {
		return nil, errStatsCorrupt
	}
	out := make(map[signal.SeriesID]SeriesAgg, n)
	ids := p.diskRowIDs(ctx)
	for ids.Next() {
		rows, used := binary.Varint(body)
		if used <= 0 || len(body)-used < 24 {
			return nil, errStatsCorrupt
		}
		body = body[used:]
		out[ids.At()] = SeriesAgg{
			Count: float64(rows), Rows: rows,
			Sum: math.Float64frombits(binary.BigEndian.Uint64(body)),
			Min: math.Float64frombits(binary.BigEndian.Uint64(body[8:])),
			Max: math.Float64frombits(binary.BigEndian.Uint64(body[16:])),
		}
		body = body[24:]
	}
	if err := ids.Err(); err != nil {
		return nil, err
	}
	if len(body) != 0 || len(out) != n {
		return nil, errStatsCorrupt
	}
	return out, nil
}

func (p *part) openPartWatermarks(ctx context.Context) (*watermark.Reader, error) {
	return watermark.OpenBoundReader(ctx, p.be, watermark.Key(p.prefix), p.index.seriesCount(), p.ordinalMetadataBinding())
}

func (p *part) decodeOrdinalWatermarks(ctx context.Context, raw []byte) ([]watermark.Entry, bool) {
	n := p.index.seriesCount()
	body, err := sidecar.OrdinalBody(raw, p.ordinalMetadataBinding(), n)
	if err != nil || n != len(body)/8 || len(body)%8 != 0 {
		return nil, false
	}
	out := make([]watermark.Entry, 0, n)
	ids := p.diskRowIDs(p.reader.ReaderContext(ctx))
	for ids.Next() {
		if len(body) < 8 {
			return nil, false
		}
		out = append(out, watermark.Entry{ID: ids.At(), Max: int64(binary.BigEndian.Uint64(body))})
		body = body[8:]
	}
	return out, ids.Err() == nil && len(body) == 0
}

// Bind both the manifest and the exact checked index that determines the row ordinals.
func bindOrdinalMetadata(manifest [32]byte, crc [4]byte) [32]byte {
	var raw [36]byte
	copy(raw[:32], manifest[:])
	copy(raw[32:], crc[:])
	return sha256.Sum256(raw[:])
}

func (p *part) ordinalMetadataBinding() [32]byte {
	var crc [4]byte
	if p.index.paged != nil {
		crc = p.index.paged.crc
	} else {
		// A missing or corrupt SIDX is reconstructed from the series column by openPart.
		// Hash the reconstructed entries without allocating the encoded sidecar.
		var scratch [32]byte
		raw := binary.BigEndian.AppendUint32(scratch[:0], sidxMagic)
		raw = append(raw, sidxVersion)
		raw = binary.AppendUvarint(raw, uint64(len(p.index.ids)))
		raw = binary.AppendUvarint(raw, uint64(p.reader.RowCount()))
		sum := crc32.Checksum(raw, sidxCRC)
		for i, id := range p.index.ids {
			raw = id.AppendBinary(scratch[:0])
			raw = binary.BigEndian.AppendUint32(raw, uint32(p.index.starts[i]))
			sum = crc32.Update(sum, sidxCRC, raw)
		}
		binary.BigEndian.PutUint32(crc[:], sum)
	}
	return bindOrdinalMetadata(p.reader.ManifestDigest(), crc)
}
