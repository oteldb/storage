package engine

import (
	"cmp"
	"context"
	"math"
	"slices"

	"github.com/go-faster/errors"

	"github.com/oteldb/storage/backend"
	"github.com/oteldb/storage/block"
	"github.com/oteldb/storage/signal"
)

const maxReplicaTrimSamples = 64 << 10
const maxReplicaTrimWorkspace = 16 << 20

type replicaSampleProof struct {
	timestamp       int64
	value, sf       uint64
	seen, ambiguous bool
}

// trimDiskReplicaLocked removes only unambiguous samples proved present in immutable parts.
// The caller owns catalog, writer and state; decoding admits readers, publication does not.
func (e *Engine) trimDiskReplicaLocked(ctx context.Context) error {
	if e.flushing != nil || !e.diskSeriesCompleteLocked() {
		return nil
	}
	parts := e.readablePartsLocked()
	for _, part := range parts {
		part.acquire()
	}
	defer func() {
		for _, part := range parts {
			part.release()
		}
	}()
	var proofs []replicaSampleProof
	var scratch replicaTrimScratch
	for id, buf := range e.head.samples {
		if len(buf.ts) > maxReplicaTrimSamples {
			continue
		}
		proofs = replicaHeadProofs(proofs, buf)
		if len(proofs) == 0 {
			continue
		}
		supported := true
		err := e.mu.WithReaders(func() error {
			for _, part := range parts {
				ok, err := part.proveReplicaSamples(ctx, id, proofs, &scratch)
				if err != nil {
					return err
				}
				if !ok {
					supported = false
					return nil
				}
			}
			return nil
		})
		if err != nil {
			return errors.Wrap(err, "prove replica sample coverage")
		}
		if supported {
			trimProvenReplicaSamples(buf, proofs)
		}
	}
	e.head.recountBytes()
	return nil
}

func replicaHeadProofs(dst []replicaSampleProof, buf *sampleBuf) []replicaSampleProof {
	dst = resize(dst, len(buf.ts))
	for i, ts := range buf.ts {
		dst[i] = replicaSampleProof{timestamp: ts, value: math.Float64bits(buf.values[i]), sf: math.Float64bits(bufSF(buf, i))}
	}
	slices.SortFunc(dst, func(a, b replicaSampleProof) int { return cmp.Compare(a.timestamp, b.timestamp) })
	n := 0
	for _, proof := range dst {
		if n > 0 && dst[n-1].timestamp == proof.timestamp {
			if dst[n-1].value != proof.value || dst[n-1].sf != proof.sf {
				dst[n-1].ambiguous = true
			}
			continue
		}
		dst[n] = proof
		n++
	}
	return dst[:n]
}

func trimProvenReplicaSamples(buf *sampleBuf, proofs []replicaSampleProof) {
	n := 0
	for i, ts := range buf.ts {
		at, found := slices.BinarySearchFunc(proofs, ts, func(p replicaSampleProof, t int64) int { return cmp.Compare(p.timestamp, t) })
		if found && proofs[at].seen && !proofs[at].ambiguous {
			continue
		}
		buf.ts[n], buf.values[n] = ts, buf.values[i]
		if buf.sf != nil {
			buf.sf[n] = buf.sf[i]
		}
		n++
	}
	buf.ts, buf.values = buf.ts[:n], buf.values[:n]
	if buf.sf != nil {
		buf.sf = buf.sf[:n]
	}
}

type replicaTrimScratch struct {
	ts         []int64
	values, sf []float64
}

type replicaTrimReader struct {
	desc     [3]block.ColumnDesc
	decoders [3]*block.Decoder
	geometry *block.Decoder
}

// proveReplicaSamples retains unsupported layouts instead of guessing their coverage.
func (p *part) proveReplicaSamples(
	ctx context.Context, id signal.SeriesID, proofs []replicaSampleProof, scratch *replicaTrimScratch,
) (bool, error) {
	if p.maxTime < proofs[0].timestamp || p.minTime > proofs[len(proofs)-1].timestamp {
		return true, nil
	}
	if index := p.index.paged; index != nil && index.view.Load() == nil {
		if !backend.RangesNatively(ctx, index.be, index.key) || (len(index.fences) == 0 && index.n > indexPageEntries) {
			return false, nil
		}
	}
	rng, found, err := p.index.lookup(ctx, id)
	if err != nil {
		return false, err
	}
	if !found {
		return true, nil
	}
	if len(p.rollup) > 0 {
		return false, nil
	}
	reader, supported, err := newReplicaTrimReader(ctx, p)
	if err != nil || !supported {
		return supported, err
	}
	defer reader.close()
	if reader.geometry == nil {
		noteReplicaSample(proofs, reader.desc[0].ConstInt64, reader.desc[1].ConstFloat64, reader.constantSF())
		return true, nil
	}
	first, err := reader.geometry.BlockForRow(rng.start)
	if err != nil {
		return false, err
	}
	last, err := reader.geometry.BlockForRow(rng.end - 1)
	if err != nil {
		return false, err
	}
	for b := first; b <= last; b++ {
		if err := ctx.Err(); err != nil {
			return false, err
		}
		if err := reader.proveBlock(b, rng, proofs, scratch); err != nil {
			return false, err
		}
	}
	return true, nil
}

func (r *replicaTrimReader) proveBlock(b int, rng rowRange, proofs []replicaSampleProof, scratch *replicaTrimScratch) error {
	lo, hi := r.geometry.BlockSpan(b)
	if err := r.decode(b, scratch); err != nil {
		return err
	}
	for row := max(rng.start, lo); row < min(rng.end, hi); row++ {
		offset := row - lo
		ts := r.desc[0].ConstInt64
		if r.decoders[0] != nil {
			ts = scratch.ts[offset]
		}
		value := r.desc[1].ConstFloat64
		if r.decoders[1] != nil {
			value = scratch.values[offset]
		}
		sf := r.constantSF()
		if r.decoders[2] != nil {
			sf = scratch.sf[offset]
		}
		noteReplicaSample(proofs, ts, value, sf)
	}
	return nil
}

func noteReplicaSample(proofs []replicaSampleProof, ts int64, value, sf float64) {
	at, found := slices.BinarySearchFunc(proofs, ts, func(p replicaSampleProof, t int64) int { return cmp.Compare(p.timestamp, t) })
	if !found {
		return
	}
	proof := &proofs[at]
	if proof.value != math.Float64bits(value) || proof.sf != math.Float64bits(sf) {
		proof.ambiguous = true
		return
	}
	proof.seen = true
}

func newReplicaTrimReader(ctx context.Context, p *part) (*replicaTrimReader, bool, error) {
	r := &replicaTrimReader{}
	names := []string{colTs, colValue, colSF}
	var workspace int64
	for i, name := range names {
		if name == colSF && !p.hasSF {
			r.desc[i] = block.ColumnDesc{Const: true, ConstFloat64: 1}
			continue
		}
		desc, ok := p.reader.ColumnDescByName(name)
		if !ok {
			return nil, false, errors.Wrap(block.ErrCorrupt, "missing replica coverage column")
		}
		r.desc[i] = desc
		if desc.Const {
			continue
		}
		retained, opening, ok := p.reader.ScanBound(ctx, name, 0)
		remaining := int64(maxReplicaTrimWorkspace) - workspace
		if !ok || retained < 0 || opening < 0 || retained > remaining || opening > remaining-retained {
			return nil, false, nil
		}
		workspace += retained + opening
	}
	for i, name := range names {
		if r.desc[i].Const {
			continue
		}
		decoder, err := p.reader.OpenColumnScan(ctx, name, 0)
		if err != nil {
			r.close()
			return nil, false, err
		}
		r.decoders[i] = decoder
		if r.geometry == nil {
			r.geometry = decoder
		}
	}
	return r, true, nil
}

func (r *replicaTrimReader) constantSF() float64 { return r.desc[2].ConstFloat64 }

func (r *replicaTrimReader) decode(b int, scratch *replicaTrimScratch) error {
	lo, hi := r.geometry.BlockSpan(b)
	for _, decoder := range r.decoders {
		if decoder == nil {
			continue
		}
		if b >= decoder.NumBlocks() {
			return errors.Wrap(block.ErrCorrupt, "replica coverage block count")
		}
		a, z := decoder.BlockSpan(b)
		if a != lo || z != hi {
			return errors.Wrap(block.ErrCorrupt, "replica coverage block geometry")
		}
	}
	var err error
	if d := r.decoders[0]; d != nil {
		scratch.ts, err = d.DecodeInt64Into(b, scratch.ts[:0])
		if err != nil {
			return err
		}
	}
	if d := r.decoders[1]; d != nil {
		scratch.values, err = d.DecodeFloat64Into(b, scratch.values[:0])
		if err != nil {
			return err
		}
	}
	if d := r.decoders[2]; d != nil {
		scratch.sf, err = d.DecodeFloat64Into(b, scratch.sf[:0])
		if err != nil {
			return err
		}
	}
	return nil
}

func (r *replicaTrimReader) close() {
	for _, decoder := range r.decoders {
		_ = decoder.Close()
	}
}
