package block

import (
	"encoding/binary"

	"github.com/go-faster/errors"

	"github.com/oteldb/storage/encoding/compress"
)

func compactDirBound(s ColumnSizing, timeBounds bool) int64 {
	perGranule := int64(2 * binary.MaxVarintLen64)
	if timeBounds {
		perGranule += 2 * binary.MaxVarintLen64
	}
	return satAdd(3*binary.MaxVarintLen64+dirCRCBytes,
		satAdd(satMul(s.NumFrames, 2*binary.MaxVarintLen64+dirCRCBytes), satMul(s.NumGranules, perGranule)))
}

func compactDirOpenBytes(desc ColumnDesc) int64 {
	return compactDirBound(desc.Sizing, desc.TimeBounds)
}

func compactDirCompressor(comp *compress.Compressor) *compress.Compressor {
	if comp != nil && comp.Algorithm() == compress.AlgorithmLZ4 {
		return comp
	}
	return compress.NewCompressor(compress.AlgorithmLZ4, 0)
}

func parseCompactDir(dir []byte, frameBytes int64, chk dirCheck) (blockDir, error) {
	if chk.sizing == nil || chk.maxBlockRows == 0 || !chk.checked || chk.dirComp == nil || len(dir) < dirCRCBytes {
		return blockDir{}, errors.Wrap(ErrCorrupt, "invalid compact directory")
	}
	if err := chk.dirLen(len(dir)); err != nil {
		return blockDir{}, err
	}
	end := len(dir) - dirCRCBytes
	if err := verifyDirCRC(dir, end); err != nil {
		return blockDir{}, err
	}
	n, k := binary.Uvarint(dir[:end])
	if k <= 0 || n < dirCRCBytes || n > uint64(compactDirBound(*chk.sizing, chk.timeBounds)) || n > uint64(^uint(0)>>1) {
		return blockDir{}, errors.Wrap(ErrCorrupt, "compact directory raw size")
	}
	raw, err := compactDirCompressor(chk.dirComp).DecompressLimit(nil, dir[k:end], int(n))
	if err != nil {
		return blockDir{}, errors.Errorf("%w: decompress directory: %w", ErrCorrupt, err)
	}
	if len(raw) != int(n) {
		return blockDir{}, errors.Wrap(ErrCorrupt, "compact directory raw length differs")
	}
	sizing := *chk.sizing
	sizing.DirLen = int64(n)
	chk.sizing, chk.compactDir = &sizing, false
	return parseDetachedDir(raw, frameBytes, chk)
}
