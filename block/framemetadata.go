package block

import (
	"encoding/binary"

	"github.com/go-faster/errors"
)

func appendFrameMetadata(dst []byte, granules, packed int, checksum uint32) []byte {
	dst = binary.AppendUvarint(dst, uint64(granules))
	dst = binary.AppendUvarint(dst, uint64(packed))
	return binary.LittleEndian.AppendUint32(dst, checksum)
}

// nextFrameGranules walks our own encoded records for the buffered dictionary fallback.
func nextFrameGranules(src []byte) ([]byte, int, error) {
	count, n := binary.Uvarint(src)
	if n <= 0 || count == 0 || count > uint64(^uint(0)>>1) {
		return nil, 0, errors.Wrap(ErrCorrupt, "buffered frame granules")
	}
	src = src[n:]
	_, n = binary.Uvarint(src)
	if n <= 0 || len(src)-n < 4 {
		return nil, 0, errors.Wrap(ErrCorrupt, "buffered frame metadata")
	}
	return src[n+4:], int(count), nil
}
