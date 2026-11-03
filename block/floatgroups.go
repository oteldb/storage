package block

import (
	"github.com/go-faster/errors"

	"github.com/oteldb/storage/encoding/chunk"
)

func (c Column) checkFloatGroups(buffered bool) error {
	if c.Codec != chunk.CodecFloatGroups {
		return nil
	}
	if c.Kind != KindFloat64 || !c.Block || c.AutoCodec || c.FloatPrecisionBits != 0 {
		return errors.New("block: float groups require a blocked lossless float column")
	}
	if !buffered {
		return nil
	}
	last := 0
	for _, end := range c.FloatGroupEnds {
		if end <= last || end > len(c.Float64) {
			return errors.New("block: invalid float group ends")
		}
		last = end
	}
	if last != len(c.Float64) {
		return errors.New("block: incomplete float group ends")
	}
	return nil
}
