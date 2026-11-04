package recordengine

import (
	"github.com/oteldb/storage/block"
	"github.com/oteldb/storage/query/fetch"
)

func (p *part) rangesMayMatch(conds []fetch.Condition) bool {
	for i := range conds {
		c := &conds[i]
		if c.Int64Range == nil {
			continue
		}
		ref, ok := p.schema.ref(c.Column)
		if !ok || ref.kind != KindInt64 {
			continue
		}
		desc, ok := p.reader.ColumnDescByName(c.Column)
		if !ok || desc.Kind != block.KindInt64 {
			continue
		}
		if !c.Int64Range.Overlaps(desc.MinInt64, desc.MaxInt64) {
			return false
		}
	}
	return true
}
