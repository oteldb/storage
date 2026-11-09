package metric

import "github.com/oteldb/storage/signal"

// projectRepeatedPoints leaves b untouched when the initial pointer pattern is not periodic.
// The caller supplies at least four points.
func (p *projector) projectRepeatedPoints(b *Batch, points []NumberPoint) bool {
	if sameAttributeStorage(points[0].Attributes, points[1].Attributes) {
		return false
	}
	slots := 0
	if sameAttributeStorage(points[0].Attributes, points[2].Attributes) &&
		sameAttributeStorage(points[1].Attributes, points[3].Attributes) {
		slots = 2
	} else if len(points) >= 8 && sameAttributeStorage(points[0].Attributes, points[4].Attributes) &&
		sameAttributeStorage(points[1].Attributes, points[5].Attributes) &&
		sameAttributeStorage(points[2].Attributes, points[6].Attributes) &&
		sameAttributeStorage(points[3].Attributes, points[7].Attributes) {
		slots = 4
	}
	if slots == 0 {
		return false
	}

	var cache [4]struct {
		attrs signal.Attributes
		id    signal.SeriesID
	}
	used, next, misses := 0, 0, 0
	for i := range points {
		point := &points[i]
		var id signal.SeriesID
		hit := false
		if misses < 16 {
			for slot := range used {
				if sameAttributeStorage(cache[slot].attrs, point.Attributes) {
					id, hit = cache[slot].id, true
					break
				}
			}
		}
		if !hit {
			id = p.id(point.Attributes)
			if misses < 16 {
				cache[next].attrs, cache[next].id = point.Attributes, id
				next = (next + 1) & (slots - 1)
				used = min(used+1, slots)
				misses++
			}
		} else {
			misses = 0
		}
		b.IDs = append(b.IDs, id)
		b.Ts = append(b.Ts, point.Ts)
		b.Values = append(b.Values, point.Value)
	}
	return true
}
