package metric

import "github.com/oteldb/storage/signal"

func (p *projector) projectPoints(b *Batch, points []NumberPoint) {
	// Probe once: repeatedly comparing unrelated identities costs more than hashing them.
	if len(points) < 2 || (!sameAttributeStorage(points[0].Attributes, points[1].Attributes) &&
		!points[0].Attributes.Equal(points[1].Attributes)) {
		for i := range points {
			pt := &points[i]
			b.IDs = append(b.IDs, p.id(pt.Attributes))
			b.Ts = append(b.Ts, pt.Ts)
			b.Values = append(b.Values, pt.Value)
		}
		return
	}
	previous := points[0].Attributes
	id := p.id(previous)
	compareEqual := true
	for i := range points {
		pt := &points[i]
		if i > 0 && !sameAttributeStorage(previous, pt.Attributes) &&
			(!compareEqual || !previous.Equal(pt.Attributes)) {
			compareEqual = false
			previous, id = pt.Attributes, p.id(pt.Attributes)
		}
		b.IDs = append(b.IDs, id)
		b.Ts = append(b.Ts, pt.Ts)
		b.Values = append(b.Values, pt.Value)
	}
}

func sameAttributeStorage(a, b signal.Attributes) bool {
	if len(a) != len(b) {
		return false
	}
	return len(a) == 0 || &a[0] == &b[0]
}
