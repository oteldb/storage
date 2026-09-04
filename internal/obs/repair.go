package obs

import (
	"context"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
)

// Repair instruments cross-node part repair: a shard owner that holds an index entry it cannot
// read pulls the part back from a peer, and when no owner has it, acknowledges the loss.
//
// lost_parts is the alertable one and is a monotone counter, never a gauge: data loss is a fact,
// not a level. A gauge that a restart or a successful repair returns to zero erases the only
// record that a range of a shard was ever acknowledged as gone.
type Repair struct {
	attempts metric.Int64Counter
	lost     metric.Int64Counter
	revoked  metric.Int64Counter
}

// Record accounts one repair pass over a shard: n attempts that ended with result ("local",
// "fetched", "absent", "incomplete" or "failed"). A zero n is ignored.
func (r *Repair) Record(ctx context.Context, n int64, result string) {
	if n <= 0 {
		return
	}

	r.attempts.Add(ctx, n, metric.WithAttributes(attribute.String("result", result)))
}

// Lost accounts n parts acknowledged as lost — a hole committed at their identity because no owner
// of the shard held them or any successor containing them.
func (r *Repair) Lost(ctx context.Context, n int64) {
	if n > 0 {
		r.lost.Add(ctx, n)
	}
}

// Revoked accounts n holes replaced by the real part turning up after all.
func (r *Repair) Revoked(ctx context.Context, n int64) {
	if n > 0 {
		r.revoked.Add(ctx, n)
	}
}

func newRepair(m metric.Meter) (*Repair, error) {
	b := &imb{m: m}
	r := &Repair{
		attempts: b.counter("storage.repair.attempts",
			"part repair attempts, by result", "{attempt}"),
		lost: b.counter("storage.repair.lost_parts",
			"parts acknowledged as lost (a hole committed at their identity)", "{part}"),
		revoked: b.counter("storage.repair.revoked_holes",
			"holes replaced by the part turning up after all", "{hole}"),
	}

	return r, b.err
}
