package engine

import (
	"context"
	"math"

	"github.com/oteldb/storage/index/series"
	"github.com/oteldb/storage/internal/buildbudget"
)

func headEntryWork(encoded, materialized, sorting, identity, rows int64) uint16 {
	work := buildbudget.Add(512, buildbudget.Mul(encoded, 4))
	work = buildbudget.Add(work, buildbudget.Add(materialized, identity))
	work = buildbudget.Add(work, buildbudget.Add(sorting, buildbudget.Mul(rows, 16)))
	return uint16(min(buildbudget.Add(work, 63)/64, math.MaxUint16))
}

func headJobParts(rows int64, limit int) int {
	if limit > 0 {
		return int(1 + (rows-1)/int64(limit))
	}
	return 1
}

func (p *headPartPlan) balance(planner *headPartBuilder) error {
	if len(p.jobs) != 2 || len(p.entries) < 2 {
		return nil
	}
	limit := planner.partRows()
	cut := p.balanceCut(limit)
	if cut == len(p.jobs[0].entries) {
		return nil
	}
	base := int64(0)
	for _, job := range p.jobs {
		fixed := job.privateBytes - job.identityBytes - 4*job.encoded - int64(len(job.entries))*512 - job.materialized - job.sorting
		base = max(base, fixed)
	}
	maxPrivate := min(planner.reserved, planner.target, max(p.jobs[0].privateBytes, p.jobs[1].privateBytes))
	first, err := priceHeadJob(planner.ctx, planner.view, p.entries[:cut:cut], base, limit, &planner.identities)
	if err != nil {
		return err
	}
	if first.privateBytes > maxPrivate {
		return nil
	}
	second, err := priceHeadJob(planner.ctx, planner.view, p.entries[cut:len(p.entries):len(p.entries)], base, limit, &planner.identities)
	if err != nil {
		return err
	}
	if second.privateBytes > maxPrivate {
		return nil
	}
	p.jobs[0], p.jobs[1] = first, second
	p.retained = buildbudget.Add(first.retainedBytes, second.retainedBytes)
	p.balanced = true
	return nil
}

func (p *headPartPlan) balanceCut(limit int) int {
	oldCut := len(p.jobs[0].entries)
	var total, oldLeft, totalRows int64
	for i, entry := range p.entries {
		if entry.work == 0 || entry.work == math.MaxUint16 || limit > 0 && len(entry.buf.ts) > limit {
			return oldCut
		}
		total += int64(entry.work)
		totalRows += int64(len(entry.buf.ts))
		if i < oldCut {
			oldLeft += int64(entry.work)
		}
	}
	oldPeak := max(oldLeft, total-oldLeft)
	cut, peak := oldCut, oldPeak
	var left, rows int64
	for i, entry := range p.entries[:len(p.entries)-1] {
		left += int64(entry.work)
		rows += int64(len(entry.buf.ts))
		if candidate := max(left, total-left); candidate < peak &&
			headJobParts(rows, limit)+headJobParts(totalRows-rows, limit) <= p.parts {
			cut, peak = i+1, candidate
		}
	}
	if peak >= oldPeak-oldPeak/10 {
		return oldCut
	}
	return cut
}

func priceHeadJob(
	ctx context.Context, view series.View, entries []headRunEntry, base int64, limit int,
	identities *series.EncodingBudget,
) (headPartJob, error) {
	job := headPartJob{entries: entries}
	identities.Reset()
	for i, entry := range entries {
		if i%4096 == 0 {
			if err := ctx.Err(); err != nil {
				return headPartJob{}, err
			}
		}
		identities.Add(view, int(entry.ref))
		job.encoded = buildbudget.Add(job.encoded, view.EncodingBytes(int(entry.ref)))
		job.materialized = max(job.materialized, view.MaterializationBytes(int(entry.ref)))
		if !entry.ordered {
			job.sorting = max(job.sorting, buildbudget.Mul(int64(len(entry.buf.ts)), 48))
		}
		job.rows = buildbudget.Add(job.rows, int64(len(entry.buf.ts)))
	}
	job.identityBytes = identities.Bytes()
	job.privateBytes = buildbudget.Add(base, job.identityBytes)
	job.privateBytes = buildbudget.Add(job.privateBytes, buildbudget.Mul(job.encoded, 4))
	job.privateBytes = buildbudget.Add(job.privateBytes, buildbudget.Mul(int64(len(entries)), 512))
	job.privateBytes = buildbudget.Add(job.privateBytes, buildbudget.Add(job.materialized, job.sorting))
	job.retainedBytes = buildbudget.Mul(int64(headJobParts(job.rows, limit)), buildbudget.Add(partIDBloomBytes(len(entries)), 64<<10))
	return job, nil
}
