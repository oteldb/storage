package recordengine

import "time"

func (e *Engine) retentionOptions(opts MergeOptions) MergeOptions {
	if opts.Background && !opts.Force {
		opts.retentionNow = e.now()
	} else {
		opts.retentionNow = time.Time{}
	}
	return opts
}

func (e *Engine) stampRetention(src, out []*part, cutoff int64) {
	var last time.Time
	expired := false
	for _, p := range src {
		if p.retentionAt.After(last) {
			last = p.retentionAt
		}
		expired = expired || (cutoff > 0 && p.minTime < cutoff)
	}
	if expired {
		last = e.now()
	}
	for _, p := range out {
		p.retentionAt = last
	}
}
