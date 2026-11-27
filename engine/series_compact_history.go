package engine

const maxDescriptorMergeInputs = 8

type descriptorMergeAttempt struct {
	inputs    [maxDescriptorMergeInputs][32]byte
	count     int
	allowance int64
}

type descriptorMergeHistory struct {
	attempts [maxDescriptorMergeInputs]descriptorMergeAttempt
	next     int
}

func (h *descriptorMergeHistory) remember(attempt descriptorMergeAttempt) {
	h.attempts[h.next] = attempt
	h.next = (h.next + 1) % len(h.attempts)
}

func (e *Engine) pickDescriptorMerge(candidates []*seriesRootHandle, remaining int, allowance int64) (
	[]*seriesRootHandle, int64, descriptorMergeAttempt,
) {
	// Probe neighboring size-ordered groups after a measured no-gain.
	for lo := 0; lo+1 < len(candidates) && lo < len(e.seriesNoGain.attempts); lo++ {
		sources, scratch := boundedSeriesSources(candidates[lo:min(len(candidates), lo+remaining, lo+maxDescriptorMergeInputs)], allowance)
		if len(sources) < 2 {
			continue
		}
		attempt := descriptorMergeAttempt{count: len(sources), allowance: allowance - scratch}
		for i, source := range sources {
			attempt.inputs[i] = source.binding.ManifestDigest
		}
		if !e.seriesNoGain.contains(attempt) {
			return sources, scratch, attempt
		}
	}
	return nil, 0, descriptorMergeAttempt{}
}

func (h *descriptorMergeHistory) contains(attempt descriptorMergeAttempt) bool {
	for i := range h.attempts {
		if h.attempts[i] == attempt {
			return true
		}
	}
	return false
}
