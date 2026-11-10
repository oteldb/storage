package engine

import (
	"slices"

	"github.com/oteldb/storage/signal"
)

// finishDiskReplicaTrimLocked reclaims drained samples even when a later proof failed.
// Writer/state ownership excludes appenders; head read plans already own their copies.
func (e *Engine) finishDiskReplicaTrimLocked(before int64) {
	h := e.head
	h.recountBytes()
	h.replicaTrimmedBytes += max(before-h.bytes, 0)
	for id, buf := range h.samples {
		if len(buf.ts) == 0 {
			delete(h.samples, id)
		}
	}
	if h.bytes == 0 {
		old := h.memory
		h.memory = sampleArena{}
		h.samples = make(map[signal.SeriesID]*sampleBuf)
		h.buffers = headBuffers{}
		h.replicaTrimmedBytes = 0
		old.release()
	} else if h.replicaTrimmedBytes >= max(h.bytes/2, SampleBytes) {
		h.repackReplicaSamples()
	}
	e.compactDiskHeadLocked()
}

func (h *head) repackReplicaSamples() {
	var next sampleArena
	samples := make(map[signal.SeriesID]*sampleBuf, len(h.samples))
	for id, buf := range h.samples {
		copyBuf := next.buffer()
		next.initColumns(copyBuf, len(buf.ts))
		copyBuf.ts = append(copyBuf.ts, buf.ts...)
		copyBuf.values = append(copyBuf.values, buf.values...)
		copyBuf.sf = slices.Clone(buf.sf)
		samples[id] = copyBuf
	}
	old := h.memory
	h.memory = next
	h.samples = samples
	h.resetBuffers()
	h.replicaTrimmedBytes = 0
	old.release()
}
