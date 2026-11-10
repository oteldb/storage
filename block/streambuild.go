package block

import (
	"encoding/binary"
	"slices"

	"github.com/oteldb/storage/internal/memsize"
)

const maxStreamBuildScratchBytes = 4 << 20

// StreamBuildScratch reuses metadata encoding buffers across sequential part writes.
// Retained capacity above 4 MiB is released after a write. Raw Marks views are never pooled.
type StreamBuildScratch struct {
	descriptors []ColumnDesc
	objects     [][]byte
	marks       []byte
	manifest    []byte
}

// SizeBytes returns the buffer capacity retained by s.
func (s *StreamBuildScratch) SizeBytes() int64 {
	if s == nil {
		return 0
	}
	return memsize.Slice(s.descriptors) + memsize.Slice(s.objects) + memsize.Slice(s.marks) + memsize.Slice(s.manifest)
}

func (s *StreamBuildScratch) growDescriptors(n int) []ColumnDesc {
	s.descriptors = slices.Grow(s.descriptors[:0], n)[:n]
	return s.descriptors
}

func (s *StreamBuildScratch) growObjects(n int) [][]byte {
	s.objects = slices.Grow(s.objects[:0], n)[:n]
	return s.objects
}

func (s *StreamBuildScratch) reset() {
	clear(s.descriptors[:cap(s.descriptors)])
	clear(s.objects[:cap(s.objects)])
	s.descriptors = s.descriptors[:0]
	s.objects = s.objects[:0]
	s.marks = s.marks[:0]
	s.manifest = s.manifest[:0]
	if s.SizeBytes() > maxStreamBuildScratchBytes {
		*s = StreamBuildScratch{}
	}
}

func (w *StreamWriter) buildBytes() int64 {
	headers := satMul(int64(len(w.cols)), memsize.Of[ColumnDesc]()+memsize.Of[[]byte]())
	manifest := int64(128)
	for _, column := range w.cols {
		constantBytes := len(column.first)
		if column.kind == KindBytes && column.allSame && column.bytes != nil && len(column.bytes.g.vals) == 1 {
			constantBytes = max(constantBytes, len(column.bytes.g.vals[0]))
		}
		manifest = satAdd(manifest, satAdd(512, int64(len(column.name)+constantBytes)))
	}
	if w.rollup != nil {
		manifest = satAdd(manifest, satMul(int64(len(w.rollup.Tiers)), 64))
	}
	marks := int64(64)
	if i := w.sortKeyIndex(); i >= 0 {
		marks = satAdd(marks, satMul(int64(len(w.cols[i].granules)+1), 3*binary.MaxVarintLen64))
	}
	return satMul(3, satAdd(headers, satAdd(marks, manifest)))
}
