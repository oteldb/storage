package recordengine

import (
	"context"
	"encoding/binary"
	"math"

	"github.com/oteldb/storage/backend"
	"github.com/oteldb/storage/block"
	"github.com/oteldb/storage/encoding/compress"
)

// sourceBound bounds from p's manifest what the merge source it opens as will report
// ([mergeSource.residentBytes]) and how many stable dictionary entries it will hand a writer
// ([mergeSource.dictEntries]), so a merge can reserve its memory before opening anything. ok is false
// when the manifest cannot size a column the source reads.
func (e *Engine) sourceBound(ctx context.Context, p *part) (bytes int64, entries int, ok bool) {
	if !forwardReadable(p.ranges) || mergeReadWhole || p.tsDisorder.Load() {
		return wholeBound(p)
	}

	longest := 0
	for _, r := range p.ranges {
		longest = max(longest, r.end-r.start)
	}

	rows := int64(p.reader.RowCount())

	intCol := func(name string, fillRows int) (int64, bool) {
		desc, ok := p.reader.ColumnDescByName(name)
		switch {
		case !ok:
			return 0, false
		case desc.Const:
			return int64(fillRows) * 8, true
		case !desc.Blocked:
			return rows * 8, true
		default:
			return p.reader.ScanBound(ctx, name, e.mergeReadWindow)
		}
	}

	n, ok := intCol(colTs, longest)
	if !ok {
		return 0, 0, false
	}

	bytes += n

	for k := range p.schema.numInts() {
		if n, ok = intCol(p.schema.intColumn(k).Name, mergeGranuleRows); !ok {
			return 0, 0, false
		}

		bytes += n
	}

	for k := range p.schema.numBytes() {
		name := p.schema.byteColumn(k).Name

		desc, ok := p.reader.ColumnDescByName(name)
		if !ok {
			return 0, 0, false
		}

		switch {
		case desc.Const:
			bytes += mergeGranuleRows
		case !desc.Blocked:
			if !desc.HasSizing {
				return 0, 0, false
			}

			bytes += desc.Sizing.StreamRaw + rows*(viewBytes+binary.MaxVarintLen32+4)
			entries += int(rows)
		default:
			if n, ok = p.reader.ScanBound(ctx, name, e.mergeReadWindow); !ok {
				return 0, 0, false
			}

			bytes += n
			entries += int(desc.DictEntries)
		}
	}

	return bytes, entries, true
}

// wholeBound is [Engine.sourceBound] for a part decoded whole: its columns as [part.readForMerge]
// decodes them, a byte column's dictionary and values allowed their full growth, since they are built
// by appending.
func wholeBound(p *part) (bytes int64, entries int, ok bool) {
	rows := int64(p.reader.RowCount())
	bytes = 8 * rows * int64(1+p.schema.numInts())

	for k := range p.schema.numBytes() {
		desc, ok := p.reader.ColumnDescByName(p.schema.byteColumn(k).Name)
		switch {
		case !ok:
			return 0, 0, false
		case desc.Const:
			bytes += int64(len(desc.ConstBytes)) + viewBytes + binary.MaxVarintLen32 + rows
			entries++

			continue
		case !desc.HasSizing:
			return 0, 0, false
		}

		values := rows + desc.DictEntries
		bytes += desc.DictRaw + desc.Sizing.StreamRaw + 2*values*(viewBytes+binary.MaxVarintLen32) + 2*4*rows
		entries += int(values)
	}

	return bytes, entries, true
}

// mergeFloorRun is the run the writer budget assumes for a merge not bound by a part size.
const mergeFloorRun = 1 << 20

// appendReserve is the room the router keeps for one append: a writer binding every source's
// dictionaries, 12 B an entry, plus one run.
func appendReserve(entries int, runBytes int64) int64 {
	const bindEntryBytes = 12

	if runBytes <= 0 {
		runBytes = mergeFloorRun
	}

	return runBytes + int64(entries)*bindEntryBytes
}

// mergeNeed is what a merge of src must be able to hold before anything opens: the sources as their
// manifests bound them, the encoders, and the writers' floor of two appends. ok is false when a
// source's manifest cannot say.
func (e *Engine) mergeNeed(ctx context.Context, src []*part, capBytes int64) (int64, bool) {
	need := e.newMergeCoders(src).workspace()
	entries := 0

	for _, p := range src {
		n, k, ok := e.sourceBound(ctx, p)
		if !ok {
			return 0, false
		}

		need += n
		entries += k
	}

	return need + 2*appendReserve(entries, e.mergePartBytes(capBytes)/mergeRunFraction), true
}

// mergeCoders are the compressors a merge's day writers share: frames go through a 1 MiB-window
// encoder, whole objects — dictionary regions, a column written unframed, the stream id column —
// through one whose window fits the largest such object the merge can write. Each is used by one
// writer at a time, so a merge holds one encoder of each.
type mergeCoders struct {
	frames, objects *compress.Compressor
}

func (e *Engine) newMergeCoders(src []*part) *mergeCoders {
	alg, level := e.cfg.MergeCompression, e.cfg.MergeCompressionLevel

	return &mergeCoders{
		frames:  block.NewFrameCompressor(alg, level),
		objects: compress.NewFrameCompressor(alg, level, mergeObjectBytes(src, !backend.StreamsWrites(e.cfg.Backend))),
	}
}

func (c *mergeCoders) workspace() int64 {
	return c.frames.EncodeWorkspace() + c.objects.EncodeWorkspace()
}

// mergeObjectBytes bounds the largest whole object a merge of src compresses. An output part's
// dictionary holds values its sources held in their dictionaries or in their self-encoded granules,
// so a byte column's dictionary region is at most its sources' dictionaries and streams together.
// Only a writer that buffers (unframed) rewrites a column no granule of which joined as one stream,
// the sources' streams merged; a streaming one keeps its frames. The stream id column is a run per
// stream. A source without sizing stats bounds nothing, and the encoder gets the default window.
func mergeObjectBytes(src []*part, buffered bool) int {
	const runBytes = 24

	if len(src) == 0 {
		return 0
	}

	var streams, largest int64

	for _, p := range src {
		streams += int64(len(p.ranges))
	}

	largest = streams * runBytes

	for k := range src[0].schema.numBytes() {
		var n int64

		for _, p := range src {
			desc, ok := p.reader.ColumnDescByName(p.schema.byteColumn(k).Name)
			switch {
			case !ok || (!desc.Const && !desc.HasSizing):
				return math.MaxInt
			case desc.Const:
				n += int64(len(desc.ConstBytes)) + binary.MaxVarintLen32
			case buffered || desc.SharedDict:
				n += desc.DictRaw + desc.Sizing.StreamRaw
			}
		}

		largest = max(largest, n)
	}

	return int(min(largest, math.MaxInt))
}
