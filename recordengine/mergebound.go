package recordengine

import (
	"context"
	"encoding/binary"
	"math"
	"unsafe"

	"github.com/oteldb/storage/index/series"

	"github.com/oteldb/storage/backend"
	"github.com/oteldb/storage/block"
	"github.com/oteldb/storage/encoding/chunk"
	"github.com/oteldb/storage/encoding/compress"
)

// sourceBudget is what a merge source costs, bounded from its part's manifest: steady is what it
// reports once open ([mergeSource.residentBytes]), open what opening one of its columns holds on top
// at most, and entries the stable dictionary entries it hands a writer ([mergeSource.dictEntries]).
type sourceBudget struct {
	steady, open int64
	entries      int
}

// wholeReadOpen is what reading and decoding one column whole holds beside what it keeps: the object
// as read and, for a compressed one, the stream decompressed into scratch.
func wholeReadOpen(desc block.ColumnDesc) int64 {
	return desc.Bytes + desc.Sizing.StreamRaw + desc.DictRaw + int64(compress.OutputSlack(desc.Compress))
}

// sourceBound bounds from p's manifest what the merge source it opens as will cost, so a merge can
// reserve its memory before opening anything. ok is false when the manifest cannot size a column the
// source reads.
func (e *Engine) sourceBound(ctx context.Context, p *part) (b sourceBudget, ok bool) {
	if !forwardReadable(p.ranges) || mergeReadWhole || p.tsDisorder.Load() {
		return wholeBound(p)
	}

	longest := 0
	for _, r := range p.ranges {
		longest = max(longest, r.end-r.start)
	}

	rows := int64(p.reader.RowCount())

	intCol := func(name string, fillRows int) bool {
		desc, ok := p.reader.ColumnDescByName(name)
		switch {
		case !ok:
			return false
		case desc.Const:
			b.steady += int64(fillRows) * 8
		case !desc.Blocked:
			if !desc.HasSizing {
				return false
			}

			b.steady += rows * 8
			b.open = max(b.open, wholeReadOpen(desc))
		default:
			steady, open, ok := p.reader.ScanBound(ctx, name, e.mergeReadWindow)
			if !ok {
				return false
			}

			b.steady += steady
			b.open = max(b.open, open)
		}

		return true
	}

	if !intCol(colTs, longest) {
		return sourceBudget{}, false
	}

	for k := range p.schema.numInts() {
		if !intCol(p.schema.intColumn(k).Name, mergeGranuleRows) {
			return sourceBudget{}, false
		}
	}

	for k := range p.schema.numBytes() {
		name := p.schema.byteColumn(k).Name

		desc, ok := p.reader.ColumnDescByName(name)
		if !ok {
			return sourceBudget{}, false
		}

		switch {
		case desc.Const:
			b.steady += mergeGranuleRows
		case !desc.Blocked:
			if !desc.HasSizing {
				return sourceBudget{}, false
			}

			b.steady += desc.Sizing.StreamRaw + rows*(viewBytes+binary.MaxVarintLen32+4)
			b.open = max(b.open, wholeReadOpen(desc))
			b.entries += int(rows)
		default:
			steady, open, ok := p.reader.ScanBound(ctx, name, e.mergeReadWindow)
			if !ok {
				return sourceBudget{}, false
			}

			b.steady += steady
			b.open = max(b.open, open)
			b.entries += int(desc.DictEntries)
		}
	}

	return b, true
}

// wholeBound is [Engine.sourceBound] for a part decoded whole: its columns as [part.readForMerge]
// decodes them, a byte column's dictionary and values allowed their full growth, since they are built
// by appending, and the most a stream gathered and sorted out of it can hold ([gatherBound]).
func wholeBound(p *part) (b sourceBudget, ok bool) {
	rows := int64(p.reader.RowCount())
	b.steady = 8*rows*int64(1+p.schema.numInts()) + gatherBound(p)

	for _, name := range append([]string{colTs}, intColumnNames(p.schema)...) {
		desc, ok := p.reader.ColumnDescByName(name)
		switch {
		case !ok:
			return sourceBudget{}, false
		case desc.Const:
		case !desc.HasSizing:
			return sourceBudget{}, false
		default:
			b.open = max(b.open, wholeReadOpen(desc))
		}
	}

	for k := range p.schema.numBytes() {
		desc, ok := p.reader.ColumnDescByName(p.schema.byteColumn(k).Name)
		switch {
		case !ok:
			return sourceBudget{}, false
		case desc.Const:
			b.steady += int64(len(desc.ConstBytes)) + viewBytes + binary.MaxVarintLen32 + rows
			b.entries++

			continue
		case !desc.HasSizing:
			return sourceBudget{}, false
		}

		values := rows + desc.DictEntries
		b.steady += desc.DictRaw + desc.Sizing.StreamRaw + 2*values*(viewBytes+binary.MaxVarintLen32) + 2*4*rows
		b.open = max(b.open, wholeReadOpen(desc))
		b.entries += int(values)
	}

	return b, true
}

func intColumnNames(schema *Schema) []string {
	names := make([]string, schema.numInts())
	for k := range names {
		names[k] = schema.intColumn(k).Name
	}

	return names
}

// gatherBound is the most a whole-decoded part's gather can hold ([gatherRun]): one stream's rows —
// at most the part's, d decoded bytes over r rows — copied out with append growth (2d plus 2×4 B of
// offset a row per byte column), sorted through an index (8 B a row) into fresh timestamp and int
// arrays (at most d again) and a second set of byte blobs and offsets (2d plus the offsets again),
// with a view a row per byte column (24 B): 5d + (8 + 40 per byte column) B a row.
func gatherBound(p *part) int64 {
	rows := int64(p.reader.RowCount())

	return 5*p.sizeBytes() + rows*(8+40*int64(p.schema.numBytes()))
}

// mergeFloorRun is the run the writer budget assumes for a merge not bound by a part size.
const mergeFloorRun = 1 << 20

// appendReserve is the room the router keeps for one append: a writer binding every source's
// dictionaries, 12 B an entry, plus one run, plus what a writer charges for its finish before it holds
// anything ([writerFinishBase]).
func appendReserve(schema *Schema, entries int, runBytes int64) int64 {
	const bindEntryBytes = 12

	if runBytes <= 0 {
		runBytes = mergeFloorRun
	}

	return runBytes + int64(entries)*bindEntryBytes + writerFinishBase(schema)
}

// writerFinishBase is the least a writer charges for its finish ([recordPartStreamWriter.finishBytes]):
// one frame of the default compress block compressed, and each bloom column's smallest filter built,
// encoded and read back.
func writerFinishBase(schema *Schema) int64 {
	const (
		frame  = 64 << 10
		header = 32
	)

	n := 2 * (frame + frame/255 + 64)

	for k := range schema.numBytes() {
		if schema.byteColumn(k).Bloom != BloomNone {
			n += 3 * (smallFilterBytes + header)
		}
	}

	return int64(n)
}

// mergeNeed is what a merge of src must be able to hold before anything opens: the sources as their
// manifests bound them, and on top the larger of what opening one column holds — the sources open
// one column at a time, before any writer — and the encoders with the writers' floor: two appends and
// one writer's finish at the cap ([Engine.writerFinishAtCap]).
// ok is false when a source's manifest cannot say.
func (e *Engine) mergeNeed(ctx context.Context, src []*part, capBytes int64) (int64, bool) {
	var (
		steady, open int64
		entries      int
	)

	for _, p := range src {
		b, ok := e.sourceBound(ctx, p)
		if !ok {
			return 0, false
		}

		steady += b.steady
		open = max(open, b.open)
		entries += b.entries
	}

	coders, err := e.newMergeCoders(ctx, src)
	if err != nil {
		return 0, false
	}

	writers := coders.workspace() + e.writerFinishAtCap(src, capBytes) +
		2*appendReserve(e.cfg.Schema, entries, e.mergePartBytes(capBytes)/mergeRunFraction)

	return steady + max(open, writers), true
}

// mergeCoders is what a merge's day writers share: the compressors — frames go through a 1 MiB-window
// encoder, whole objects (dictionary regions, a column written unframed, the stream id column)
// through one whose window fits the largest such object the merge can write, each used by one writer
// at a time, so a merge holds one encoder of each — and sidecars, what a side store's sidecar union
// holds while a part finishes ([Engine.sidecarBytes]).
type mergeCoders struct {
	frames, objects *compress.Compressor
	sidecars        int64
}

func (e *Engine) newMergeCoders(ctx context.Context, src []*part) (*mergeCoders, error) {
	alg, level := e.cfg.MergeCompression, e.cfg.MergeCompressionLevel

	sidecars, err := e.sidecarBytes(ctx, src)
	if err != nil {
		return nil, err
	}

	return &mergeCoders{
		frames:   block.NewFrameCompressor(alg, level),
		objects:  compress.NewFrameCompressor(alg, level, mergeObjectBytes(src, !backend.StreamsWrites(e.cfg.Backend))),
		sidecars: sidecars,
	}, nil
}

// workspace is what the merge holds in them beside its writers: an encoder of each compressor, and
// one sidecar union, since the router finishes one writer at a time.
func (c *mergeCoders) workspace() int64 {
	return c.frames.EncodeWorkspace() + c.objects.EncodeWorkspace() + c.sidecars
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

// residentBytes is what an open part holds in RAM beyond its reader: its stream ranges, blooms and
// record keys. A merge keeps the parts it seals until it commits them.
func (p *part) residentBytes() int64 {
	n := int64(cap(p.ranges)) * int64(unsafe.Sizeof(streamRange{}))

	for _, f := range p.blooms {
		n += f.SizeBytes()
	}

	for _, k := range p.recordKeys {
		n += int64(len(k)) + viewBytes
	}

	return n
}

// writerFinishAtCap estimates what one writer charges for its finish
// ([recordPartStreamWriter.finishBytes]) once it holds a part at the cap, so the writers' floor lets a
// part reach it: the cap's share of the sources' rows as stream ids, every source stream's range,
// watermark and identity, the sources' blooms and record keys built, encoded and read back, and for
// each dictionary-coded column its region, serialized and compressed, at the least of its sources'
// shared dictionaries, the cap and the dictionary cap; a column no source built a dictionary for
// charges only its last frame. With no cap, or a side store, a part is the whole merge.
//
// It sizes the floor, not a charge: a writer charges its actual finish in its resident, so an output
// dictionary past its sources' — values their declined granules held joining the merged one — seals
// that part early rather than holding more than the grant.
func (e *Engine) writerFinishAtCap(src []*part, capBytes int64) int64 {
	const (
		idBytes    = 16
		runBytes   = 24
		wmarkBytes = 24
	)

	var rows, decoded, streams, sidecars int64

	for _, p := range src {
		rows += int64(p.reader.RowCount())
		decoded += p.sizeBytes()
		streams += int64(len(p.ranges))

		for _, f := range p.blooms {
			sidecars += 3 * f.SizeBytes()
		}

		for _, k := range p.recordKeys {
			sidecars += 3 * (int64(len(k)) + viewBytes)
		}
	}

	part := decoded
	if c := e.mergePartBytes(capBytes); c > 0 {
		part = min(part, c)
	}

	n := sidecars + writerFinishBase(e.cfg.Schema)
	if decoded > 0 {
		n += int64(idBytes * float64(rows) * float64(part) / float64(decoded))
	}

	n += streams * (runBytes + int64(unsafe.Sizeof(streamRange{})) + wmarkBytes + int64(unsafe.Sizeof(series.Entry{})))
	n += e.identitiesBound(src)

	for k := range e.cfg.Schema.numBytes() {
		if e.cfg.Schema.byteColumn(k).Codec != chunk.CodecDict {
			continue
		}

		var dict int64

		for _, p := range src {
			if desc, ok := p.reader.ColumnDescByName(p.schema.byteColumn(k).Name); ok && desc.SharedDict {
				dict += desc.DictRaw
			}
		}

		// The dictionary serialized, and the region buffer it compresses into at worst.
		dict = min(dict, part, block.DefaultSharedDictBytes)
		n += 2*dict + dict/255 + 128
	}

	return n
}

// identitiesBound bounds the identity objects of the sources' streams as a finish encodes them
// ([recordPartStreamWriter.identityBound]).
func (e *Engine) identitiesBound(src []*part) int64 {
	var (
		n       int64
		scratch []byte
	)

	e.mu.RLock()
	defer e.mu.RUnlock()

	for _, p := range src {
		for _, r := range p.ranges {
			if s, ok := e.head.series.Get(r.id); ok {
				n += identityBytes(s, &scratch)
			}
		}
	}

	return n
}
