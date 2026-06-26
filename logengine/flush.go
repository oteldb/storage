package logengine

import (
	"context"
	"fmt"
	"slices"

	"github.com/go-faster/errors"

	"github.com/oteldb/storage/backend"
	"github.com/oteldb/storage/block"
	"github.com/oteldb/storage/encoding/chunk"
	"github.com/oteldb/storage/signal"
)

// flushColumns is the head's buffered records laid out as flat part columns: the stream-id sort
// grouping plus the per-record column set, one row per record, sorted by (stream, ts).
type flushColumns struct {
	stream []chunk.U128
	cols   recordCols
}

func (f *flushColumns) len() int { return len(f.stream) }

// drainHead snapshots every buffered record into part columns sorted by (stream, ts) and clears
// the head's record buffers (the stream index is retained — identities outlive a flush). It
// returns nil if no stream has buffered records.
func (h *head) drainHead() *flushColumns {
	ids := make([]signal.SeriesID, 0, len(h.records))
	for id, buf := range h.records {
		if buf.len() > 0 {
			ids = append(ids, id)
		}
	}

	if len(ids) == 0 {
		return nil
	}

	slices.SortFunc(ids, func(a, b signal.SeriesID) int { return a.Compare(b) })

	f := &flushColumns{}

	for _, id := range ids {
		buf := h.records[id]

		// Sort each stream's records by ts so the part is ordered by (stream, ts).
		ordered := &recordCols{}
		for i := range buf.ts {
			ordered.appendRow(buf, i)
		}

		ordered.sortByTs()

		u := idToU128(id)
		for i := range ordered.ts {
			f.stream = append(f.stream, u)
			f.cols.appendRow(ordered, i)
		}
	}

	h.records = make(map[signal.SeriesID]*recordCols)

	return f
}

const (
	minInt64 = int64(-1 << 63)
	maxInt64 = int64(1<<63 - 1)
)

// writePart writes f as a log part under prefix via [block.PartWriter]. Numeric columns use the
// integer codecs (DoD for the monotonic-within-stream timestamp, T64 for the small-range fields);
// byte columns dict-encode (bodies and ids repeat heavily across a stream).
func writePart(ctx context.Context, b backend.Backend, prefix string, f *flushColumns) error {
	w := block.NewPartWriter(block.WithSortKey(colTs))

	add := func(c block.Column) error { return w.AddColumn(c) }

	if err := add(block.Column{Name: colStream, Kind: block.KindInt128, Int128: f.stream}); err != nil {
		return err
	}

	intCols := []struct {
		name  string
		data  []int64
		codec chunk.Codec
	}{
		{colTs, f.cols.ts, chunk.CodecDoD},
		{colObserved, f.cols.observed, chunk.CodecT64},
		{colSeverity, f.cols.severity, chunk.CodecT64},
		{colFlags, f.cols.flags, chunk.CodecT64},
		{colDropped, f.cols.dropped, chunk.CodecT64},
	}
	for _, c := range intCols {
		if err := add(block.Column{Name: c.name, Kind: block.KindInt64, Codec: c.codec, Int64: c.data}); err != nil {
			return err
		}
	}

	byteCols := []struct {
		name string
		data [][]byte
	}{
		{colSevText, f.cols.sevText},
		{colBody, f.cols.body},
		{colTraceID, f.cols.traceID},
		{colSpanID, f.cols.spanID},
		{colAttrs, f.cols.attrs},
	}
	for _, c := range byteCols {
		if err := add(block.Column{Name: c.name, Kind: block.KindBytes, Codec: chunk.CodecDict, Bytes: c.data}); err != nil {
			return err
		}
	}

	if err := block.WritePart(ctx, b, prefix, w); err != nil {
		return errors.Wrapf(err, "write part %q", prefix)
	}

	return nil
}

// partPrefix is the backend key prefix of the seq-th part of this engine.
func (e *Engine) partPrefix(seq int) string {
	return fmt.Sprintf("%s/%010d", e.cfg.Prefix, seq)
}

// colsTimeRange returns the inclusive min/max timestamp across f (which has ≥ 1 record when a part
// is written).
func colsTimeRange(f *flushColumns) (minTime, maxTime int64) {
	minTime, maxTime = maxInt64, minInt64
	for _, t := range f.cols.ts {
		if t < minTime {
			minTime = t
		}

		if t > maxTime {
			maxTime = t
		}
	}

	return minTime, maxTime
}
