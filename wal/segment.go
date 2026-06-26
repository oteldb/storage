package wal

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"

	"github.com/go-faster/errors"

	"github.com/oteldb/storage/signal"
)

const (
	segmentExt = ".wal"

	// DefaultMaxSegmentBytes is the size at which the writer rotates to a new segment.
	DefaultMaxSegmentBytes = 32 << 20 // 32 MiB
)

// SegmentWriter appends WAL records to numbered segment files in a directory, rotating
// to a fresh segment once the current one reaches the size limit. Replaying the
// directory in order ([ReplayDir]) reconstructs the logged state. Not safe for
// concurrent use.
type SegmentWriter struct {
	dir      string
	maxBytes int
	seq      int
	f        *os.File
	size     int
	w        *Writer
}

// Create opens (creating the directory if needed) a segmented WAL writer. A non-positive maxBytes
// uses [DefaultMaxSegmentBytes]. If the directory already holds segments from a prior run, Create
// **resumes**: it opens a fresh segment numbered beyond the existing ones (never truncating them), so
// [ReplayDir] can still recover the prior segments before the next [SegmentWriter.Checkpoint] discards
// them.
func Create(dir string, maxBytes int) (*SegmentWriter, error) {
	if maxBytes <= 0 {
		maxBytes = DefaultMaxSegmentBytes
	}

	if err := os.MkdirAll(dir, 0o750); err != nil {
		return nil, errors.Wrapf(err, "create wal dir %q", dir)
	}

	last, err := lastSegmentSeq(dir)
	if err != nil {
		return nil, err
	}

	sw := &SegmentWriter{dir: dir, maxBytes: maxBytes, seq: last}
	sw.w = NewWriter(sw) // the inner Writer frames records and writes them through sw

	if err := sw.openNext(); err != nil {
		return nil, err
	}

	return sw, nil
}

// Checkpoint discards every segment written so far and starts a fresh one. Call it after a full head
// flush, whose part durably supersedes those records. It rotates to a new segment, then best-effort
// deletes the obsolete ones. (Part commit and this deletion are not atomic across the two stores: a
// crash in between can leave a deleted-part's records to be replayed, a benign re-flush — deduped on
// merge for metrics, a rare duplicate for append-only records.)
func (sw *SegmentWriter) Checkpoint() error {
	obsolete := sw.seq // the segment currently open, about to be closed by rotate

	if err := sw.rotate(); err != nil {
		return err
	}

	for i := 1; i <= obsolete; i++ {
		if err := os.Remove(filepath.Join(sw.dir, segmentName(i))); err != nil && !os.IsNotExist(err) {
			return errors.Wrapf(err, "remove obsolete segment %d", i)
		}
	}

	return nil
}

// lastSegmentSeq returns the highest segment number present in dir, or 0 if none.
func lastSegmentSeq(dir string) (int, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return 0, errors.Wrapf(err, "read wal dir %q", dir)
	}

	last := 0
	for _, e := range entries {
		if e.IsDir() || !strings.HasSuffix(e.Name(), segmentExt) {
			continue
		}

		if seq, ok := parseSegmentSeq(e.Name()); ok && seq > last {
			last = seq
		}
	}

	return last, nil
}

// parseSegmentSeq parses a segment file name (e.g. "00000007.wal") to its sequence number.
func parseSegmentSeq(name string) (int, bool) {
	base := strings.TrimSuffix(name, segmentExt)

	seq, err := strconv.Atoi(base)
	if err != nil || seq <= 0 {
		return 0, false
	}

	return seq, true
}

// Write implements [io.Writer], appending to the current segment and tracking its size
// so the writer knows when to rotate.
func (sw *SegmentWriter) Write(p []byte) (int, error) {
	n, err := sw.f.Write(p)
	sw.size += n

	return n, err
}

// WriteSeries logs a series registration, rotating to a new segment first if the current
// one is full.
func (sw *SegmentWriter) WriteSeries(id signal.SeriesID, s signal.Series) error {
	if sw.size >= sw.maxBytes {
		if err := sw.rotate(); err != nil {
			return err
		}
	}

	return sw.w.WriteSeries(id, s)
}

// WriteSamples logs a run of samples for one series, rotating first if the current
// segment is full.
func (sw *SegmentWriter) WriteSamples(id signal.SeriesID, ts []int64, values []float64) error {
	if sw.size >= sw.maxBytes {
		if err := sw.rotate(); err != nil {
			return err
		}
	}

	return sw.w.WriteSamples(id, ts, values)
}

// WriteRecords logs a stream's opaque engine-encoded record payload, rotating first if the current
// segment is full.
func (sw *SegmentWriter) WriteRecords(id signal.SeriesID, payload []byte) error {
	if sw.size >= sw.maxBytes {
		if err := sw.rotate(); err != nil {
			return err
		}
	}

	return sw.w.WriteRecords(id, payload)
}

// WriteSide logs an opaque engine-encoded side-store delta, rotating first if the current segment is
// full.
func (sw *SegmentWriter) WriteSide(payload []byte) error {
	if sw.size >= sw.maxBytes {
		if err := sw.rotate(); err != nil {
			return err
		}
	}

	return sw.w.WriteSide(payload)
}

// Sync flushes the current segment to stable storage.
func (sw *SegmentWriter) Sync() error { return sw.f.Sync() }

// Close syncs and closes the current segment.
func (sw *SegmentWriter) Close() error {
	if sw.f == nil {
		return nil
	}

	err := sw.f.Sync()
	if cerr := sw.f.Close(); err == nil {
		err = cerr
	}

	sw.f = nil

	return err
}

func (sw *SegmentWriter) rotate() error {
	if err := sw.Close(); err != nil {
		return err
	}

	return sw.openNext()
}

func (sw *SegmentWriter) openNext() error {
	sw.seq++
	name := filepath.Join(sw.dir, segmentName(sw.seq))

	f, err := os.OpenFile(name, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o600) //nolint:gosec // name is a segment file under the WAL dir
	if err != nil {
		return errors.Wrapf(err, "open segment %q", name)
	}

	sw.f, sw.size = f, 0

	return nil
}

func segmentName(seq int) string { return fmt.Sprintf("%08d%s", seq, segmentExt) }

// ReplayDir replays every segment in dir, in ascending segment order, dispatching each
// record to h. A torn final record in the last segment ends replay cleanly.
func ReplayDir(dir string, h Handlers) error {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return errors.Wrapf(err, "read wal dir %q", dir)
	}

	var segs []string
	for _, e := range entries {
		if !e.IsDir() && strings.HasSuffix(e.Name(), segmentExt) {
			segs = append(segs, e.Name())
		}
	}

	slices.Sort(segs) // zero-padded names sort in segment order

	for _, name := range segs {
		data, err := os.ReadFile(filepath.Join(dir, name)) //nolint:gosec // name is a *.wal entry under dir
		if err != nil {
			return errors.Wrapf(err, "read segment %q", name)
		}

		if err := Replay(data, h); err != nil {
			return errors.Wrapf(err, "replay segment %q", name)
		}
	}

	return nil
}

// ensure io.Writer is satisfied.
var _ io.Writer = (*SegmentWriter)(nil)
