package wal

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
	"slices"
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

// Create opens (creating the directory if needed) a new segmented WAL writer. A
// non-positive maxBytes uses [DefaultMaxSegmentBytes].
func Create(dir string, maxBytes int) (*SegmentWriter, error) {
	if maxBytes <= 0 {
		maxBytes = DefaultMaxSegmentBytes
	}

	if err := os.MkdirAll(dir, 0o750); err != nil {
		return nil, errors.Wrapf(err, "create wal dir %q", dir)
	}

	sw := &SegmentWriter{dir: dir, maxBytes: maxBytes}
	sw.w = NewWriter(sw) // the inner Writer frames records and writes them through sw

	if err := sw.openNext(); err != nil {
		return nil, err
	}

	return sw, nil
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

// WriteLogRecords logs a run of log records for one stream, rotating first if the current
// segment is full.
func (sw *SegmentWriter) WriteLogRecords(id signal.SeriesID, recs []LogRecord) error {
	if sw.size >= sw.maxBytes {
		if err := sw.rotate(); err != nil {
			return err
		}
	}

	return sw.w.WriteLogRecords(id, recs)
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
