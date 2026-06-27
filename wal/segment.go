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
	"go.uber.org/zap"

	"github.com/oteldb/storage/internal/obs"
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
	epoch    uint64 // flush generation stamped into new segment names; see [SegmentWriter.SetEpoch]
	f        *os.File
	size     int
	w        *Writer
	sync     bool        // fsync after every framed write (durability vs throughput)
	metrics  *obs.WAL    // append/fsync/rotation counters; nil ⇒ not metered
	log      *zap.Logger // segment open/rotate/checkpoint logging; nil ⇒ no-op
}

// SetObs attaches the WAL metrics handle (append/fsync/rotation counters). nil disables metering.
func (sw *SegmentWriter) SetObs(m *obs.WAL) { sw.metrics = m }

// SetLogger attaches a logger that records segment lifecycle events (open/rotate/checkpoint) at
// Debug. The WAL append path takes no context, so these lines are not trace-correlated. nil ⇒ no-op.
func (sw *SegmentWriter) SetLogger(l *zap.Logger) {
	if l != nil {
		sw.log = l
	}
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

	// Open lazily on the first write, so the segment carries whatever epoch the engine has set by
	// then (and a resumed dir's prior segments are left intact for replay). Epoch starts at 1 (the
	// first generation; the recovery watermark is 0), so a writer that never calls SetEpoch — the
	// metric engine — still has all its segments replayed by ReplayDir's epoch>0 filter.
	sw := &SegmentWriter{dir: dir, maxBytes: maxBytes, seq: last, epoch: 1}
	sw.w = NewWriter(sw) // the inner Writer frames records and writes them through sw

	return sw, nil
}

// SetSync enables (or disables) an fsync after every framed write — power-loss durability at a
// throughput cost. The default is off (records reach the OS page cache, surviving a process crash
// but not necessarily a power loss).
func (sw *SegmentWriter) SetSync(on bool) { sw.sync = on }

// SetEpoch stamps subsequent segments with epoch (a flush generation). Because [SegmentWriter.Checkpoint]
// closes the current segment without opening a new one, the next write starts a segment carrying the
// epoch set here — so each segment self-describes the generation of its records, and
// [ReplayDirFrom] can skip whole segments already superseded by a flushed part.
func (sw *SegmentWriter) SetEpoch(epoch uint64) { sw.epoch = epoch }

// Checkpoint discards every segment written so far. Call it after a full head flush, whose part
// durably supersedes those records. It closes the current segment and best-effort deletes all
// segments (the next write lazily opens a fresh one carrying the current epoch). The flush advances
// the epoch and persists it as the bucket-index watermark *before* this call, so even a crash between
// the part committing and this deletion replays nothing already flushed (exactly-once — the watermark
// and the part list advance atomically; see [ReplayDirFrom]).
func (sw *SegmentWriter) Checkpoint() error {
	obsolete := sw.seq

	if err := sw.Close(); err != nil { // close the current segment; next write reopens lazily
		return err
	}

	entries, err := os.ReadDir(sw.dir)
	if err != nil {
		return errors.Wrapf(err, "read wal dir %q", sw.dir)
	}

	for _, e := range entries {
		seq, _, ok := parseSegment(e.Name())
		if ok && seq <= obsolete {
			if rerr := os.Remove(filepath.Join(sw.dir, e.Name())); rerr != nil && !os.IsNotExist(rerr) {
				return errors.Wrapf(rerr, "remove obsolete segment %q", e.Name())
			}
		}
	}

	sw.logger().Debug("wal checkpoint (obsolete segments discarded)",
		zap.String("dir", sw.dir), zap.Int("through_seq", obsolete))

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
		if seq, _, ok := parseSegment(e.Name()); ok && seq > last {
			last = seq
		}
	}

	return last, nil
}

// parseSegment parses a segment file name ("{seq}-{epoch}.wal") into its sequence and epoch.
func parseSegment(name string) (seq int, epoch uint64, ok bool) {
	base, found := strings.CutSuffix(name, segmentExt)
	if !found {
		return 0, 0, false
	}

	s, e, found := strings.Cut(base, "-")
	if !found {
		return 0, 0, false
	}

	seq, err := strconv.Atoi(s)
	if err != nil || seq <= 0 {
		return 0, 0, false
	}

	epoch, err = strconv.ParseUint(e, 10, 64)
	if err != nil {
		return 0, 0, false
	}

	return seq, epoch, true
}

// Write implements [io.Writer], appending to the current segment and tracking its size
// so the writer knows when to rotate.
func (sw *SegmentWriter) Write(p []byte) (int, error) {
	n, err := sw.f.Write(p)
	sw.size += n

	return n, err
}

// WriteSeries logs a series registration (opening/rotating the segment first as needed).
func (sw *SegmentWriter) WriteSeries(id signal.SeriesID, s signal.Series) error {
	if err := sw.prepare(); err != nil {
		return err
	}

	return sw.afterWrite(sw.w.WriteSeries(id, s))
}

// WriteSamples logs a run of samples for one series.
func (sw *SegmentWriter) WriteSamples(id signal.SeriesID, ts []int64, values []float64) error {
	if err := sw.prepare(); err != nil {
		return err
	}

	return sw.afterWrite(sw.w.WriteSamples(id, ts, values))
}

// WriteSamplesSF logs a run of samples for one series that also carry per-sample scale factors.
func (sw *SegmentWriter) WriteSamplesSF(id signal.SeriesID, ts []int64, values, sf []float64) error {
	if err := sw.prepare(); err != nil {
		return err
	}

	return sw.afterWrite(sw.w.WriteSamplesSF(id, ts, values, sf))
}

// WriteRecords logs a stream's opaque engine-encoded record payload.
func (sw *SegmentWriter) WriteRecords(id signal.SeriesID, payload []byte) error {
	if err := sw.prepare(); err != nil {
		return err
	}

	return sw.afterWrite(sw.w.WriteRecords(id, payload))
}

// WriteSide logs an opaque engine-encoded side-store delta.
func (sw *SegmentWriter) WriteSide(payload []byte) error {
	if err := sw.prepare(); err != nil {
		return err
	}

	return sw.afterWrite(sw.w.WriteSide(payload))
}

// Sync flushes the current segment to stable storage (no-op when no segment is open).
func (sw *SegmentWriter) Sync() error {
	if sw.f == nil {
		return nil
	}

	if sw.metrics != nil {
		sw.metrics.Fsync()
	}

	return sw.f.Sync()
}

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

// prepare ensures a current segment is open and not over the size limit before a write.
func (sw *SegmentWriter) prepare() error {
	switch {
	case sw.f == nil:
		return sw.openNext()
	case sw.size >= sw.maxBytes:
		return sw.rotate()
	default:
		return nil
	}
}

// afterWrite fsyncs the segment when the sync policy is on (and the write succeeded).
func (sw *SegmentWriter) afterWrite(err error) error {
	if err != nil {
		return err
	}

	if sw.metrics != nil {
		sw.metrics.Append()
	}

	if !sw.sync {
		return nil
	}

	if sw.metrics != nil {
		sw.metrics.Fsync()
	}

	return sw.f.Sync()
}

func (sw *SegmentWriter) logger() *zap.Logger {
	if sw.log == nil {
		return zap.NewNop()
	}

	return sw.log
}

func (sw *SegmentWriter) rotate() error {
	if err := sw.Close(); err != nil {
		return err
	}

	if sw.metrics != nil {
		sw.metrics.Rotate()
	}

	sw.logger().Debug("wal rotate", zap.String("dir", sw.dir), zap.Int("from_seq", sw.seq), zap.Int("size", sw.size))

	return sw.openNext()
}

func (sw *SegmentWriter) openNext() error {
	sw.seq++
	name := filepath.Join(sw.dir, segmentName(sw.seq, sw.epoch))

	f, err := os.OpenFile(name, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o600) //nolint:gosec // name is a segment file under the WAL dir
	if err != nil {
		return errors.Wrapf(err, "open segment %q", name)
	}

	sw.f, sw.size = f, 0
	sw.logger().Debug("wal segment opened", zap.String("name", name), zap.Int("seq", sw.seq), zap.Uint64("epoch", sw.epoch))

	return nil
}

// segmentName encodes a segment's sequence (the sort/replay order) and epoch (the flush generation
// of its records) into "{seq}-{epoch}.wal".
func segmentName(seq int, epoch uint64) string {
	return fmt.Sprintf("%020d-%020d%s", seq, epoch, segmentExt)
}

// ReplayDir replays every segment in dir (all epochs). See [ReplayDirFrom].
func ReplayDir(dir string, h Handlers) error { return ReplayDirFrom(dir, 0, h) }

// ReplayDirFrom replays the segments in dir whose epoch is greater than minEpoch, in ascending
// segment order, dispatching each record to h. Segments at or below minEpoch are skipped — their
// records are already durable in a flushed part (the watermark), so skipping them makes recovery
// exactly-once. A torn final record in the last replayed segment ends replay cleanly.
func ReplayDirFrom(dir string, minEpoch uint64, h Handlers) error {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return errors.Wrapf(err, "read wal dir %q", dir)
	}

	type seg struct {
		seq  int
		name string
	}

	var segs []seg
	for _, e := range entries {
		if seq, epoch, ok := parseSegment(e.Name()); ok && epoch > minEpoch {
			segs = append(segs, seg{seq: seq, name: e.Name()})
		}
	}

	slices.SortFunc(segs, func(a, b seg) int { return a.seq - b.seq })

	for _, s := range segs {
		data, err := os.ReadFile(filepath.Join(dir, s.name)) //nolint:gosec // name is a *.wal entry under dir
		if err != nil {
			return errors.Wrapf(err, "read segment %q", s.name)
		}

		if err := Replay(data, h); err != nil {
			return errors.Wrapf(err, "replay segment %q", s.name)
		}
	}

	return nil
}

// ensure io.Writer is satisfied.
var _ io.Writer = (*SegmentWriter)(nil)
