package wal

import (
	"fmt"
	"io"
	"os"
	"slices"
	"strconv"
	"strings"

	"github.com/go-faster/errors"
	"go.uber.org/zap"

	"github.com/oteldb/storage/internal/obs"
	"github.com/oteldb/storage/internal/vfs"
	"github.com/oteldb/storage/signal"
)

const (
	segmentExt = ".wal"
	repairExt  = ".repair"

	// DefaultMaxSegmentBytes is the size at which the writer rotates to a new segment.
	DefaultMaxSegmentBytes = 32 << 20 // 32 MiB
)

// SegmentWriter appends WAL records to numbered segment files in a directory, rotating
// to a fresh segment once the current one reaches the size limit. Replaying the
// directory in order ([ReplayDir]) reconstructs the logged state. Not safe for
// concurrent use.
type SegmentWriter struct {
	fsys     vfs.FS // rooted at the segment directory; held for the writer's lifetime
	dir      string // the rooted directory's name, for log lines only
	closed   bool   // the directory handle is released; no further segment can be opened
	maxBytes int
	seq      int
	epoch    uint64 // flush generation stamped into new segment names; see [SegmentWriter.SetEpoch]
	f        vfs.File
	name     string // the open segment's name: its epoch is the one current when it was opened
	size     int
	w        *Writer
	sync     bool        // fsync after every framed write (durability vs throughput)
	metrics  *obs.WAL    // append/fsync/rotation counters; nil ⇒ not metered
	log      *zap.Logger // segment open/rotate/checkpoint logging; nil ⇒ no-op

	// tornName is the segment a failed write may have left bytes in past its last whole frame, and
	// tornGood the length of that whole-frame prefix. Empty when no write has failed since the last
	// heal. See [SegmentWriter.heal].
	tornName string
	tornGood int

	// rotateSyncErr is the failed sync of a segment a rotation closed. The write that rotated reports
	// its own failure, but the records already in that segment were acknowledged and may not survive a
	// power cut, so the next Sync, Seal or Close reports it too: none may claim them durable.
	rotateSyncErr error
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
// **resumes**: it repairs the last segment's torn tail (see [repair]) and opens a fresh segment
// numbered beyond the existing ones, so [ReplayDir] can still recover the prior segments before the
// next [SegmentWriter.Checkpoint] discards them.
func Create(dir string, maxBytes int) (*SegmentWriter, error) {
	fsys, err := vfs.OpenRoot(dir, 0o750)
	if err != nil {
		return nil, errors.Wrapf(err, "create wal dir %q", dir)
	}

	sw, err := createFS(fsys, maxBytes)
	if err != nil {
		_ = fsys.Close()

		return nil, err
	}

	sw.dir = dir

	return sw, nil
}

// createFS is [Create] over an already-rooted filesystem — the seam the durability tests inject a
// crash model through. The writer keeps fsys open for its lifetime.
func createFS(fsys vfs.FS, maxBytes int) (*SegmentWriter, error) {
	if maxBytes <= 0 {
		maxBytes = DefaultMaxSegmentBytes
	}

	last, err := lastSegmentSeq(fsys)
	if err != nil {
		return nil, err
	}

	if err := removeRepairTemps(fsys); err != nil {
		return nil, err
	}

	if err := repair(fsys, last); err != nil {
		return nil, err
	}

	// Open lazily on the first write, so the segment carries whatever epoch the engine has set by
	// then (and a resumed dir's prior segments are left intact for replay). Epoch starts at 1 (the
	// first generation; the recovery watermark is 0), so a writer that never calls SetEpoch — the
	// metric engine — still has all its segments replayed by ReplayDir's epoch>0 filter.
	sw := &SegmentWriter{fsys: fsys, maxBytes: maxBytes, seq: last, epoch: 1}
	sw.w = NewWriter(sw) // the inner Writer frames records and writes them through sw

	return sw, nil
}

// Seq returns the current segment sequence number — the count of segments opened so far (0 before
// the first write opens one). A cheap in-memory read for introspection; not safe for concurrent use.
func (sw *SegmentWriter) Seq() int { return sw.seq }

// Size returns the byte size of the current open segment (0 when none is open). A cheap in-memory
// read for introspection; not safe for concurrent use.
func (sw *SegmentWriter) Size() int { return sw.size }

// Epoch returns the flush generation stamped into new segments (see [SegmentWriter.SetEpoch]). A
// cheap in-memory read for introspection; not safe for concurrent use.
func (sw *SegmentWriter) Epoch() uint64 { return sw.epoch }

// SetSync enables (or disables) an fsync after every framed write — power-loss durability at a
// throughput cost. The default is off (records reach the OS page cache, surviving a process crash
// but not necessarily a power loss).
func (sw *SegmentWriter) SetSync(on bool) { sw.sync = on }

// SetEpoch stamps subsequent segments with epoch (a flush generation). Because [SegmentWriter.Checkpoint]
// closes the current segment without opening a new one, the next write starts a segment carrying the
// epoch set here — so each segment self-describes the generation of its records, and
// [ReplayDirFrom] can skip whole segments already superseded by a flushed part.
func (sw *SegmentWriter) SetEpoch(epoch uint64) { sw.epoch = epoch }

// Seal closes the current segment and stamps subsequent ones with epoch, returning the sequence
// number it sealed through — the argument [SegmentWriter.CheckpointThrough] takes once the flush of
// those records commits. Like [SegmentWriter.Sync], it fails while a torn segment cannot be restored.
//
// A flush seals at the instant it detaches the head, not when it publishes: the part it goes on to
// write off-lock holds exactly the records logged before that instant. A record appended during the
// part write then lands in a segment beyond the sealed number, carrying the generation past the
// watermark the flush is about to commit — so the checkpoint does not delete it and replay does not
// skip it.
func (sw *SegmentWriter) Seal(epoch uint64) (int, error) {
	if err := sw.heal(); err != nil {
		return 0, err
	}

	if err := sw.closeSegment(); err != nil { // the next write lazily opens a segment carrying epoch
		return 0, err
	}

	if err := sw.takeRotateSyncErr(); err != nil {
		return 0, err
	}

	sw.epoch = epoch

	return sw.seq, nil
}

// Checkpoint discards every segment written so far. Call it only when a durable part supersedes
// every record logged up to now — a flush that ran concurrently with ingest must use
// [SegmentWriter.CheckpointThrough] with the sequence its [SegmentWriter.Seal] returned instead.
func (sw *SegmentWriter) Checkpoint() error { return sw.CheckpointThrough(sw.seq) }

// CheckpointThrough discards the segments up to and including through, whose records a flushed part
// durably supersedes; later segments (holding records appended while that part was being written)
// are kept. The flush advances the epoch and persists it as the bucket-index watermark *before* this
// call, so even a crash between the part committing and this deletion replays nothing already
// flushed (exactly-once — the watermark and the part list advance atomically; see [ReplayDirFrom]).
func (sw *SegmentWriter) CheckpointThrough(obsolete int) error {
	if obsolete >= sw.seq {
		if err := sw.closeSegment(); err != nil { // close the current segment; next write reopens lazily
			return err
		}
	}

	entries, err := sw.fsys.ReadDir(".")
	if err != nil {
		return errors.Wrapf(err, "read wal dir %q", sw.dir)
	}

	for _, e := range entries {
		seq, _, ok := parseSegment(e.Name())
		if ok && seq <= obsolete {
			if rerr := sw.fsys.Remove(e.Name()); rerr != nil && !os.IsNotExist(rerr) {
				return errors.Wrapf(rerr, "remove obsolete segment %q", e.Name())
			}
		}
	}

	// The removals are only durable once the directory is: without this a power cut can bring back
	// segments a flush already superseded, and replay re-applies records the parts hold.
	if err := sw.fsys.SyncDir("."); err != nil {
		return errors.Wrapf(err, "sync wal dir %q", sw.dir)
	}

	sw.logger().Debug("wal checkpoint (obsolete segments discarded)",
		zap.String("dir", sw.dir), zap.Int("through_seq", obsolete))

	return nil
}

// lastSegmentSeq returns the highest segment number present in fsys, or 0 if none.
func lastSegmentSeq(fsys vfs.FS) (int, error) {
	entries, err := fsys.ReadDir(".")
	if err != nil {
		return 0, errors.Wrap(err, "read wal dir")
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
//
// A failed write can still land a prefix of p — a full or failing disk returns a short write — and a
// later write appending behind it would turn a torn tail, which recovery discards, into corruption in
// the middle of the log, which fails every replay. So a failure is healed before anything else is
// written: see [SegmentWriter.heal].
func (sw *SegmentWriter) Write(p []byte) (int, error) {
	if err := sw.prepare(); err != nil {
		return 0, err
	}

	good := sw.size

	n, err := sw.f.Write(p)
	if err == nil {
		sw.size += n

		return n, nil
	}

	sw.tornName, sw.tornGood = sw.name, good
	if herr := sw.heal(); herr != nil {
		sw.logger().Warn("wal write failed and its segment could not be restored yet; writes are refused until it is",
			zap.String("dir", sw.dir), zap.String("segment", sw.tornName), zap.Error(herr))
	}

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

// WriteFrames logs a run of records that is **already framed** by a [Writer] — the form the cluster
// write path builds to replicate the accepted set. It is byte-identical to writing those records one
// call at a time, without re-encoding them. p must end on a frame boundary; an empty p is a no-op.
func (sw *SegmentWriter) WriteFrames(p []byte) error {
	if len(p) == 0 {
		return nil
	}

	if err := sw.prepare(); err != nil {
		return err
	}

	_, err := sw.Write(p)

	return sw.afterWrite(err)
}

// WriteSide logs an opaque engine-encoded side-store delta.
func (sw *SegmentWriter) WriteSide(payload []byte) error {
	if err := sw.prepare(); err != nil {
		return err
	}

	return sw.afterWrite(sw.w.WriteSide(payload))
}

// Sync flushes the current segment to stable storage (no-op when no segment is open). It first finishes
// restoring a segment a failed write tore, and fails while it cannot: the records before the failed
// write are durable only once the restore is. It also fails, once, when a segment a rotation closed
// failed to sync since the last call, so a nil return covers every record acknowledged since then.
func (sw *SegmentWriter) Sync() error {
	if err := sw.heal(); err != nil {
		return err
	}

	if err := sw.takeRotateSyncErr(); err != nil {
		return err
	}

	if sw.f == nil {
		return nil
	}

	if sw.metrics != nil {
		sw.metrics.Fsync()
	}

	return sw.f.Sync()
}

// Close syncs and closes the current segment and releases the writer's handle on the segment
// directory. The writer is spent: a later append cannot open a segment. Idempotent. Like
// [SegmentWriter.Sync], it fails when a torn segment cannot be restored.
func (sw *SegmentWriter) Close() error {
	var err error
	if !sw.closed {
		err = sw.heal()
	}

	if cerr := sw.closeSegment(); err == nil {
		err = cerr
	}

	if rerr := sw.takeRotateSyncErr(); err == nil {
		err = rerr
	}

	if !sw.closed {
		sw.closed = true
		if cerr := sw.fsys.Close(); err == nil {
			err = cerr
		}
	}

	return err
}

func (sw *SegmentWriter) takeRotateSyncErr() error {
	err := sw.rotateSyncErr
	sw.rotateSyncErr = nil

	return err
}

// closeSegment syncs and closes the current segment, leaving the directory handle open so the next
// append can lazily open the following segment. Rotation, sealing, and checkpointing use it.
func (sw *SegmentWriter) closeSegment() error {
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
	// Healing comes first: opening the next segment behind a torn one would leave the tear in the
	// middle of the log.
	if err := sw.heal(); err != nil {
		return err
	}

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
	if err := sw.closeSegment(); err != nil {
		sw.rotateSyncErr = errors.Wrap(err, "a rotated segment failed to sync")

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
	name := segmentName(sw.seq, sw.epoch)

	f, err := sw.fsys.OpenFile(name, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o600)
	if err != nil {
		return errors.Wrapf(err, "open segment %q", name)
	}

	// Syncing the segment commits its bytes; the entry naming them reaches the disk only when the
	// directory does. Without this, a power cut after a synced append can leave records on the
	// platter that no name reaches.
	if err := sw.fsys.SyncDir("."); err != nil {
		_ = f.Close()

		return errors.Wrapf(err, "sync wal dir %q", sw.dir)
	}

	sw.f, sw.name, sw.size = f, name, 0
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
// exactly-once. A torn final record in the **last** replayed segment ends replay cleanly, but only
// once nothing whole is found after it; a torn record anywhere earlier, or one followed by a
// CRC-valid frame, is a hole in the middle of history and returns an [ErrCorrupt]-wrapping error.
func ReplayDirFrom(dir string, minEpoch uint64, h Handlers) error {
	fsys, err := vfs.Open(dir)
	if err != nil {
		return errors.Wrapf(err, "read wal dir %q", dir)
	}
	defer func() { _ = fsys.Close() }()

	if err := replayDirFrom(fsys, minEpoch, h); err != nil {
		return errors.Wrapf(err, "wal dir %q", dir)
	}

	return nil
}

// replayDirFrom is [ReplayDirFrom] over an already-rooted filesystem.
func replayDirFrom(fsys vfs.FS, minEpoch uint64, h Handlers) error {
	segs, err := segments(fsys, minEpoch)
	if err != nil {
		return err
	}

	for i, s := range segs {
		data, err := fsys.ReadFile(s.name)
		if err != nil {
			return errors.Wrapf(err, "read segment %q", s.name)
		}

		if h.OnDamage != nil {
			if err := salvageSegment(fsys, s.name, data, i == len(segs)-1, &h); err != nil {
				return errors.Wrapf(err, "replay segment %q", s.name)
			}

			continue
		}

		n, err := replay(data, h)
		if err != nil {
			return errors.Wrapf(err, "replay segment %q", s.name)
		}

		if n == len(data) {
			continue
		}

		// Only the segment a crash was appending to may end mid-frame. Stopping short of any earlier
		// one means the rest of that segment is skipped — a hole in the middle of history that the
		// segments after it would paper over.
		if i != len(segs)-1 {
			return errors.Wrapf(ErrCorrupt, "torn record in non-final segment %q at offset %d", s.name, n)
		}

		// Even in the final segment, "ends mid-frame" is only a torn append if nothing whole follows.
		if frameAfter(data[n:]) {
			return errors.Wrapf(ErrCorrupt, "hole in final segment %q at offset %d of %d", s.name, n, len(data))
		}
	}

	return nil
}

// seg is a replayable segment: its sequence (the replay order) and its file name.
type seg struct {
	seq  int
	name string
}

// segments lists the segments in fsys whose epoch is above minEpoch, in ascending sequence order.
func segments(fsys vfs.FS, minEpoch uint64) ([]seg, error) {
	entries, err := fsys.ReadDir(".")
	if err != nil {
		return nil, errors.Wrap(err, "read wal dir")
	}

	var segs []seg
	for _, e := range entries {
		if seq, epoch, ok := parseSegment(e.Name()); ok && epoch > minEpoch {
			segs = append(segs, seg{seq: seq, name: e.Name()})
		}
	}

	slices.SortFunc(segs, func(a, b seg) int { return a.seq - b.seq })

	return segs, nil
}

// repair cuts the segment numbered last back to its readable log — the shape a crash mid-append
// leaves it in. It is what keeps a resumed directory replayable: [Create] opens a *new* segment beyond
// the existing ones, so a torn tail left behind becomes a permanent middle segment. The discarded bytes
// are an incomplete frame no valid frame follows, unreadable by construction, and repairing an intact
// segment is a no-op.
//
// Only the tail is cut. A frame that fails its CRC, or a hole with whole frames past it, is left in
// place for replay: a strict replay reports it, a salvaging one skips it and keeps what follows, and
// either way cutting it here would erase the records past it and the evidence of the damage.
func repair(fsys vfs.FS, last int) error {
	if last == 0 {
		return nil
	}

	segs, err := segments(fsys, 0)
	if err != nil {
		return err
	}

	i := slices.IndexFunc(segs, func(s seg) bool { return s.seq == last })
	if i < 0 {
		return nil
	}

	name := segs[i].name

	data, err := fsys.ReadFile(name)
	if err != nil {
		return errors.Wrapf(err, "read segment %q", name)
	}

	n, _ := walk(data, nil, func(int, int, error) error { return nil })
	if n == len(data) {
		return nil
	}

	if err := replaceSegment(fsys, name, data[:n], nil); err != nil {
		return errors.Wrapf(err, "repair segment %q", name)
	}

	if err := fsys.SyncDir("."); err != nil {
		return errors.Wrap(err, "sync wal dir")
	}

	return nil
}

// heal restores the segment a failed write left bytes in to its last whole frame, so nothing is ever
// written behind a partial frame. It is a no-op when no write has failed, and restores nothing when the
// failed write left nothing behind or its segment has since been checkpointed away. The writer then
// moves to a fresh segment: its open handle may reach bytes the replacement dropped.
//
// The replacement is a new file renamed over the old one, never an in-place truncation: a crash midway
// through rewriting the segment would take the records already acknowledged from it too. Until heal
// succeeds every write is refused, including the one that would open the next segment. That includes
// the directory sync committing the rename, which heal repeats until it succeeds: a retry finding the
// segment already restored cannot tell whether an earlier attempt's rename reached the disk.
func (sw *SegmentWriter) heal() error {
	if sw.tornName == "" {
		return nil
	}

	name, good := sw.tornName, sw.tornGood

	data, err := sw.fsys.ReadFile(name)

	switch {
	case os.IsNotExist(err):
	case err != nil:
		return errors.Wrapf(err, "read torn segment %q", name)
	case len(data) > good:
		// While a tear is unhealed nothing opens another segment, so an open handle is the torn
		// segment's. It is closed without a sync once the synced copy holds its whole frames — its tail
		// is the partial frame being discarded — and before the rename, which Windows refuses over an
		// open file.
		release := func() {
			if sw.f != nil {
				_ = sw.f.Close()
				sw.f = nil
			}
		}

		if err := replaceSegment(sw.fsys, name, data[:good], release); err != nil {
			return errors.Wrapf(err, "restore torn segment %q", name)
		}

		sw.logger().Warn("wal write failed part-way; its segment was restored to its last whole frame",
			zap.String("dir", sw.dir), zap.String("segment", name), zap.Int("kept", good), zap.Int("dropped", len(data)-good))
	}

	if err := sw.fsys.SyncDir("."); err != nil {
		return errors.Wrapf(err, "sync wal dir %q", sw.dir)
	}

	sw.tornName, sw.tornGood = "", 0

	return nil
}

// replaceSegment replaces name's contents with data: data goes to a temporary file that is synced and
// renamed over name, calling release (when non-nil) in between. The rename is durable only once the
// caller syncs the directory. The temporary name does not parse as a segment, so one left behind is
// never replayed, and [Create] removes it.
//
// Rewriting name in place instead would put its records at risk: a crash after the truncate, or a
// rewrite that fails, leaves the segment short of records that were durable before.
func replaceSegment(fsys vfs.FS, name string, data []byte, release func()) error {
	tmp := name + repairExt

	if err := writeNew(fsys, tmp, data); err != nil {
		_ = fsys.Remove(tmp)

		return err
	}

	if release != nil {
		release()
	}

	if err := fsys.Rename(tmp, name); err != nil {
		_ = fsys.Remove(tmp)

		return err
	}

	return nil
}

// removeRepairTemps removes the temporary files a restore interrupted by a crash, or whose cleanup
// failed, left behind.
func removeRepairTemps(fsys vfs.FS) error {
	entries, err := fsys.ReadDir(".")
	if err != nil {
		return errors.Wrap(err, "read wal dir")
	}

	for _, e := range entries {
		if !strings.HasSuffix(e.Name(), repairExt) {
			continue
		}

		if err := fsys.Remove(e.Name()); err != nil && !os.IsNotExist(err) {
			return errors.Wrapf(err, "remove %q", e.Name())
		}
	}

	return nil
}

func writeNew(fsys vfs.FS, name string, data []byte) error {
	f, err := fsys.OpenFile(name, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o600)
	if err != nil {
		return err
	}

	_, err = f.Write(data)
	if err == nil {
		err = f.Sync()
	}

	if cerr := f.Close(); err == nil {
		err = cerr
	}

	return err
}

// ensure io.Writer is satisfied.
var _ io.Writer = (*SegmentWriter)(nil)

// CreateFS is [Create] over an already-rooted filesystem, for tests outside this package that inject
// faults through a fake one.
func CreateFS(fsys vfs.FS, maxBytes int) (*SegmentWriter, error) { return createFS(fsys, maxBytes) }
