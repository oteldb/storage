package wal

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"math/rand/v2"
	"strconv"
	"strings"
	"testing"

	"github.com/go-faster/errors"

	"github.com/oteldb/storage/internal/vfs/faultfs"
)

var errInjectedFault = errors.New("injected filesystem fault")

// script hands out a fuzz input a byte at a time, and zeros once it runs out, so every input is a
// complete schedule.
type script struct {
	b []byte
	i int
}

func (s *script) next() int {
	if s.i >= len(s.b) {
		return 0
	}

	s.i++

	return int(s.b[s.i-1])
}

// crashRecord is what the schedule knows about one record it tried to log.
type crashRecord struct {
	seg     int  // the segment it landed in, for checkpoints
	acked   bool // its write returned nil
	durable bool // a sync the writer reported as successful covers it
	healed  bool // its write failed, and a later successful Sync restored the segment without it
	gone    bool // a successful checkpoint removed its segment
	// droppable is a record whose segment a checkpoint that failed or crashed may have removed.
	droppable bool
	lost      bool // a recovery already came back without it, so it must never reappear
}

type crashKind int

const (
	crashKill    crashKind = iota // process death: the page cache survives
	crashNone                     // power cut keeping only what was synced
	crashAll                      // power cut after the disk had written everything out
	crashPartial                  // power cut keeping a random part of the unsynced state
)

func (k crashKind) String() string {
	return [...]string{"kill", "crash", "crash-keep-all", "crash-partial"}[k]
}

var crashOps = []faultfs.Op{
	faultfs.OpOpen, faultfs.OpCreate, faultfs.OpRead, faultfs.OpWrite, faultfs.OpSync, faultfs.OpSyncDir,
	faultfs.OpRename, faultfs.OpLink, faultfs.OpRemove, faultfs.OpMkdir, faultfs.OpStat, faultfs.OpReadDir,
}

// faultOps weights the injected fault toward the calls a durability bug hides behind.
var faultOps = []faultfs.Op{
	faultfs.OpWrite, faultfs.OpWrite, faultfs.OpWrite, faultfs.OpWrite, faultfs.OpWrite,
	faultfs.OpSync, faultfs.OpSync, faultfs.OpSync, faultfs.OpSyncDir, faultfs.OpSyncDir,
	faultfs.OpRename, faultfs.OpCreate, faultfs.OpRead, faultfs.OpRemove, faultfs.OpReadDir, faultfs.OpStat,
}

// crashRun drives a [SegmentWriter] on faultfs through a schedule; see [FuzzCrashDurability].
type crashRun struct {
	t        *testing.T
	s        *script
	maxBytes int
	epoch    uint64
	records  []crashRecord
	pending  []int // acked since the last Sync or Seal call
	failed   []int // failed while the writer did not sync each write, not yet covered by a Sync

	fsys    *faultfs.FS
	crashed *faultfs.FS // the snapshot the armed crash point took, nil until it fires
	kind    crashKind
	pct     int
	seed    uint64
	calls   int
	crashAt int

	lossy   bool // a crash since the last check may drop acked records it did not sync
	partial bool // a crash so far kept part of the unsynced state, so holes are legal

	log []string
}

func (r *crashRun) logf(format string, args ...any) {
	r.log = append(r.log, fmt.Sprintf(format, args...))
}

func (r *crashRun) fatalf(format string, args ...any) {
	r.t.Helper()
	r.t.Fatalf("%s\nschedule:\n  %s", fmt.Sprintf(format, args...), strings.Join(r.log, "\n  "))
}

// arm installs a cycle's crash point and fault on r.fsys, numbering every filesystem call.
func (r *crashRun) arm() {
	r.calls, r.crashed = 0, nil
	r.kind = crashKind(r.s.next() % 4)
	r.crashAt = 1 + r.s.next() + r.s.next()%128
	r.pct, r.seed = 1+r.s.next()%99, uint64(r.s.next())

	r.logf("arm %s at call %d (unsynced %d%%, seed %d)", r.kind, r.crashAt, r.pct, r.seed)

	fsys := r.fsys

	for _, op := range crashOps {
		fsys.Add(faultfs.Rule{
			Op: op,
			Match: func(faultfs.Call) bool {
				r.calls++

				return r.calls == r.crashAt
			},
			Before: func(c faultfs.Call) {
				r.logf("crash point reached at %s %s", c.Op, c.Name)
				r.crashed = r.snapshot(fsys)
			},
			Times: 1,
		})
	}

	for range 1 + r.s.next()%2 {
		if r.s.next()%4 == 0 {
			continue
		}

		op := faultOps[r.s.next()%len(faultOps)]
		at := 1 + r.s.next()%96
		times := 1 + r.s.next()%3
		short := r.s.next() * 4

		r.logf("fault %s from call %d x%d, short %d", op, at, times, short)

		fsys.Add(faultfs.Rule{
			Op:    op,
			Match: func(faultfs.Call) bool { return r.calls >= at },
			Err:   errInjectedFault,
			Short: short,
			Times: times,
		})
	}
}

func (r *crashRun) snapshot(fsys *faultfs.FS) *faultfs.FS {
	switch r.kind {
	case crashKill:
		return fsys.Kill()
	case crashNone:
		return fsys.Crash()
	case crashAll:
		return fsys.CrashWith(faultfs.CrashConfig{UnsyncedPercent: 100})
	default:
		return fsys.CrashWith(faultfs.CrashConfig{UnsyncedPercent: r.pct, Seed: r.seed})
	}
}

// crash moves the run onto the crashed filesystem: the armed crash point's snapshot, or one taken now.
func (r *crashRun) crash() {
	if r.crashed == nil {
		r.crashed = r.snapshot(r.fsys)
	}

	r.lossy = r.lossy || r.kind == crashNone || r.kind == crashPartial
	r.partial = r.partial || r.kind == crashPartial
	r.logf("crashed (%s)", r.kind)
	r.fsys, r.crashed = r.crashed, nil
}

func crashPayload(id, size int) []byte {
	p := binary.BigEndian.AppendUint32(nil, uint32(id))
	for i := range size {
		p = append(p, byte(id*31+i))
	}

	return p
}

// recover opens a writer on r.fsys and salvage-replays it, retrying once without faults when an
// injected fault fails either, then checks the replay. It returns nil when the crash point fell inside
// the recovery: that recovery crashed, and the next one checks the same promises.
func (r *crashRun) recover() *SegmentWriter {
	r.t.Helper()

	for attempt := 0; ; attempt++ {
		var (
			ids     []int
			damages int
		)

		w, err := createFS(r.fsys, r.maxBytes)
		if err == nil {
			err = replayDirFrom(r.fsys, 0, Handlers{
				OnSide: func(p []byte) error {
					if len(p) < 4 {
						r.fatalf("replayed a %d-byte record that was never written", len(p))
					}

					id := int(binary.BigEndian.Uint32(p))
					if id >= len(r.records) || !bytes.Equal(p, crashPayload(id, len(p)-4)) {
						r.fatalf("replayed a record that was never written intact (id %d, %d bytes)", id, len(p))
					}

					ids = append(ids, id)

					return nil
				},
				OnDamage: func(d Damage) error {
					r.logf("damage in %s at %d (+%d): %v", d.Segment, d.Offset, d.Length, d.Err)
					damages++

					return nil
				},
			})
		}

		if r.crashed != nil {
			r.crash()

			return nil
		}

		if err != nil {
			if attempt > 0 || !errors.Is(err, errInjectedFault) {
				r.fatalf("recovery failed: %+v", err)
			}

			r.logf("recovery failed on the injected fault, retrying without it: %v", err)
			r.fsys.Reset()

			continue
		}

		r.check(ids, damages)

		return w
	}
}

// check holds a recovery's replay to what the writer promised before the crash.
func (r *crashRun) check(ids []int, damages int) {
	r.t.Helper()

	r.logf("replayed %d records, %d damaged regions", len(ids), damages)

	present := make(map[int]bool, len(ids))

	for i, id := range ids {
		if i > 0 && id <= ids[i-1] {
			r.fatalf("record %d replayed after %d: out of order or duplicated", id, ids[i-1])
		}

		present[id] = true
	}

	for id := range r.records {
		rec := r.records[id]

		switch {
		case present[id] && rec.lost:
			r.fatalf("record %d came back after a recovery had lost it", id)
		case present[id] && rec.gone:
			r.fatalf("record %d came back from a checkpointed segment", id)
		case present[id] && rec.healed:
			r.fatalf("record %d came back after a Sync healed its failed write", id)
		case !present[id] && rec.durable && !rec.gone && !rec.droppable:
			r.fatalf("durable record %d was lost", id)
		case !present[id] && rec.acked && !rec.gone && !rec.droppable && !r.lossy:
			r.fatalf("acknowledged record %d was lost without a power cut", id)
		}
	}

	if damages > 0 && !r.partial {
		r.fatalf("%d damaged regions, but no crash kept part of the unsynced state", damages)
	}

	// What this recovery replayed is now the log's durable content, and what it lost stays lost.
	for id := range r.records {
		switch rec := &r.records[id]; {
		case present[id]:
			*rec = crashRecord{acked: true, durable: true}
		case !rec.gone:
			*rec = crashRecord{lost: true}
		}
	}

	r.pending, r.failed, r.lossy = r.pending[:0], r.failed[:0], false
}

// ops runs a cycle's operations until the schedule ends it or the crash point is reached.
func (r *crashRun) ops(w *SegmentWriter, syncing bool) {
	w.SetSync(syncing)
	r.rebase(w)

	for n := 1 + r.s.next()%40; n > 0 && r.crashed == nil; n-- {
		switch r.s.next() % 11 {
		case 0, 1, 2, 3, 4:
			r.write(w, syncing, 1)
		case 10:
			if w = r.restart(w); w == nil {
				return
			}

			w.SetSync(syncing)
		case 6:
			r.write(w, syncing, 2+r.s.next()%4)
		case 7:
			err := w.Sync()
			r.logf("sync: %v", err)
			r.synced(err == nil, true)
		case 8:
			syncing = r.s.next()%2 == 1
			w.SetSync(syncing)
			r.logf("set sync %v", syncing)
		case 9:
			r.seal(w)
		}
	}
}

func (r *crashRun) seal(w *SegmentWriter) {
	r.epoch++
	sealed, err := w.Seal(r.epoch)
	r.logf("seal epoch %d through segment %d: %v", r.epoch, sealed, err)
	r.synced(err == nil, false)

	if r.crashed != nil || err != nil || r.s.next()%2 == 0 {
		return
	}

	err = w.CheckpointThrough(sealed)
	r.logf("checkpoint through segment %d: %v", sealed, err)

	for id := range r.records {
		rec := &r.records[id]
		if rec.seg > sealed || (!rec.acked && !rec.durable) {
			continue
		}

		if r.crashed == nil && err == nil {
			rec.gone = true
		} else {
			rec.droppable = true
		}
	}
}

// rebase files every record already in the log under w's current segment, which any later checkpoint
// covers.
func (r *crashRun) rebase(w *SegmentWriter) {
	for id := range r.records {
		if r.records[id].durable || r.records[id].acked {
			r.records[id].seg = w.Seq()
		}
	}
}

// restart closes w and opens a new writer on the same filesystem, as a clean shutdown and start do. A
// successful Close covers every record acked so far. It returns nil when the crash point is reached.
func (r *crashRun) restart(w *SegmentWriter) *SegmentWriter {
	err := w.Close()
	r.logf("close: %v", err)
	r.synced(err == nil, true)

	// A failed write the closed writer could not heal is repaired by the next one at frame boundaries,
	// which keeps whatever whole frames of its batch had landed.
	r.failed = r.failed[:0]

	for attempt := 0; r.crashed == nil; attempt++ {
		next, err := createFS(r.fsys, r.maxBytes)
		r.logf("reopen: %v", err)

		switch {
		case r.crashed != nil:
		case err == nil:
			r.rebase(next)

			return next
		case attempt > 0 || !errors.Is(err, errInjectedFault):
			r.fatalf("reopen failed: %+v", err)
		default:
			r.fsys.Reset()
		}
	}

	return nil
}

// synced applies a Sync or Seal outcome: success covers every record acked since the previous call.
// Only Sync restores a torn segment, so only it heals the failed writes before it.
func (r *crashRun) synced(ok, heals bool) {
	if r.crashed != nil {
		return
	}

	if ok {
		for _, id := range r.pending {
			r.records[id].durable = true
		}

		if heals {
			for _, id := range r.failed {
				r.records[id].healed = true
			}

			r.failed = r.failed[:0]
		}
	}

	r.pending = r.pending[:0]
}

// write logs n records in a single write: one WriteSide, or WriteFrames for a batch.
func (r *crashRun) write(w *SegmentWriter, syncing bool, n int) {
	first := len(r.records)

	var (
		frames  []byte
		payload []byte
	)

	for i := range n {
		payload = crashPayload(first+i, r.s.next()*(1+r.s.next()%24))
		frames = appendFrame(frames, recordSide, payload)
		r.records = append(r.records, crashRecord{})
	}

	var err error
	if n == 1 {
		err = w.WriteSide(payload)
	} else {
		err = w.WriteFrames(frames)
	}

	r.logf("write %d..%d (%d bytes, sync %v): %v", first, first+n-1, len(frames), syncing, err)

	if r.crashed != nil {
		return
	}

	for id := first; id < first+n; id++ {
		rec := &r.records[id]

		switch {
		case err == nil:
			rec.acked, rec.seg = true, w.Seq()
			if syncing {
				rec.durable = true
			} else {
				r.pending = append(r.pending, id)
			}
		case !syncing:
			// A write that fails without a per-write sync failed before or while landing, so the
			// writer restores its segment without it. A per-write sync that fails leaves it whole.
			r.failed = append(r.failed, id)
		}
	}
}

func runCrashScript(t *testing.T, input []byte) {
	t.Helper()

	s := &script{b: input}
	r := &crashRun{t: t, s: s, maxBytes: 256 + s.next()*64, fsys: faultfs.New()}
	flags := s.next()
	syncing, windows := flags&1 == 1, flags&2 == 2
	cycles := 1 + s.next()%4

	r.logf("max segment %d bytes, sync %v, windows %v, %d cycles", r.maxBytes, syncing, windows, cycles)

	if windows {
		r.fsys.LockOpenFiles()
	}

	for range cycles {
		r.arm()

		if w := r.recover(); w != nil {
			r.ops(w, syncing)
			r.crash()
		}
	}

	// The last crash's filesystem carries no rules: this recovery must open, and its writer must work.
	w := r.recover()
	if w == nil {
		r.fatalf("a recovery with no crash point armed crashed")
	}

	sentinel := len(r.records)
	r.records = append(r.records, crashRecord{})

	if err := w.WriteSide(crashPayload(sentinel, 16)); err != nil {
		r.fatalf("the recovered writer refuses writes: %+v", err)
	}

	if err := w.Close(); err != nil {
		r.fatalf("the recovered writer fails to close: %+v", err)
	}

	r.records[sentinel] = crashRecord{acked: true, durable: true}
	r.kind = crashNone
	r.crash()
	r.recover()
}

// FuzzCrashDurability runs a schedule of writes, batched writes, syncs, seals and checkpoints against
// a segment writer on faultfs, injecting one filesystem fault and one power cut or process death per
// cycle over several cycles; a crash point can fall inside a recovery too. Every recovery must open
// and salvage-replay the log, and the replay must keep the writer's promises: records in write order,
// intact, none duplicated or invented; every record a successful sync covered; every acknowledged one
// when no power cut intervened; no failed write back after a Sync healed it; nothing back from a
// checkpointed segment or from an earlier recovery that lost it; and damage only once a crash kept part
// of the unsynced state.
func FuzzCrashDurability(f *testing.F) {
	for seed := range uint64(16) {
		f.Add(crashSchedule(seed))
	}

	f.Fuzz(runCrashScript)
}

func crashSchedule(seed uint64) []byte {
	rng := rand.New(rand.NewPCG(seed, 0x6f74656c6462))
	b := make([]byte, 64+rng.IntN(512))

	for i := range b {
		b[i] = byte(rng.Uint32())
	}

	return b
}

// TestCrashDurabilitySchedules runs a fixed sweep of pseudo-random schedules, so CI covers far more
// than the fuzz seed corpus.
func TestCrashDurabilitySchedules(t *testing.T) {
	t.Parallel()

	n := 400
	if testing.Short() {
		n = 50
	}

	for seed := range uint64(n) {
		t.Run(strconv.FormatUint(seed, 10), func(t *testing.T) {
			t.Parallel()
			runCrashScript(t, crashSchedule(seed))
		})
	}
}
