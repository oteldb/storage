package bucketindex

import "slices"

// MaxWriters bounds how many per-writer flush watermarks an index carries. A slot has to outlive
// every restart of the node that owns it, and no writer can know when a peer is gone for good, so
// it is kept for a bounded number of *other* writers instead.
//
// A writer whose slot aged out reads a zero watermark on its next recovery and replays its WAL
// from the start: duplicate work bounded by what its last checkpoint left on disk, never loss.
// A prefix is written by the replicas of one shard, so this is far above the real fan-out.
const MaxWriters = 64

// WriterEpoch is one writer's WAL flush watermark, and the generation of the commit that recorded
// it.
//
// The watermark is a *per-node* quantity: it counts that node's own flushes, and indexes the
// segments of that node's own WAL. Two writers over one prefix — every replica of a shard, over a
// shared object store — therefore produce unrelated integers, and a single scalar in a shared index
// means nothing to whichever node did not write it. Keeping one slot per writer is what lets the
// watermark stay in the index at all, which is where it has to be: it advances atomically with the
// part list, so a crash between a flush committing and its WAL being truncated replays exactly the
// records no part holds.
//
// Generation is what the slot is aged out by, since it orders commits across writers.
type WriterEpoch struct {
	Writer     string
	Epoch      uint64
	Generation Generation
}

// WriterEpoch returns writer's WAL flush watermark, or 0 if this index records none for it.
//
// The empty writer is the *anonymous* one — a single-writer engine, and every writer of a
// pre-v4 index — whose slot is [Index.FlushedEpoch].
//
// A named writer with no slot reads 0 rather than falling back to the anonymous scalar. The
// fallback would be right exactly when that scalar happens to be this node's own, and silently
// lossy otherwise (a watermark above the reader's true one skips records it never persisted), and
// nothing in the index says which case holds. Zero costs a replay of whatever the last checkpoint
// left on disk.
func (ix *Index) WriterEpoch(writer string) uint64 {
	if writer == "" {
		return ix.FlushedEpoch
	}

	i, found := slices.BinarySearchFunc(ix.Epochs, WriterEpoch{Writer: writer}, compareWriter)
	if !found {
		return 0
	}

	return ix.Epochs[i].Epoch
}

// SetWriterEpoch records writer's watermark as of gen, replacing any slot it already holds. The
// empty writer sets [Index.FlushedEpoch].
func (ix *Index) SetWriterEpoch(writer string, epoch uint64, gen Generation) {
	if writer == "" {
		ix.FlushedEpoch = epoch

		return
	}

	we := WriterEpoch{Writer: writer, Epoch: epoch, Generation: gen}

	i, found := slices.BinarySearchFunc(ix.Epochs, we, compareWriter)
	if found {
		ix.Epochs[i] = we

		return
	}

	ix.Epochs = slices.Insert(ix.Epochs, i, we)
}

// TrimWriters drops all but the keep newest slots by generation, never dropping the one named by
// self — a writer trimming its own watermark would replay its whole WAL on the next recovery.
func TrimWriters(epochs []WriterEpoch, self string, keep int) []WriterEpoch {
	if len(epochs) <= keep {
		return epochs
	}

	out := slices.Clone(epochs)
	slices.SortFunc(out, func(a, b WriterEpoch) int {
		switch {
		case a.Writer == self:
			return -1
		case b.Writer == self:
			return 1
		default:
			return b.Generation.Compare(a.Generation)
		}
	})

	out = out[:keep]
	slices.SortFunc(out, compareWriter)

	return out
}

func compareWriter(a, b WriterEpoch) int {
	switch {
	case a.Writer < b.Writer:
		return -1
	case a.Writer > b.Writer:
		return 1
	default:
		return 0
	}
}
