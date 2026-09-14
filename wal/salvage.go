package wal

import (
	"encoding/binary"
	"io"
	"os"
	"slices"
	"strings"

	"github.com/go-faster/errors"

	"github.com/oteldb/storage/internal/vfs"
)

const (
	damagedExt = ".damaged"

	// maxDamagedCopies bounds the damaged segments kept for inspection, so a disk that keeps
	// corrupting the log cannot fill itself with copies of it.
	maxDamagedCopies = 4
)

// Damage is a region of a segment that replay could not read and skipped.
type Damage struct {
	Segment string // the segment's file name
	Offset  int    // where the unreadable region starts
	Length  int    // how many bytes replay skipped
	Err     error  // why the region is unreadable
	// Kept names the copy of the segment kept for inspection, empty when none could be written.
	Kept string
}

// decodeError marks a CRC-valid frame whose payload does not decode: damage, not a handler's
// failure.
type decodeError struct{ error }

func (e decodeError) Unwrap() error { return e.error }

// walk dispatches the frames of one segment (to h, when non-nil) and returns the offset where its
// readable log ends: len(data), or the start of a torn tail no valid frame follows.
//
// With damaged nil walk is strict: a frame failing its CRC or one that does not decode is an error,
// and a torn frame ends the walk. Otherwise every unreadable region — a bad frame, a hole, an
// undecodable payload — is handed to damaged and skipped, and the walk resumes at the next valid
// frame ([resync]); damaged returning an error stops the walk with it.
func walk(data []byte, h *Handlers, damaged func(off, n int, err error) error) (int, error) {
	budget := scanCRCBudget
	off := 0

	for off < len(data) {
		typ, payload, n, err := readFrame(data[off:])
		if err == nil {
			undecodable := func(derr error) error { return damaged(off, n, derr) }
			if err := dispatchFrame(typ, payload, h, undecodable, damaged != nil); err != nil {
				return off, err
			}

			off += n

			continue
		}

		torn := errors.Is(err, io.EOF)
		if damaged == nil {
			if torn {
				return off, nil
			}

			return off, err
		}

		next := resync(data, off+1, &budget)

		switch {
		case next < 0 && torn:
			return off, nil
		case next < 0:
			next = len(data)
		case torn:
			err = errors.Wrap(ErrCorrupt, "hole")
		}

		if err := damaged(off, next-off, err); err != nil {
			return off, err
		}

		off = next
	}

	return off, nil
}

// dispatchFrame hands a CRC-valid frame to h, if any. A payload that does not decode is reported to
// undecodable when salvaging, and an error otherwise; a handler's own error is always returned.
func dispatchFrame(typ byte, payload []byte, h *Handlers, undecodable func(error) error, salvaging bool) error {
	if h == nil {
		return nil
	}

	err := dispatch(typ, payload, *h)
	if err == nil {
		return nil
	}

	var decode decodeError
	if salvaging && errors.As(err, &decode) {
		return undecodable(err)
	}

	return err
}

// resync returns the offset of the first frame at or after from that replay can trust, or -1. A
// trusted frame is CRC-valid and followed by another valid frame or by the end of data: a chance
// CRC32C match inside damaged bytes is 2^-32 per offset, and two in a row is negligible. The cost is
// a lone frame between a hole and a torn tail, which is dropped with the tail.
//
// The scan is byte-aligned, because a hole destroys frame alignment. budget is the CRC work left for
// the segment (see [scanCRCBudget]); once spent, nothing more is trusted.
func resync(data []byte, from int, budget *int) int {
	for off := from; off < len(data); off++ {
		bodyLen, n := binary.Uvarint(data[off:])
		if n <= 0 || bodyLen == 0 {
			continue
		}

		avail := len(data) - off - n
		if avail < 4 || bodyLen > uint64(avail-4) {
			continue
		}

		if *budget -= n + int(bodyLen); *budget < 0 {
			return -1
		}

		_, _, size, err := readFrame(data[off:])
		if err != nil {
			continue
		}

		if end := off + size; end == len(data) {
			return off
		} else if _, _, _, err := readFrame(data[end:]); err == nil {
			return off
		}
	}

	return -1
}

// keepDamaged copies a damaged segment next to the log under a name replay never reads, and removes
// the oldest copies past [maxDamagedCopies]. It is best effort — the log is salvaged either way — and
// returns the copy's name, or "" when none was written.
func keepDamaged(fsys vfs.FS, name string, data []byte) string {
	kept := name + damagedExt

	switch _, err := fsys.Stat(kept); {
	case os.IsNotExist(err):
		if err := writeNew(fsys, kept, data); err != nil {
			_ = fsys.Remove(kept)

			return ""
		}
	case err != nil:
		return ""
	}

	entries, err := fsys.ReadDir(".")
	if err != nil {
		return kept
	}

	var copies []string

	for _, e := range entries {
		if strings.HasSuffix(e.Name(), damagedExt) {
			copies = append(copies, e.Name())
		}
	}

	slices.Sort(copies) // segment names sort by sequence, so the oldest copies come first
	for _, old := range copies[:max(len(copies)-maxDamagedCopies, 0)] {
		if fsys.Remove(old) == nil && old == kept {
			kept = ""
		}
	}

	return kept
}

// salvageSegment replays one segment of a directory, reporting each damaged region to h.OnDamage and
// skipping it. final is whether the segment is the directory's last, the only one a torn tail may end.
func salvageSegment(fsys vfs.FS, name string, data []byte, final bool, h *Handlers) error {
	kept, tried := "", false

	damaged := func(off, n int, err error) error {
		if !tried {
			kept, tried = keepDamaged(fsys, name, data), true
		}

		return h.OnDamage(Damage{Segment: name, Offset: off, Length: n, Err: err, Kept: kept})
	}

	end, err := walk(data, h, damaged)
	if err != nil {
		return err
	}

	if end < len(data) && !final {
		return damaged(end, len(data)-end, errors.Wrap(ErrCorrupt, "torn record in a non-final segment"))
	}

	return nil
}
