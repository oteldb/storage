// Package sidecar streams checked records through bounded scratch.
package sidecar

import (
	"context"
	"encoding/binary"
	"hash/crc32"
	"io"
	"sync"

	"github.com/go-faster/errors"

	"github.com/oteldb/storage/backend"
)

// ScratchBytes bounds the encoder buffer shared by sequential sidecar writes.
const ScratchBytes = 32 << 10

var buffers = sync.Pool{New: func() any { return new([ScratchBytes]byte) }}
var checksums = crc32.MakeTable(crc32.Castagnoli)

// Write emits header, records and a big-endian CRC without a whole-object encoding buffer.
func Write(ctx context.Context, b backend.Backend, key string, header []byte, count, maxEntry int, entry func([]byte, int) []byte) error {
	if count < 0 || maxEntry <= 0 || maxEntry > ScratchBytes-4 || len(header) > ScratchBytes-4 {
		return errors.New("sidecar: invalid record geometry")
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	w, err := backend.CreateObjectDeferred(ctx, b, key)
	if err != nil {
		return err
	}
	defer w.Abort()
	storage := buffers.Get().(*[ScratchBytes]byte)
	defer buffers.Put(storage)
	buf := storage[:0]
	buf = append(buf, header...)
	var checksum uint32
	flush := func() error {
		if err := ctx.Err(); err != nil {
			return err
		}
		checksum = crc32.Update(checksum, checksums, buf)
		n, err := w.Write(buf)
		if err != nil {
			return err
		}
		if n != len(buf) {
			return io.ErrShortWrite
		}
		buf = buf[:0]
		return nil
	}
	for i := range count {
		if cap(buf)-len(buf) < maxEntry+4 {
			if err := flush(); err != nil {
				return err
			}
		}
		start := len(buf)
		buf = entry(buf, i)
		if len(buf)-start > maxEntry {
			return errors.New("sidecar: record exceeds bound")
		}
	}
	checksum = crc32.Update(checksum, checksums, buf)
	buf = binary.BigEndian.AppendUint32(buf, checksum)
	if err := ctx.Err(); err != nil {
		return err
	}
	n, err := w.Write(buf)
	if err != nil {
		return err
	}
	if n != len(buf) {
		return io.ErrShortWrite
	}
	return w.Commit(ctx)
}
