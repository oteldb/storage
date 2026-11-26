package sidecar

import (
	"context"
	"encoding/binary"
	"hash/crc32"
	"io"

	"github.com/go-faster/errors"

	"github.com/oteldb/storage/backend"
)

// OrdinalFooterBytes includes the manifest binding, record count, and checksum.
const OrdinalFooterBytes = 32 + 8 + 4

var ErrOrdinal = errors.New("invalid ordinal sidecar")

// OrdinalWriter streams records whose order is supplied by the part's series index.
type OrdinalWriter struct {
	w       backend.ObjectWriter
	storage *[ScratchBytes]byte
	buf     []byte
	crc     uint32
}

func OpenOrdinalWriter(ctx context.Context, b backend.Backend, key string, magic uint32) (*OrdinalWriter, error) {
	w, err := backend.CreateObjectDeferred(ctx, b, key)
	if err != nil {
		return nil, err
	}
	storage := buffers.Get().(*[ScratchBytes]byte)
	return &OrdinalWriter{w: w, storage: storage, buf: binary.BigEndian.AppendUint32(storage[:0], magic)}, nil
}

func (w *OrdinalWriter) Append(ctx context.Context, record []byte) error {
	if len(record) > ScratchBytes {
		return ErrOrdinal
	}
	if len(record) > cap(w.buf)-len(w.buf) {
		if err := w.flush(ctx); err != nil {
			return err
		}
	}
	w.buf = append(w.buf, record...)
	return nil
}

func (w *OrdinalWriter) Commit(ctx context.Context, binding [32]byte, count int) error {
	var footer [OrdinalFooterBytes]byte
	copy(footer[:], binding[:])
	binary.BigEndian.PutUint64(footer[32:], uint64(count))
	if err := w.Append(ctx, footer[:40]); err != nil {
		return err
	}
	binary.BigEndian.PutUint32(footer[40:], crc32.Update(w.crc, checksums, w.buf))
	if err := w.Append(ctx, footer[40:]); err != nil {
		return err
	}
	if err := w.flush(ctx); err != nil {
		return err
	}
	return w.w.Commit(ctx)
}

func (w *OrdinalWriter) Abort() {
	w.w.Abort()
	buffers.Put(w.storage)
}

func (w *OrdinalWriter) flush(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	n, err := w.w.Write(w.buf)
	if err != nil {
		return err
	}
	if n != len(w.buf) {
		return io.ErrShortWrite
	}
	w.crc = crc32.Update(w.crc, checksums, w.buf)
	w.buf = w.buf[:0]
	return nil
}

func CheckOrdinalFooter(footer []byte, binding [32]byte, count int, checksum uint32) error {
	if len(footer) != OrdinalFooterBytes || [32]byte(footer[:32]) != binding ||
		binary.BigEndian.Uint64(footer[32:]) != uint64(count) ||
		binary.BigEndian.Uint32(footer[40:]) != crc32.Update(checksum, checksums, footer[:40]) {
		return ErrOrdinal
	}
	return nil
}

func OrdinalBody(raw []byte, binding [32]byte, count int) ([]byte, error) {
	if len(raw) < 4+OrdinalFooterBytes {
		return nil, ErrOrdinal
	}
	end := len(raw) - OrdinalFooterBytes
	if err := CheckOrdinalFooter(raw[end:], binding, count, crc32.Checksum(raw[:end], checksums)); err != nil {
		return nil, err
	}
	return raw[4:end], nil
}
