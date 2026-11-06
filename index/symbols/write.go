package symbols

import (
	"encoding/binary"
	"hash/crc32"
	"io"
)

// WriteTo writes [Table.Encode]'s format without materializing the encoded table.
func (t *Table) WriteTo(dst io.Writer) (written int64, err error) {
	checksum := crc32.New(castagnoli)
	w := io.MultiWriter(dst, checksum)
	write := func(data []byte) error {
		n, err := w.Write(data)
		if n < 0 || n > len(data) {
			return io.ErrShortWrite
		}
		written += int64(n)
		return err
	}
	var scratch [4 + 2*binary.MaxVarintLen64]byte
	header := binary.BigEndian.AppendUint32(scratch[:0], symbolsMagic)
	header = binary.AppendUvarint(header, uint64(symbolsVersion))
	header = binary.AppendUvarint(header, uint64(len(t.syms)))
	if err := write(header); err != nil {
		return written, err
	}
	for _, symbol := range t.syms {
		if err := write(binary.AppendUvarint(scratch[:0], uint64(len(symbol)))); err != nil {
			return written, err
		}
		if err := write(symbol); err != nil {
			return written, err
		}
	}
	trailer := binary.BigEndian.AppendUint32(scratch[:0], checksum.Sum32())
	n, err := dst.Write(trailer)
	if n < 0 || n > len(trailer) {
		return written, io.ErrShortWrite
	}
	written += int64(n)
	if err == nil && n != len(trailer) {
		err = io.ErrShortWrite
	}
	return written, err
}
