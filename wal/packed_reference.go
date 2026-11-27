package wal

import (
	"encoding/binary"
	"io"
	"math"
	"math/bits"

	"github.com/go-faster/errors"

	"github.com/oteldb/storage/signal"
)

// MaxPackedReferences bounds the number of samples sharing a checksum.
const MaxPackedReferences = 1024

type packedTimestampMode uint8

const (
	packedSame packedTimestampMode = iota
	packedDelta
	packedRaw
)

const packedReferenceBytes = seriesIDLen + 8

// WritePackedReferences writes one local block of singleton samples whose descriptors are durable.
// sample must return the same value for an index on repeated calls. False means no frame was written.
func (wr *Writer) WritePackedReferences(ids []signal.SeriesID, sample func(int) (int64, float64)) (bool, error) {
	if len(ids) < 2 || len(ids) > MaxPackedReferences {
		return false, nil
	}

	mode, base, tsBytes, legacyBytes := packedReferenceLayout(len(ids), sample)
	bodyLen := 2 + unsignedVarintBytes(uint64(len(ids))) + len(ids)*packedReferenceBytes + tsBytes
	frameLen := unsignedVarintBytes(uint64(bodyLen)) + bodyLen + 4
	if frameLen >= legacyBytes {
		return false, nil
	}

	wr.frame = binary.AppendUvarint(wr.frame[:0], uint64(bodyLen))
	wr.frame = append(wr.frame, recordPackedReferences, byte(mode))
	wr.frame = binary.AppendUvarint(wr.frame, uint64(len(ids)))
	for i, id := range ids {
		_, value := sample(i)
		wr.frame = binary.BigEndian.AppendUint64(id.AppendBinary(wr.frame), math.Float64bits(value))
	}
	wr.frame = appendPackedTimestamps(wr.frame, mode, base, len(ids), sample)
	wr.frame = finishFrame(wr.frame, 0)
	n, err := wr.w.Write(wr.frame)
	if err != nil {
		return false, errors.Wrap(err, "write packed references")
	}
	if n != len(wr.frame) {
		return false, errors.Wrap(io.ErrShortWrite, "write packed references")
	}
	return true, nil
}

func packedReferenceLayout(n int, sample func(int) (int64, float64)) (mode packedTimestampMode, base int64, bytes, legacy int) {
	base, _ = sample(0)
	// A signed timestamp fits in ten bytes, so the old singleton frame length always fits in one.
	const legacyFixedBytes = 1 + 1 + seriesIDLen + 1 + 8 + 4
	legacy = legacyFixedBytes + signedVarintBytes(base)
	deltaBytes, allSame := 8, true
	for i := 1; i < n; i++ {
		ts, _ := sample(i)
		delta := ts - base // Modulo 2^64 arithmetic also preserves extreme timestamp pairs.
		allSame = allSame && delta == 0
		deltaBytes += signedVarintBytes(delta)
		legacy += legacyFixedBytes + signedVarintBytes(ts)
	}
	switch {
	case allSame:
		return packedSame, base, 8, legacy
	case deltaBytes < n*8:
		return packedDelta, base, deltaBytes, legacy
	default:
		return packedRaw, base, n * 8, legacy
	}
}

func appendPackedTimestamps(dst []byte, mode packedTimestampMode, base int64, n int, sample func(int) (int64, float64)) []byte {
	switch mode {
	case packedSame:
		return binary.BigEndian.AppendUint64(dst, uint64(base))
	case packedDelta:
		dst = binary.BigEndian.AppendUint64(dst, uint64(base))
		for i := 1; i < n; i++ {
			ts, _ := sample(i)
			dst = binary.AppendVarint(dst, ts-base)
		}
	case packedRaw:
		for i := range n {
			ts, _ := sample(i)
			dst = binary.BigEndian.AppendUint64(dst, uint64(ts))
		}
	}
	return dst
}

func unsignedVarintBytes(v uint64) int { return (max(1, bits.Len64(v)) + 6) / 7 }

func signedVarintBytes(v int64) int { return unsignedVarintBytes(uint64(v)<<1 ^ uint64(v>>63)) }

func dispatchPackedReferences(payload []byte, h Handlers) error {
	if h.OnPackedReference == nil {
		return errors.Wrap(ErrCorrupt, "unsupported packed references")
	}
	if len(payload) == 0 {
		return decodeError{errors.Wrap(ErrCorrupt, "missing packed reference mode")}
	}
	count, consumed := binary.Uvarint(payload[1:])
	if consumed <= 0 || count < 2 || count > MaxPackedReferences {
		return decodeError{errors.Wrap(ErrCorrupt, "invalid packed reference count")}
	}
	rows := payload[1+consumed:]
	split := int(count) * packedReferenceBytes
	if len(rows) < split {
		return decodeError{errors.Wrap(ErrCorrupt, "truncated packed reference values")}
	}

	// Salvage may skip a malformed frame. Validate all timestamp bytes before any callback.
	var timestamps [MaxPackedReferences]int64
	if err := decodePackedTimestamps(rows[split:], packedTimestampMode(payload[0]), timestamps[:int(count)]); err != nil {
		return decodeError{err}
	}
	for i := range int(count) {
		row := rows[i*packedReferenceBytes : (i+1)*packedReferenceBytes]
		id := signal.SeriesID{
			Hi: binary.BigEndian.Uint64(row),
			Lo: binary.BigEndian.Uint64(row[8:]),
		}
		value := math.Float64frombits(binary.BigEndian.Uint64(row[seriesIDLen:]))
		if err := h.OnPackedReference(id, timestamps[i], value); err != nil {
			return err
		}
	}
	return nil
}

func decodePackedTimestamps(data []byte, mode packedTimestampMode, timestamps []int64) error {
	switch mode {
	case packedSame:
		if len(data) != 8 {
			return errors.Wrap(ErrCorrupt, "invalid shared timestamp length")
		}
		base := int64(binary.BigEndian.Uint64(data))
		for i := range timestamps {
			timestamps[i] = base
		}
	case packedDelta:
		if len(data) < 8 {
			return errors.Wrap(ErrCorrupt, "missing timestamp base")
		}
		base := int64(binary.BigEndian.Uint64(data))
		data = data[8:]
		timestamps[0] = base
		for i := 1; i < len(timestamps); i++ {
			delta, n := binary.Varint(data)
			if n <= 0 {
				return errors.Wrap(ErrCorrupt, "invalid timestamp delta")
			}
			data = data[n:]
			timestamps[i] = base + delta
		}
		if len(data) != 0 {
			return errors.Wrap(ErrCorrupt, "trailing timestamp deltas")
		}
	case packedRaw:
		if len(data) != len(timestamps)*8 {
			return errors.Wrap(ErrCorrupt, "invalid raw timestamp length")
		}
		for i := range timestamps {
			timestamps[i] = int64(binary.BigEndian.Uint64(data[i*8:]))
		}
	default:
		return errors.Wrap(ErrCorrupt, "invalid packed reference mode")
	}
	return nil
}
