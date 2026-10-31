package pdataconv

import (
	"encoding/binary"
	"math"
	"slices"
	"strings"

	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/pmetric"

	"github.com/oteldb/storage/signal"
	"github.com/oteldb/storage/signal/metric"
)

type mapEntry struct {
	key   string
	value pcommon.Value
}

type metricsHash struct {
	entries  []mapEntry
	buf      []byte
	prefix   int
	reserved []byte
	offsets  [6]int
}

var metricKeys = [...]string{"__kind__", "__monotonic__", "__name__", "__temporality__", "__unit__"}

func appendHashString(dst []byte, s string) []byte {
	dst = binary.AppendUvarint(dst, uint64(len(s)))
	return append(dst, s...)
}

func (h *metricsHash) sorted(m pcommon.Map) []mapEntry {
	start := len(h.entries)
	for k, v := range m.All() {
		h.entries = append(h.entries, mapEntry{key: k, value: v})
	}
	entries := h.entries[start:]
	slices.SortStableFunc(entries, func(a, b mapEntry) int { return strings.Compare(a.key, b.key) })
	return entries
}

func (h *metricsHash) appendMap(dst []byte, m pcommon.Map) []byte {
	start := len(h.entries)
	entries := h.sorted(m)
	dst = binary.AppendUvarint(dst, uint64(len(entries)))
	for _, entry := range entries {
		dst = appendHashString(dst, entry.key)
		dst = h.appendValue(dst, entry.value)
	}
	clear(h.entries[start:])
	h.entries = h.entries[:start]
	return dst
}

func (h *metricsHash) appendValue(dst []byte, v pcommon.Value) []byte {
	switch v.Type() {
	case pcommon.ValueTypeStr:
		dst = append(dst, byte(signal.KindStr))
		return appendHashString(dst, v.Str())
	case pcommon.ValueTypeBool:
		dst = append(dst, byte(signal.KindBool))
		var n uint64
		if v.Bool() {
			n = 1
		}
		return binary.BigEndian.AppendUint64(dst, n)
	case pcommon.ValueTypeInt:
		dst = append(dst, byte(signal.KindInt))
		return binary.BigEndian.AppendUint64(dst, uint64(v.Int()))
	case pcommon.ValueTypeDouble:
		dst = append(dst, byte(signal.KindDouble))
		return binary.BigEndian.AppendUint64(dst, math.Float64bits(v.Double()))
	case pcommon.ValueTypeBytes:
		dst = append(dst, byte(signal.KindBytes))
		data := v.Bytes()
		dst = binary.AppendUvarint(dst, uint64(data.Len()))
		for i := range data.Len() {
			dst = append(dst, data.At(i))
		}
		return dst
	case pcommon.ValueTypeSlice:
		dst = append(dst, byte(signal.KindSlice))
		values := v.Slice()
		dst = binary.AppendUvarint(dst, uint64(values.Len()))
		for i := range values.Len() {
			dst = h.appendValue(dst, values.At(i))
		}
		return dst
	case pcommon.ValueTypeMap:
		dst = append(dst, byte(signal.KindMap))
		return h.appendMap(dst, v.Map())
	default:
		return append(dst, byte(signal.KindEmpty))
	}
}

func (h *metricsHash) setGroup(r signal.Resource, s signal.Scope) {
	h.buf = s.AppendHashInput(r.AppendHashInput(h.buf[:0]))
	h.prefix = len(h.buf)
}

func (h *metricsHash) setMetric(m pmetric.Metric) {
	kind, temporality, monotonic := metricMetadata(m)
	h.reserved = h.reserved[:0]
	for i, key := range metricKeys {
		h.offsets[i] = len(h.reserved)
		h.reserved = appendHashString(h.reserved, key)
		switch i {
		case 0:
			h.reserved = append(h.reserved, byte(signal.KindInt))
			h.reserved = binary.BigEndian.AppendUint64(h.reserved, uint64(kind))
		case 1:
			h.reserved = append(h.reserved, byte(signal.KindBool))
			var n uint64
			if monotonic {
				n = 1
			}
			h.reserved = binary.BigEndian.AppendUint64(h.reserved, n)
		case 2:
			h.reserved = append(h.reserved, byte(signal.KindStr))
			h.reserved = appendHashString(h.reserved, m.Name())
		case 3:
			h.reserved = append(h.reserved, byte(signal.KindInt))
			h.reserved = binary.BigEndian.AppendUint64(h.reserved, uint64(temporality))
		case 4:
			h.reserved = append(h.reserved, byte(signal.KindStr))
			h.reserved = appendHashString(h.reserved, m.Unit())
		}
	}
	h.offsets[len(metricKeys)] = len(h.reserved)
}

func (h *metricsHash) id(m pcommon.Map) signal.SeriesID {
	entries := h.sorted(m)
	dst := binary.AppendUvarint(h.buf[:h.prefix], uint64(len(entries)+len(metricKeys)))
	reserved := 0
	for _, entry := range entries {
		for reserved < len(metricKeys) && metricKeys[reserved] < entry.key {
			dst = append(dst, h.reserved[h.offsets[reserved]:h.offsets[reserved+1]]...)
			reserved++
		}
		dst = appendHashString(dst, entry.key)
		dst = h.appendValue(dst, entry.value)
	}
	dst = append(dst, h.reserved[h.offsets[reserved]:]...)
	clear(h.entries)
	h.entries = h.entries[:0]
	h.buf = dst
	return signal.HashBytes(dst)
}

func metricMetadata(m pmetric.Metric) (metric.PointKind, metric.Temporality, bool) {
	if m.Type() == pmetric.MetricTypeSum {
		sum := m.Sum()
		return metric.KindSum, temporalityOf(sum.AggregationTemporality()), sum.IsMonotonic()
	}
	return metric.KindGauge, metric.TemporalityUnspecified, false
}
