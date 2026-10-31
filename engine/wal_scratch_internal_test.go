package engine

import (
	"bytes"
	"fmt"
	"strconv"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/signal"
	"github.com/oteldb/storage/wal"
)

func TestSharedWALIdentityScratch(t *testing.T) {
	t.Parallel()
	for _, everySeries := range []bool{false, true} {
		t.Run(strconv.FormatBool(everySeries), func(t *testing.T) {
			t.Parallel()
			h := newHead()
			b := newWALBatch()
			var expected bytes.Buffer
			writer := wal.NewWriter(&expected)
			for i, count := range []int{12, 0, 4, 16} {
				attrs := make([]signal.KeyValue, count)
				for j := range attrs {
					attrs[j] = signal.KeyValue{Key: fmt.Appendf(nil, "key-%02d", j), Value: signal.IntValue(int64(i*100 + j))}
				}
				s := signal.Series{Attributes: signal.NewAttributes(attrs...)}
				h.registerSeries(s)
				id := s.Hash()
				ref, ok := h.series.Ref(id)
				require.True(t, ok)
				b.order = append(b.order, id)
				b.accs = append(b.accs, walSeriesAcc{isNew: !everySeries, knownRef: true, identityRef: ref,
					ts: []int64{int64(i)}, values: []float64{float64(i)}})
				require.NoError(t, writer.WriteSeries(id, s))
				require.NoError(t, writer.WriteSamples(id, []int64{int64(i)}, []float64{float64(i)}))
			}
			require.NoError(t, b.encode(h.series.View(), everySeries))
			assert.Equal(t, expected.Bytes(), b.frames.Bytes(), "reused identities preserve exact WAL bytes")
			require.GreaterOrEqual(t, cap(b.attrs), 16)
			b.reset()
			for _, attr := range b.attrs[:cap(b.attrs)] {
				assert.Equal(t, signal.KeyValue{}, attr)
			}
		})
	}
}

func TestWALIdentityScratchBound(t *testing.T) {
	t.Parallel()
	for _, capacity := range []int{16, maxWALIdentityAttributes, maxWALIdentityAttributes + 1} {
		t.Run(strconv.Itoa(capacity), func(t *testing.T) {
			t.Parallel()
			b := newWALBatch()
			b.attrs = make(signal.Attributes, 0, capacity)
			b.attrs[:cap(b.attrs)][capacity-1] = signal.KeyValue{Key: []byte("borrowed")}
			b.reset()
			if capacity > maxWALIdentityAttributes {
				assert.Nil(t, b.attrs)
			} else {
				assert.Equal(t, capacity, cap(b.attrs))
				assert.Empty(t, b.attrs)
				assert.Equal(t, signal.KeyValue{}, b.attrs[:cap(b.attrs)][capacity-1])
			}
		})
	}
}

func TestWALStageReleasesBorrowedReferences(t *testing.T) {
	t.Parallel()
	for _, fail := range []bool{false, true} {
		name := "success"
		if fail {
			name = "failure"
		}
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			e, fsys := newFaultWALEngine(t, 0)
			if fail {
				failNextWALWrite(fsys)
			}
			s := stagedTestSeries("borrowed")
			id := s.Hash()
			_, err := e.AppendBatch([]signal.SeriesID{id}, []int64{10}, []float64{1}, nil,
				func(int) signal.Series { return s }, AppendLimits{})
			if fail {
				require.ErrorIs(t, err, errInjectedWAL)
			} else {
				require.NoError(t, err)
			}
			require.Len(t, e.walB.accs, 1)
			acc := &e.walB.accs[0]
			assert.Equal(t, signal.Series{}, acc.series)
			assert.Nil(t, acc.buffer)
			ts, values, weights := cap(acc.ts), cap(acc.values), cap(acc.sf)
			_, err = e.Append(s, 20, 2)
			require.NoError(t, err)
			assert.Equal(t, signal.Series{}, acc.series)
			assert.Nil(t, acc.buffer)
			assert.Equal(t, ts, cap(acc.ts))
			assert.Equal(t, values, cap(acc.values))
			assert.Equal(t, weights, cap(acc.sf))
		})
	}
}
