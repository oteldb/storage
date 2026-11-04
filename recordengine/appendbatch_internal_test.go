package recordengine

import (
	"bytes"
	"path"
	"strconv"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/internal/vfs/faultfs"
	"github.com/oteldb/storage/signal"
	"github.com/oteldb/storage/wal"
)

func appendRowsReference(h *head, b *Batch, limits AppendLimits, window int64) AppendResult {
	h.ensureStream(b.Stream, b.Identity, 0)
	var res AppendResult
	for i, ts := range b.Ts {
		if limits.EnforceMinTimestamp && ts < limits.MinTimestamp {
			res.RejectedRetention++
			continue
		}
		r := rec{ts: ts, ints: make([]int64, len(b.Ints)), bytes: make([][]byte, len(b.Bytes))}
		for k := range b.Ints {
			r.ints[k] = b.Ints[k][i]
		}
		for k := range b.Bytes {
			r.bytes[k] = b.Bytes[k][i]
		}
		res = res.with(h.appendRecord(b.Stream, r, window, limits.MaxInFlightBytes))
	}
	return res
}

func requireSameRecords(t *testing.T, want, got *head) {
	t.Helper()
	require.Equal(t, want.bytes, got.bytes)
	require.Equal(t, want.newest, got.newest)
	require.Equal(t, want.streamNewest, got.streamNewest)
	require.Len(t, got.records, len(want.records))
	for id, expected := range want.records {
		actual := got.records[id]
		require.NotNil(t, actual)
		assert.Equal(t, expected.ts, actual.ts)
		assert.Equal(t, expected.ints, actual.ints)
		assert.Equal(t, expected.tsMin, actual.tsMin)
		assert.Equal(t, expected.tsMax, actual.tsMax)
		assert.Equal(t, expected.distinctAttrKeys(), actual.distinctAttrKeys())
		for k := range expected.bytes {
			assert.Equal(t, expected.bytes[k].views(nil), actual.bytes[k].views(nil))
		}
	}
}

func testBatchColumns(schema *Schema, times []int64) *Batch {
	b := payloadBatch(0)
	b.Ts = times
	b.Ints = make([][]int64, schema.numInts())
	b.Bytes = make([][][]byte, schema.numBytes())
	for k := range b.Ints {
		for i := range times {
			b.Ints[k] = append(b.Ints[k], int64(k*100+i))
		}
	}
	for k := range b.Bytes {
		for i := range times {
			b.Bytes[k] = append(b.Bytes[k], bytes.Repeat([]byte{byte(i + k)}, (i+k)%7))
		}
	}
	if k, ok := schema.attrsByteCol(); ok {
		for i := range times {
			b.Bytes[k][i] = signal.NewAttributes(signal.KeyValue{
				Key: []byte("key" + strconv.Itoa(i%3)), Value: signal.IntValue(int64(i)),
			}).AppendHashInput(nil)
		}
	}
	return b
}

func TestAppendBatchColumnsMatchRows(t *testing.T) {
	t.Parallel()
	schemas := []*Schema{
		NewSchema(),
		headTestSchema,
		NewSchema(Column{Name: "a", Kind: KindInt64}, Column{Name: "b", Kind: KindInt64},
			Column{Name: "body", Kind: KindBytes}, Column{Name: "attrs", Kind: KindBytes, Bloom: BloomAttrs}),
	}
	for n, schema := range schemas {
		for _, logged := range []bool{false, true} {
			for _, capBytes := range []int64{0, 30, 150} {
				t.Run(strconv.Itoa(n)+"/wal="+strconv.FormatBool(logged)+"/cap="+strconv.FormatInt(capBytes, 10), func(t *testing.T) {
					t.Parallel()
					fsys := faultfs.New()
					cfg := Config{Schema: schema, OOOWindow: 10}
					if logged {
						w, err := wal.CreateFS(fsys, 0)
						require.NoError(t, err)
						t.Cleanup(func() { _ = w.Close() })
						cfg.WAL = w
					}
					e := New(cfg)
					want := newHead(schema)
					e.head.detachedBytes, want.detachedBytes = 11, 11
					limits := AppendLimits{EnforceMinTimestamp: true, MinTimestamp: -5, MaxInFlightBytes: capBytes}
					for _, times := range [][]int64{{-5, -6, 100, 50, 101, 100, 110, 105}, {109, 0, 120, 112}} {
						batch := testBatchColumns(schema, times)
						wantRes := appendRowsReference(want, batch, limits, cfg.OOOWindow)
						got, err := e.AppendBatch(batch, limits)
						require.NoError(t, err)
						require.Equal(t, wantRes, got)
						for _, col := range batch.Bytes {
							for _, cell := range col {
								clear(cell)
							}
						}
						requireSameRecords(t, want, e.head)
					}
					if logged {
						replayed := New(Config{Schema: schema})
						entries, err := fsys.ReadDir(".")
						require.NoError(t, err)
						for _, entry := range entries {
							if path.Ext(entry.Name()) != ".wal" {
								continue
							}
							data, err := fsys.ReadFile(entry.Name())
							require.NoError(t, err)
							require.NoError(t, replayed.ApplyReplicated(data))
						}
						requireSameRecords(t, want, replayed.head)
					}
				})
			}
		}
	}
}

func FuzzAppendBatchColumnsMatchRows(f *testing.F) {
	f.Add([]byte{100, 40, 101, 0, 105, 100}, uint16(60), uint8(10))
	f.Add([]byte{0, 0, 10, 30, 12, 45}, uint16(0), uint8(0))
	f.Fuzz(func(t *testing.T, script []byte, maxBytes uint16, window uint8) {
		if len(script) > 1000 {
			t.Skip()
		}
		e := New(Config{Schema: headTestSchema, OOOWindow: int64(window)})
		want := newHead(headTestSchema)
		times := make([]int64, len(script))
		for i, v := range script {
			times[i] = int64(v) - 10
		}
		batch := testBatchColumns(headTestSchema, times)
		limits := AppendLimits{EnforceMinTimestamp: true, MinTimestamp: 0, MaxInFlightBytes: int64(maxBytes)}
		wantRes := appendRowsReference(want, batch, limits, int64(window))
		got, err := e.AppendBatch(batch, limits)
		require.NoError(t, err)
		require.Equal(t, wantRes, got)
		if len(script) == 0 {
			return // Retention admission skips registration for an empty batch.
		}
		for _, ts := range times {
			if ts >= 0 {
				requireSameRecords(t, want, e.head)
				return
			}
		}
	})
}

func TestAppendBatchFirstRecordsFailure(t *testing.T) {
	t.Parallel()
	fsys := faultfs.New()
	w, err := wal.CreateFS(fsys, 0)
	require.NoError(t, err)
	t.Cleanup(func() { _ = w.Close() })
	e := New(Config{Schema: headTestSchema, WAL: w, OOOWindow: 10})
	batch := testBatchColumns(headTestSchema, []int64{100, 50, 105})
	materialize := batch.Identity
	calls := 0
	batch.Identity = func() signal.Series {
		calls++
		return materialize()
	}
	writes := 0
	fsys.Add(faultfs.Rule{
		Op: faultfs.OpWrite,
		Match: func(faultfs.Call) bool {
			writes++
			return writes == 2 // Identity succeeds; the first records frame fails.
		},
		Err: assert.AnError,
	})
	res, err := e.AppendBatch(batch, AppendLimits{})
	require.ErrorIs(t, err, assert.AnError)
	assert.Equal(t, AppendResult{}, res)
	assert.Zero(t, e.HeadBytes())
	assert.Empty(t, e.head.records[batch.Stream].ts)
	assert.Empty(t, e.head.streamNewest)
	assert.Equal(t, 1, calls)

	res, err = e.AppendBatch(testBatchColumns(headTestSchema, []int64{90}), AppendLimits{})
	require.NoError(t, err)
	assert.Equal(t, AppendResult{Accepted: 1}, res)
	res, err = e.AppendBatch(batch, AppendLimits{})
	require.NoError(t, err)
	assert.Equal(t, AppendResult{Accepted: 2, RejectedOOO: 1}, res)
	assert.Equal(t, 1, calls)
}
