package recordengine_test

import (
	"context"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/go-faster/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/backend"
	"github.com/oteldb/storage/backend/bucketindex"
	"github.com/oteldb/storage/backend/faultbackend"
	"github.com/oteldb/storage/recordengine"
	"github.com/oteldb/storage/wal"
)

func TestWriteAndReadDuringPublication(t *testing.T) {
	t.Parallel()
	be := faultbackend.Wrap(backend.Memory())
	e := newEngine(t, be)
	_, err := e.AppendBatch(mkBatch("api", rrec{ts: 1, body: "first"}), recordengine.AppendLimits{})
	require.NoError(t, err)
	gate := faultbackend.NewGate()
	release := sync.OnceFunc(gate.Release)
	t.Cleanup(release)
	be.Add(gate.Rule(faultbackend.CompareAndSwap, func(op faultbackend.Op) bool {
		return strings.HasSuffix(op.Key, bucketindex.Object)
	}))
	flushed := make(chan error, 1)
	go func() { flushed <- e.Flush(t.Context()) }()
	gate.Await(t)
	assert.Zero(t, e.PartCount(), "candidate parts stay private during publication")
	written := make(chan error, 1)
	go func() {
		_, err := e.AppendBatch(mkBatch("api", rrec{ts: 2, body: "second"}), recordengine.AppendLimits{})
		written <- err
	}()
	select {
	case err := <-written:
		require.NoError(t, err)
	case <-time.After(10 * time.Second):
		t.Fatal("write blocked by publication")
	}
	assertReadableDuringWAL(t, e, 2)
	release()
	require.NoError(t, <-flushed)
	assert.Equal(t, []string{"first", "second"}, streamBodies(t, e))
}

type responseLostBackend struct {
	backend.Backend

	fail     atomic.Bool
	readFail atomic.Bool
}

func (b *responseLostBackend) ReadVersioned(ctx context.Context, key string) ([]byte, backend.Version, error) {
	if b.readFail.Swap(false) {
		return nil, backend.VersionAbsent, errors.New("confirmation unavailable")
	}
	return b.Backend.ReadVersioned(ctx, key)
}

func (b *responseLostBackend) CompareAndSwap(ctx context.Context, key string, expected backend.Version, data []byte) (backend.Version, bool, error) {
	v, ok, err := b.Backend.CompareAndSwap(ctx, key, expected, data)
	if err == nil && ok && b.fail.Swap(false) {
		return backend.VersionAbsent, false, errors.New("commit response lost")
	}
	return v, ok, err
}

func TestConfirmedPublicationReplaysRecordsExactlyOnce(t *testing.T) {
	t.Parallel()
	be := &responseLostBackend{Backend: backend.Memory()}
	dir := t.TempDir()
	w, err := wal.Create(dir, 0)
	require.NoError(t, err)
	t.Cleanup(func() { _ = w.Close() })
	cfg := recordengine.Config{Schema: testSchema, Backend: be, Prefix: "records", WAL: w}
	e := recordengine.New(cfg)
	_, err = e.AppendBatch(mkBatch("api", rrec{ts: 1, body: "first"}), recordengine.AppendLimits{})
	require.NoError(t, err)
	be.fail.Store(true)
	require.NoError(t, e.Flush(t.Context()))
	_, err = e.AppendBatch(mkBatch("api", rrec{ts: 2, body: "second"}), recordengine.AppendLimits{})
	require.NoError(t, err)
	require.NoError(t, e.CloseWAL())
	cfg.WAL = nil
	reopened := recordengine.New(cfg)
	require.NoError(t, reopened.LoadParts(t.Context()))
	require.NoError(t, reopened.Replay(t.Context(), dir))
	assert.Equal(t, []string{"first", "second"}, streamBodies(t, reopened))
}

func TestReloadDuringFlushPreservesActiveWALEpoch(t *testing.T) {
	t.Parallel()
	be := faultbackend.Wrap(backend.Memory())
	dir := t.TempDir()
	w, err := wal.Create(dir, 0)
	require.NoError(t, err)
	t.Cleanup(func() { _ = w.Close() })
	cfg := recordengine.Config{Schema: testSchema, Backend: be, Prefix: "records", WAL: w}
	e := recordengine.New(cfg)
	_, err = e.AppendBatch(mkBatch("api", rrec{ts: 1, body: "first"}), recordengine.AppendLimits{})
	require.NoError(t, err)
	gate := faultbackend.NewGate()
	release := sync.OnceFunc(gate.Release)
	t.Cleanup(release)
	be.Add(gate.Rule(faultbackend.Write, nil))
	flushed := make(chan error, 1)
	go func() { flushed <- e.Flush(t.Context()) }()
	gate.Await(t)
	require.NoError(t, e.LoadParts(t.Context()))
	_, err = e.AppendBatch(mkBatch("api", rrec{ts: 2, body: "second"}), recordengine.AppendLimits{})
	require.NoError(t, err)
	release()
	require.NoError(t, <-flushed)
	require.NoError(t, e.CloseWAL())
	cfg.WAL = nil
	reopened := recordengine.New(cfg)
	require.NoError(t, reopened.LoadParts(t.Context()))
	require.NoError(t, reopened.Replay(t.Context(), dir))
	assert.Equal(t, []string{"first", "second"}, streamBodies(t, reopened))
}

func TestUnconfirmedPublicationRetainsWALAndOutputs(t *testing.T) {
	t.Parallel()
	for _, retry := range []bool{false, true} {
		t.Run(map[bool]string{false: "crash", true: "retry"}[retry], func(t *testing.T) {
			t.Parallel()
			be := &responseLostBackend{Backend: backend.Memory()}
			dir := t.TempDir()
			w, err := wal.Create(dir, 0)
			require.NoError(t, err)
			t.Cleanup(func() { _ = w.Close() })
			cfg := recordengine.Config{Schema: testSchema, Backend: be, Prefix: "records", WAL: w}
			e := recordengine.New(cfg)
			_, err = e.AppendBatch(mkBatch("api", rrec{ts: 1, body: "first"}), recordengine.AppendLimits{})
			require.NoError(t, err)
			be.fail.Store(true)
			be.readFail.Store(true)
			require.Error(t, e.Flush(t.Context()))
			segments, _, _, _ := e.WALState()
			assert.Positive(t, segments, "unconfirmed publication must not checkpoint")
			want := []string{"first"}
			if retry {
				_, err = e.AppendBatch(mkBatch("api", rrec{ts: 2, body: "second"}), recordengine.AppendLimits{})
				require.NoError(t, err)
				require.NoError(t, e.Flush(t.Context()))
				want = append(want, "second")
			}
			require.NoError(t, e.CloseWAL())
			cfg.WAL = nil
			reopened := recordengine.New(cfg)
			require.NoError(t, reopened.LoadParts(t.Context()))
			require.NoError(t, reopened.Replay(t.Context(), dir))
			assert.Equal(t, want, streamBodies(t, reopened))
		})
	}
}

func TestReloadLoadsIdentityBeforeRepairPublication(t *testing.T) {
	t.Parallel()
	be := faultbackend.Wrap(backend.Memory())
	writer := newEngine(t, be)
	for i, service := range []string{"api", "web", "lost"} {
		_, err := writer.AppendBatch(mkBatch(service, rrec{ts: int64(i + 1), body: service}), recordengine.AppendLimits{})
		require.NoError(t, err)
		require.NoError(t, writer.Flush(t.Context()))
	}
	for _, p := range writer.Parts() {
		if p.MaxTime == 3 {
			require.NoError(t, be.Delete(t.Context(), p.ID+"/manifest"))
		}
	}
	dir := t.TempDir()
	w, err := wal.Create(dir, 0)
	require.NoError(t, err)
	t.Cleanup(func() { _ = w.Close() })
	cfg := recordengine.Config{Schema: testSchema, Backend: be, Prefix: enginePrefix, WAL: w}
	reader := recordengine.New(cfg)
	gate := faultbackend.NewGate()
	release := sync.OnceFunc(gate.Release)
	t.Cleanup(release)
	be.Add(gate.Rule(faultbackend.CompareAndSwap, func(op faultbackend.Op) bool {
		return strings.HasSuffix(op.Key, bucketindex.Object)
	}))
	done := make(chan error, 1)
	go func() { done <- reader.LoadParts(t.Context()) }()
	gate.Await(t)
	assertReadableDuringWAL(t, reader, 2)
	_, err = reader.AppendBatch(mkBatch("api", rrec{ts: 10, body: "after-load"}), recordengine.AppendLimits{})
	require.NoError(t, err)
	assertReadableDuringWAL(t, reader, 3)
	release()
	require.NoError(t, <-done)
	require.NoError(t, reader.CloseWAL())
	cfg.WAL = nil
	restored := recordengine.New(cfg)
	require.NoError(t, restored.LoadParts(t.Context()))
	require.NoError(t, restored.Replay(t.Context(), dir))
	assertReadableDuringWAL(t, restored, 3)
}

func TestRetriedFlushCommitsEveryCoveredWALEpoch(t *testing.T) {
	t.Parallel()
	for _, failure := range []string{"part-write", "canceled-admission"} {
		t.Run(failure, func(t *testing.T) {
			t.Parallel()
			be := faultbackend.Wrap(backend.Memory())
			dir := t.TempDir()
			w, err := wal.Create(dir, 0)
			require.NoError(t, err)
			t.Cleanup(func() { _ = w.Close() })
			cfg := recordengine.Config{Schema: testSchema, Backend: be, Prefix: "records", WAL: w}
			if failure == "canceled-admission" {
				var calls atomic.Int32
				cfg.FlushAdmission = func(context.Context, int64) (func(), error) {
					if calls.Add(1) == 1 {
						return nil, context.Canceled
					}
					return func() {}, nil
				}
			} else {
				be.Add(faultbackend.Rule{Kind: faultbackend.Write, Err: errors.New("part write failed"), Times: 1})
			}
			e := recordengine.New(cfg)
			_, err = e.AppendBatch(mkBatch("api", rrec{ts: 1, body: "first"}), recordengine.AppendLimits{})
			require.NoError(t, err)
			require.Error(t, e.Flush(t.Context()))
			be.Reset()
			_, err = e.AppendBatch(mkBatch("api", rrec{ts: 2, body: "second"}), recordengine.AppendLimits{})
			require.NoError(t, err)

			gate := faultbackend.NewGate()
			rule := gate.Rule(faultbackend.CompareAndSwap, nil)
			hold := rule.Before
			rule.Before = nil
			rule.After = func(op faultbackend.Op, _ []byte) { hold(op) }
			be.Add(rule)
			done := make(chan error, 1)
			go func() { done <- e.Flush(t.Context()) }()
			defer func() {
				gate.Release()
				require.NoError(t, <-done)
			}()
			gate.Await(t)

			// The index committed, but the old WAL segments have not been checkpointed.
			cfg.WAL = nil
			cfg.FlushAdmission = nil
			restored := recordengine.New(cfg)
			require.NoError(t, restored.LoadParts(t.Context()))
			require.NoError(t, restored.Replay(t.Context(), dir))
			assert.Equal(t, []string{"first", "second"}, streamBodies(t, restored))
		})
	}
}
