package engine

import (
	"context"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/backend"
	"github.com/oteldb/storage/backend/file"
	"github.com/oteldb/storage/index/identity"
	"github.com/oteldb/storage/index/series"
	"github.com/oteldb/storage/signal"
)

type identityWriterBackend struct {
	backend.Backend
	backend.ObjectCreator
	backend.DeferredSyncer

	known, streaming       bool
	workspace              int64
	create, abort, commits int
	failWrite, failCommit  bool
}

func (b *identityWriterBackend) StreamsWrites() bool { return b.streaming }
func (b *identityWriterBackend) WriterMemory(string) (int64, bool) {
	return b.workspace, b.known
}
func (b *identityWriterBackend) CreateObjectDeferred(ctx context.Context, key string) (backend.ObjectWriter, error) {
	w, err := b.DeferredSyncer.CreateObjectDeferred(ctx, key)
	if err != nil || !strings.HasSuffix(key, "/identity") {
		return w, err
	}
	b.create++
	return &identityFaultWriter{ObjectWriter: w, owner: b}, nil
}

type identityFaultWriter struct {
	backend.ObjectWriter

	owner *identityWriterBackend
}

func (w *identityFaultWriter) Write(data []byte) (int, error) {
	if w.owner.failWrite {
		return 0, assert.AnError
	}
	return w.ObjectWriter.Write(data)
}

func (w *identityFaultWriter) Commit(ctx context.Context) error {
	w.owner.commits++
	if w.owner.failCommit {
		return assert.AnError
	}
	return w.ObjectWriter.Commit(ctx)
}

func (w *identityFaultWriter) Abort() {
	w.owner.abort++
	w.ObjectWriter.Abort()
}

func TestIdentityWriterCapabilityAndErrors(t *testing.T) {
	t.Parallel()
	for _, tt := range []struct {
		name                  string
		known, streaming      bool
		failWrite, failCommit bool
		stream                bool
	}{
		{name: "priced streaming", known: true, streaming: true, stream: true},
		{name: "unknown workspace", streaming: true},
		{name: "buffered", known: true},
		{name: "write error", known: true, streaming: true, stream: true, failWrite: true},
		{name: "commit error", known: true, streaming: true, stream: true, failCommit: true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			files, err := file.New(t.TempDir())
			require.NoError(t, err)
			b := &identityWriterBackend{Backend: files, ObjectCreator: files, DeferredSyncer: files,
				known: tt.known, streaming: tt.streaming, workspace: 64 << 10,
				failWrite: tt.failWrite, failCommit: tt.failCommit}
			e := New(Config{Backend: b, Prefix: "writer"})
			p, err := newPartStreamWriter(t.Context(), e, compressProfile{}, 0, false, false, nil)
			require.NoError(t, err)
			defer p.abort()
			require.Equal(t, tt.stream, p.streamIdentities)
			if tt.stream {
				require.Equal(t, b.workspace, p.identityWriterBytes)
			} else {
				require.Zero(t, p.identityWriterBytes)
			}
			b.known = !b.known
			index := series.New()
			s := orderSeries(1)
			id := index.Add(s)
			ref, ok := index.Ref(id)
			require.True(t, ok)
			entries := identitySnapshot{view: index.View(), refs: []uint32{ref}}
			err = p.writeIdentity(t.Context(), entries)
			if tt.failWrite || tt.failCommit {
				require.ErrorIs(t, err, assert.AnError)
				require.Equal(t, 1, b.create)
				require.Equal(t, 1, b.abort)
				_, err := files.Read(t.Context(), identityKey(p.prefix))
				require.ErrorIs(t, err, backend.ErrNotExist)
				return
			}
			require.NoError(t, err)
			if tt.stream {
				require.Equal(t, 1, b.create)
				require.Equal(t, 1, b.commits)
			} else {
				require.Zero(t, b.create)
			}
			data, err := files.Read(t.Context(), identityKey(p.prefix))
			require.NoError(t, err)
			require.NoError(t, identity.Decode(data, func(got signal.SeriesID, gotSeries signal.Series) error {
				require.Equal(t, id, got)
				require.True(t, s.Equal(gotSeries))
				return nil
			}))
		})
	}
}

func TestIdentityWriterInvalidatesCachedObject(t *testing.T) {
	t.Parallel()
	files, err := file.New(t.TempDir())
	require.NoError(t, err)
	cached := backend.Cached(files, 1<<20)
	e := New(Config{Backend: cached, Prefix: "cached-writer"})
	p, err := newPartStreamWriter(t.Context(), e, compressProfile{}, 0, false, false, nil)
	require.NoError(t, err)
	defer p.abort()
	require.True(t, p.streamIdentities)
	key := identityKey(p.prefix)
	require.NoError(t, cached.Write(t.Context(), key, []byte("old")))
	index := series.New()
	id := index.Add(orderSeries(1))
	ref, ok := index.Ref(id)
	require.True(t, ok)
	require.NoError(t, p.writeIdentity(t.Context(), identitySnapshot{view: index.View(), refs: []uint32{ref}}))
	data, err := cached.Read(t.Context(), key)
	require.NoError(t, err)
	n, err := identity.Count(data)
	require.NoError(t, err)
	require.Equal(t, 1, n)
}
