package recordengine_test

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/backend"
	"github.com/oteldb/storage/backend/faultbackend"
	"github.com/oteldb/storage/recordengine"
)

func TestKeysConcurrentColdCache(t *testing.T) {
	t.Parallel()
	for _, flushing := range []bool{false, true} {
		name := "head"
		if flushing {
			name = "flushing"
		}
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			ctx := context.Background()
			be := faultbackend.Wrap(backend.Memory())
			e := newEngine(t, be)
			t.Cleanup(func() { require.NoError(t, e.Close(ctx)) })
			rows := make([]rrec, 10000)
			for i := range rows {
				rows[i] = rrec{ts: int64(i + 1), attr: [2]string{"flag", "x"}}
			}
			ingest(t, e, mkScopedBatch("synthetic", rows...))
			var flush sync.WaitGroup
			var flushErr error
			var release func()
			if flushing {
				gate := faultbackend.NewGate()
				be.Add(gate.Rule(faultbackend.Write, func(op faultbackend.Op) bool { return strings.HasPrefix(op.Key, "t/recs/") }))
				release = sync.OnceFunc(gate.Release)
				defer release()
				flush.Go(func() { flushErr = e.Flush(ctx) })
				gate.Await(t)
			}
			start := make(chan struct{})
			var readers sync.WaitGroup
			for range 2 {
				readers.Go(func() {
					<-start
					keys := keyScopes(e.Keys(0, 0))
					assert.Equal(t, recordengine.KeyScopeRecord, keys["flag"])
					assert.Equal(t, recordengine.KeyScopeResource, keys["service.name"])
					assert.Equal(t, recordengine.KeyScopeScope, keys["scope.kind"])
				})
			}
			close(start)
			readers.Wait()
			if release != nil {
				release()
				flush.Wait()
				require.NoError(t, flushErr)
				assert.Equal(t, recordengine.KeyScopeRecord, keyScopes(e.Keys(0, 0))["flag"])
			}
		})
	}
}

func TestKeysConcurrentAppendFlush(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	e := newEngine(t, backend.Memory())
	t.Cleanup(func() { require.NoError(t, e.Close(ctx)) })
	ingest(t, e, mkScopedBatch("synthetic", rrec{ts: 1, attr: [2]string{"seed", "x"}}))
	start := make(chan struct{})
	var wg sync.WaitGroup
	const rounds = 16
	for range 2 {
		wg.Go(func() {
			<-start
			for range rounds {
				assert.Equal(t, recordengine.KeyScopeRecord, keyScopes(e.Keys(0, 0))["seed"])
				assert.Equal(t, recordengine.KeyScopeRecord, keyScopes(e.Keys(1, 1))["seed"])
			}
		})
	}
	wg.Go(func() {
		<-start
		for i := range rounds {
			ingest(t, e, mkScopedBatch("synthetic", rrec{ts: int64(i + 2), attr: [2]string{fmt.Sprintf("key.%02d", i), "x"}}))
			require.NoError(t, e.Flush(ctx))
		}
	})
	close(start)
	wg.Wait()
	keys := keyScopes(e.Keys(0, 0))
	for i := range rounds {
		assert.Equal(t, recordengine.KeyScopeRecord, keys[fmt.Sprintf("key.%02d", i)])
	}
	assert.Equal(t, recordengine.KeyScopeRecord, keys["seed"])
	ingest(t, e, mkScopedBatch("synthetic", rrec{ts: 100, attr: [2]string{"tail", "x"}}))
	assert.Equal(t, recordengine.KeyScopeRecord, keyScopes(e.Keys(0, 0))["tail"])
	ingest(t, e, mkScopedBatch("synthetic", rrec{ts: 101, attr: [2]string{"tail.next", "x"}}))
	keys = keyScopes(e.Keys(0, 0))
	assert.Equal(t, recordengine.KeyScopeRecord, keys["tail"])
	assert.Equal(t, recordengine.KeyScopeRecord, keys["tail.next"], "append invalidates the populated cache")
}
