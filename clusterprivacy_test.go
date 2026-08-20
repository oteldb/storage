package storage

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"go.uber.org/zap/zaptest/observer"

	"github.com/oteldb/storage/backend"
	"github.com/oteldb/storage/backend/file"
	"github.com/oteldb/storage/cluster"
	"github.com/oteldb/storage/cluster/etcd"
)

// sharedStore is a backend every node can read: it declines the [backend.NodeLocal] capability the
// memory backend it delegates to would otherwise promote.
type sharedStore struct{ backend.Backend }

func (sharedStore) IsNodeLocal() bool { return false }

func TestNodeLocalBackendUnshared(t *testing.T) {
	t.Parallel()

	local, err := file.New(t.TempDir())
	require.NoError(t, err)

	shared := sharedStore{backend.Memory()}

	for _, tt := range []struct {
		name string
		opts Options
		want bool
	}{
		{"single node", Options{Backend: local}, false},
		{"file backend undeclared", Options{Backend: local, Cluster: &cluster.Config{}}, true},
		{"memory backend undeclared", Options{Backend: backend.Memory(), Cluster: &cluster.Config{}}, true},
		{"cached local backend undeclared", Options{Backend: backend.Cached(local, 1<<20), Cluster: &cluster.Config{}}, true},
		{"declared private", Options{Backend: local, Cluster: &cluster.Config{PrivateBackend: true}}, false},
		{"shared store", Options{Backend: shared, Cluster: &cluster.Config{}}, false},
	} {
		assert.Equal(t, tt.want, tt.opts.nodeLocalBackendUnshared(), tt.name)
	}
}

// TestClusterRefusesNodeLocalBackendUnshared covers issue #369, as narrowed by #408: a cluster node
// whose backend is private to it while cluster.Config.PrivateBackend is unset replicates no flushed
// part, and the resulting hole reads as genuine absence. It used to open with a warning, on the
// premise that a file backend over a shared mount was a legitimate shared store landing in the same
// check. It is not one — CompareAndSwap is process-local over a directory tree, so two nodes lose
// index commits to each other in silence — which leaves the configuration with no valid reading.
//
//nolint:paralleltest // owns an embedded etcd; runs serially
func TestClusterRefusesNodeLocalBackendUnshared(t *testing.T) {
	endpoint := startEtcd(t)

	_, err := Open(context.Background(), Options{}, WithBackend(backend.Memory()),
		WithCluster(&cluster.Config{
			Etcd: []string{endpoint},
			Self: etcd.Member{ID: "node-a", Addr: "127.0.0.1:0"},
			RF:   1,
		}))

	require.Error(t, err)
	assert.Contains(t, err.Error(), "PrivateBackend", "the error names the field that fixes it")
	assert.Contains(t, err.Error(), "not be replicated", "the error names what silently does not happen")
}

// TestClusterQuietWhenPrivateBackendDeclared is the other half: a node that declares its backend
// private mirrors parts, so there is nothing to warn about.
//
//nolint:paralleltest // owns an embedded etcd; runs serially
func TestClusterQuietWhenPrivateBackendDeclared(t *testing.T) {
	endpoint := startEtcd(t)

	core, logs := observer.New(zap.WarnLevel)

	s, err := Open(context.Background(), Options{}, WithBackend(backend.Memory()), WithLogger(zap.New(core)),
		WithCluster(&cluster.Config{
			Etcd:           []string{endpoint},
			Self:           etcd.Member{ID: "node-a", Addr: "127.0.0.1:0"},
			RF:             1,
			PrivateBackend: true,
		}))
	require.NoError(t, err)

	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		_ = s.Close(ctx)
	})

	for _, e := range logs.All() {
		assert.NotContains(t, strings.ToLower(e.Message), "node-private")
	}

	cs := s.Inspect().Cluster
	require.NotNil(t, cs)
	assert.True(t, cs.PrivateBackend)
}
