package etcdtest_test

import (
	"context"
	"net/url"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	clientv3 "go.etcd.io/etcd/client/v3"

	"github.com/oteldb/storage/cluster/etcd/etcdtest"
)

func TestStart(t *testing.T) {
	t.Parallel()

	endpoint := etcdtest.Start(t)
	u, err := url.Parse(endpoint)
	require.NoError(t, err)
	assert.NotEqual(t, "0", u.Port())

	c, err := clientv3.New(clientv3.Config{Endpoints: []string{endpoint}, DialTimeout: 5 * time.Second})
	require.NoError(t, err)
	t.Cleanup(func() { _ = c.Close() })

	_, err = c.Put(t.Context(), "k", "v")
	require.NoError(t, err)
}

func TestServerRestartKeepsEndpointAndData(t *testing.T) {
	t.Parallel()

	s := etcdtest.NewServer(t)
	endpoint := s.Endpoint()
	c := s.Dial()

	_, err := c.Put(t.Context(), "k", "v")
	require.NoError(t, err)

	s.Stop()
	s.Stop()
	s.Restart()
	assert.Equal(t, endpoint, s.Endpoint())

	require.EventuallyWithT(t, func(ct *assert.CollectT) {
		ctx, cancel := context.WithTimeout(t.Context(), time.Second)
		defer cancel()

		resp, err := c.Get(ctx, "k")
		if !assert.NoError(ct, err) || !assert.Len(ct, resp.Kvs, 1) {
			return
		}

		assert.Equal(ct, "v", string(resp.Kvs[0].GetValue()))
	}, 30*time.Second, 10*time.Millisecond, "the client from before the restart reconnects to the same data")
}
