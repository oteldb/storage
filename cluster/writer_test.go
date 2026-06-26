package cluster_test

import (
	"bytes"
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/cluster"
	"github.com/oteldb/storage/cluster/replica"
	"github.com/oteldb/storage/cluster/ring"
	"github.com/oteldb/storage/engine"
	"github.com/oteldb/storage/query/fetch"
	"github.com/oteldb/storage/signal"
	"github.com/oteldb/storage/wal"
)

func TestEncodeDecodeWriteRoundTrip(t *testing.T) {
	t.Parallel()

	payload := cluster.EncodeWrite("tenant-7", []byte{1, 2, 3, 4})
	tenant, walBytes, err := cluster.DecodeWrite(payload)
	require.NoError(t, err)
	assert.Equal(t, "tenant-7", tenant)
	assert.Equal(t, []byte{1, 2, 3, 4}, walBytes)

	_, _, err = cluster.DecodeWrite([]byte{0xff}) // truncated length
	require.Error(t, err)
}

// staticRing is a fixed RingSource for tests.
type staticRing struct{ r *ring.Ring }

func (s staticRing) Ring() *ring.Ring { return s.r }

func mkSeries(job string) (signal.SeriesID, signal.Series) {
	s := signal.Series{Attributes: signal.NewAttributes(
		signal.KeyValue{Key: []byte("job"), Value: signal.StringValue([]byte(job))},
	)}

	return s.Hash(), s
}

func encodeWrite(t *testing.T, tenant string) []byte {
	t.Helper()

	id, s := mkSeries("api")
	var buf bytes.Buffer
	w := wal.NewWriter(&buf)
	require.NoError(t, w.WriteSeries(id, s))
	require.NoError(t, w.WriteSamples(id, []int64{100, 200}, []float64{1, 2}))

	return cluster.EncodeWrite(tenant, buf.Bytes())
}

// engineApply returns a replica ApplyFunc that decodes a write and applies it to eng.
func engineApply(t *testing.T, eng *engine.Engine) replica.ApplyFunc {
	t.Helper()

	return func(_ context.Context, payload []byte) error {
		_, walBytes, err := cluster.DecodeWrite(payload)
		if err != nil {
			return err
		}

		_, err = eng.ApplyReplicated(walBytes)

		return err
	}
}

func fetchOne(t *testing.T, eng *engine.Engine) *fetch.Batch {
	t.Helper()
	it, err := eng.Fetch(context.Background(), fetch.Request{
		Start: 0, End: 1 << 60,
		Matchers: []fetch.Matcher{{Name: []byte("job"), Match: func(v signal.Value) bool {
			return bytes.Equal(v.Str(), []byte("api"))
		}}},
	})
	require.NoError(t, err)
	got, err := fetch.Drain(context.Background(), it)
	require.NoError(t, err)
	require.Len(t, got, 1)

	return got[0]
}

// TestClusteredWriteReplicatesToBothNodes is the end-to-end cluster write path: a write routed
// by the ring is replicated to both owners over the real HTTP transport, and both nodes' local
// engines independently hold the data (so either can serve it).
func TestClusteredWriteReplicatesToBothNodes(t *testing.T) {
	t.Parallel()

	engA := engine.New(engine.Config{})
	engB := engine.New(engine.Config{})

	// Node B serves the replica handler over HTTP.
	rpB := replica.New("", nil, engineApply(t, engB))
	mux := http.NewServeMux()
	mux.Handle(replica.ReplicatePath, rpB.Handler())
	srvB := httptest.NewServer(mux)
	t.Cleanup(srvB.Close)
	addrB := strings.TrimPrefix(srvB.URL, "http://")

	// Node A is local; its replicator applies locally and sends to B over HTTP.
	const selfA = "node-a"
	rpA := replica.New(selfA, replica.NewHTTPTransport(nil), engineApply(t, engA))

	r := ring.New(ring.Node{ID: "A"}, ring.Node{ID: "B"})
	addrOf := func(id string) string {
		if id == "A" {
			return selfA // == rpA.self ⇒ applied locally
		}

		return addrB
	}
	writer := cluster.NewWriter(2, staticRing{r}, addrOf, rpA)

	// Ingest on node A: routed to both owners, quorum 2 ⇒ both must apply.
	require.NoError(t, writer.Write(context.Background(), "default", encodeWrite(t, "default")))

	for name, eng := range map[string]*engine.Engine{"A": engA, "B": engB} {
		b := fetchOne(t, eng)
		assert.Equalf(t, []int64{100, 200}, b.Timestamps, "node %s has the replicated samples", name)
		assert.Equalf(t, []float64{1, 2}, b.Values, "node %s values", name)
	}
}

func TestWriteEmptyRingFails(t *testing.T) {
	t.Parallel()

	w := cluster.NewWriter(2, staticRing{ring.New()}, func(string) string { return "" },
		replica.New("self", nil, func(context.Context, []byte) error { return nil }))
	require.Error(t, w.Write(context.Background(), "default", []byte("p")))
}
