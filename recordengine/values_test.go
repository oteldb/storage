package recordengine_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/backend"
	"github.com/oteldb/storage/recordengine"
)

// values drains ColumnValues into strings for readable assertions.
func values(t *testing.T, e *recordengine.Engine, req recordengine.ValuesRequest) []string {
	t.Helper()

	got, err := e.ColumnValues(context.Background(), req)
	require.NoError(t, err)

	out := make([]string, len(got))
	for i, v := range got {
		out[i] = string(v)
	}

	return out
}

func TestColumnValuesHead(t *testing.T) {
	t.Parallel()

	e := newEngine(t, nil)
	ingest(t, e, mkBatch("api",
		rrec{ts: 100, body: "alpha"},
		rrec{ts: 200, body: "beta"},
		rrec{ts: 300, body: "alpha"}, // duplicate ⇒ one value
	))
	ingest(t, e, mkBatch("web", rrec{ts: 150, body: "gamma"}))

	assert.Equal(t, []string{"alpha", "beta", "gamma"}, values(t, e, recordengine.ValuesRequest{Column: "body"}))
}

func TestColumnValuesFlushed(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	e := newEngine(t, backend.Memory())

	ingest(t, e, mkBatch("api", rrec{ts: 100, body: "alpha"}, rrec{ts: 200, body: "beta"}))
	require.NoError(t, e.Flush(ctx))
	require.NotZero(t, e.PartCount(), "flush must produce a part to answer from")

	// The flushed part answers from its dictionary; the head holds only the later record.
	ingest(t, e, mkBatch("api", rrec{ts: 300, body: "gamma"}))

	assert.Equal(t, []string{"alpha", "beta", "gamma"}, values(t, e, recordengine.ValuesRequest{Column: "body"}))
}

func TestColumnValuesWindow(t *testing.T) {
	t.Parallel()

	e := newEngine(t, nil)
	ingest(t, e, mkBatch("api",
		rrec{ts: 100, body: "alpha"},
		rrec{ts: 200, body: "beta"},
		rrec{ts: 300, body: "gamma"},
	))

	assert.Equal(t, []string{"beta"}, values(t, e, recordengine.ValuesRequest{Column: "body", Start: 150, End: 250}))
	assert.Empty(t, values(t, e, recordengine.ValuesRequest{Column: "body", Start: 400, End: 500}))
}

// A flushed part contributes its whole dictionary once its bounds overlap the window, so a windowed
// result is a superset — the contract ColumnValues documents.
func TestColumnValuesFlushedWindowIsSuperset(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	e := newEngine(t, backend.Memory())

	ingest(t, e, mkBatch("api", rrec{ts: 100, body: "alpha"}, rrec{ts: 300, body: "gamma"}))
	require.NoError(t, e.Flush(ctx))

	assert.Equal(t, []string{"alpha", "gamma"},
		values(t, e, recordengine.ValuesRequest{Column: "body", Start: 90, End: 110}))
}

func TestColumnValuesLimit(t *testing.T) {
	t.Parallel()

	e := newEngine(t, nil)
	ingest(t, e, mkBatch("api",
		rrec{ts: 100, body: "c"},
		rrec{ts: 200, body: "a"},
		rrec{ts: 300, body: "b"},
	))

	assert.Equal(t, []string{"a", "b"}, values(t, e, recordengine.ValuesRequest{Column: "body", Limit: 2}))
}

func TestColumnValuesEmptyCellsOmitted(t *testing.T) {
	t.Parallel()

	e := newEngine(t, nil)
	ingest(t, e, mkBatch("api", rrec{ts: 100, body: "alpha"}, rrec{ts: 200}))

	assert.Equal(t, []string{"alpha"}, values(t, e, recordengine.ValuesRequest{Column: "body"}))
}

func TestColumnValuesAttrKey(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	e := newEngine(t, backend.Memory())

	ingest(t, e, mkBatch("api",
		rrec{ts: 100, attr: [2]string{"http.method", "GET"}},
		rrec{ts: 200, attr: [2]string{"http.method", "POST"}},
		rrec{ts: 300, attr: [2]string{"http.status_code", "200"}},
	))
	require.NoError(t, e.Flush(ctx))
	ingest(t, e, mkBatch("api", rrec{ts: 400, attr: [2]string{"http.method", "PUT"}}))

	assert.Equal(t, []string{"GET", "POST", "PUT"},
		values(t, e, recordengine.ValuesRequest{AttrKey: []byte("http.method")}))
	assert.Equal(t, []string{"200"},
		values(t, e, recordengine.ValuesRequest{AttrKey: []byte("http.status_code")}))
	assert.Empty(t, values(t, e, recordengine.ValuesRequest{AttrKey: []byte("absent")}))
}

func TestColumnValuesErrors(t *testing.T) {
	t.Parallel()

	e := newEngine(t, nil)
	ingest(t, e, mkBatch("api", rrec{ts: 100, body: "alpha"}))

	for _, tt := range []struct {
		name string
		req  recordengine.ValuesRequest
		is   error
	}{
		{"unknown column", recordengine.ValuesRequest{Column: "nope"}, recordengine.ErrNoSuchColumn},
		{"numeric column", recordengine.ValuesRequest{Column: "sev"}, recordengine.ErrNoSuchColumn},
		{"no column", recordengine.ValuesRequest{}, recordengine.ErrNoSuchColumn},
		{"both set", recordengine.ValuesRequest{Column: "body", AttrKey: []byte("k")}, nil},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			_, err := e.ColumnValues(context.Background(), tt.req)
			require.Error(t, err)

			if tt.is != nil {
				require.ErrorIs(t, err, tt.is)
			}
		})
	}
}
