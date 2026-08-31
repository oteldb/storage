package cluster

import (
	"bytes"
	"context"
	"io"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/internal/readbudget"
)

func TestBudgetHeaderRoundTrip(t *testing.T) {
	t.Parallel()

	b := readbudget.New(1000)
	require.NoError(t, b.Reserve(400))

	h := http.Header{}
	sendBudget(readbudget.With(context.Background(), b), h)
	assert.Equal(t, "600", h.Get(budgetHeader), "a peer is told what is left, not the original limit")

	got := readbudget.From(recvBudget(context.Background(), h))
	require.NotNil(t, got)
	assert.Equal(t, int64(600), got.Remaining())
}

// Both ends run the same number on purpose. Giving each of N peers a full allowance would make the
// aggregator's real ceiling scale with the node count.
func TestBudgetHeaderIsNotMultipliedByPeers(t *testing.T) {
	t.Parallel()

	b := readbudget.New(1000)
	ctx := readbudget.With(context.Background(), b)

	for range 5 { // five peers, one budget
		h := http.Header{}
		sendBudget(ctx, h)
		assert.Equal(t, "1000", h.Get(budgetHeader))
	}

	require.NoError(t, b.Reserve(1000))
	assert.Zero(t, b.Remaining(), "the peers share one allowance, they do not each get their own")
}

func TestBudgetHeaderAbsentOrMalformed(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	// An unbudgeted caller declares nothing, and a peer must not invent a bound.
	h := http.Header{}
	sendBudget(ctx, h)
	assert.Empty(t, h.Get(budgetHeader))
	assert.Nil(t, readbudget.From(recvBudget(ctx, h)))

	for _, raw := range []string{"", "abc", "-1", "0"} {
		h := http.Header{}
		h.Set(budgetHeader, raw)
		assert.Nil(t, readbudget.From(recvBudget(ctx, h)), "malformed %q is treated as unbounded", raw)
	}
}

// The point of charging here rather than downstream: an oversized response is refused on its
// declared length, before a byte of it is allocated.
func TestReadBudgetedBodyRefusesOnContentLength(t *testing.T) {
	t.Parallel()

	ctx := readbudget.With(context.Background(), readbudget.New(100))

	var read bool
	body := readerFunc(func([]byte) (int, error) { read = true; return 0, io.EOF })

	_, _, err := readBudgetedBody(ctx, body, 500)
	require.ErrorIs(t, err, readbudget.ErrExceeded)
	assert.False(t, read, "the body is never touched")
}

// A peer that lies about (or omits) Content-Length must not get past the bound either.
func TestReadBudgetedBodyCapsUndeclaredLength(t *testing.T) {
	t.Parallel()

	ctx := readbudget.With(context.Background(), readbudget.New(100))

	_, _, err := readBudgetedBody(ctx, bytes.NewReader(make([]byte, 500)), -1)
	require.ErrorIs(t, err, readbudget.ErrExceeded)
}

func TestReadBudgetedBodyChargesAndReleases(t *testing.T) {
	t.Parallel()

	b := readbudget.New(100)
	ctx := readbudget.With(context.Background(), b)

	data, release, err := readBudgetedBody(ctx, bytes.NewReader(make([]byte, 40)), 40)
	require.NoError(t, err)
	assert.Len(t, data, 40)
	assert.Equal(t, int64(60), b.Remaining(), "the wire bytes are held while they are decoded")

	release()
	assert.Equal(t, int64(100), b.Remaining(),
		"and handed back after, so the batches they became are not charged twice")
}

func TestReadBudgetedBodyUnbounded(t *testing.T) {
	t.Parallel()

	data, release, err := readBudgetedBody(context.Background(), bytes.NewReader(make([]byte, 500)), 500)
	require.NoError(t, err)
	assert.Len(t, data, 500, "with no budget the read is unchanged")

	release()
}

type readerFunc func([]byte) (int, error)

func (f readerFunc) Read(p []byte) (int, error) { return f(p) }
