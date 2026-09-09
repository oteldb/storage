package cluster

import (
	"bytes"
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/readbudget"
)

func TestAcceptsZSTD(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		header string
		want   bool
	}{
		{"", false},
		{"zstd", true},
		{"ZSTD", true},
		{"gzip, zstd", true},
		{"gzip, deflate", false},
		{"zstd;q=1.0", true},
		{"zstd;q=0", false},
		{"zstd;q=0.0", false},
		{"gzip;q=0, zstd;q=0.5", true},
		{"zstdx", false},
		{"*", false}, // a wildcard is not a promise this peer can inflate zstd
	} {
		h := http.Header{}
		h.Set(acceptEncodingHeader, tc.header)
		assert.Equal(t, tc.want, acceptsZSTD(h), "Accept-Encoding: %q", tc.header)
	}
}

// The point of charging the inflated size: a compressed body's Content-Length is the wire size, and
// honoring it would loosen every budget by the compression ratio.
func TestReadBudgetedBodyChargesDecompressedLength(t *testing.T) {
	t.Parallel()

	raw := bytes.Repeat([]byte("log line, highly compressible\n"), 4000)
	enc := wireCompressor.Compress(nil, raw)
	require.Less(t, len(enc), len(raw)/4, "the payload must actually compress for this test to mean anything")

	b := readbudget.New(int64(len(raw)) + 100)
	ctx := readbudget.With(context.Background(), b)

	data, release, err := readBudgetedBody(ctx, bytes.NewReader(enc), int64(len(enc)), true)
	require.NoError(t, err)
	assert.Equal(t, raw, data)
	assert.Equal(t, int64(100), b.Remaining(), "the resident bytes are charged, not the wire bytes")

	release()
	assert.Equal(t, int64(len(raw))+100, b.Remaining())

	// The same body against a budget that fits the wire size but not the payload must be refused,
	// which is precisely the case an uncompressed-length check would have waved through.
	small := readbudget.New(int64(len(enc)) * 2)
	_, _, err = readBudgetedBody(readbudget.With(context.Background(), small), bytes.NewReader(enc), int64(len(enc)), true)
	require.ErrorIs(t, err, readbudget.ErrExceeded)
}

// A peer sending a small body that inflates without bound must be cut off during inflation, not
// after the allocation.
func TestReadBudgetedBodyRefusesDecompressionBomb(t *testing.T) {
	t.Parallel()

	const budget = 1 << 20

	bomb := wireCompressor.Compress(nil, make([]byte, 64<<20))
	require.Less(t, len(bomb), budget/4, "the wire bytes must fit the budget, or the cap is not what refused it")

	src := &countingReader{r: bytes.NewReader(bomb)}

	ctx := readbudget.With(context.Background(), readbudget.New(budget))
	_, _, err := readBudgetedBody(ctx, src, int64(len(bomb)), true)
	require.ErrorIs(t, err, readbudget.ErrExceeded)
	assert.Less(t, src.n, len(bomb), "the frame is abandoned mid-stream, not inflated whole")
}

func TestReadBudgetedBodyRejectsCorruptFrame(t *testing.T) {
	t.Parallel()

	_, _, err := readBudgetedBody(context.Background(), bytes.NewReader([]byte("not a zstd frame")), 16, true)
	require.Error(t, err)
}

// A new node must understand a plaintext answer from a node too old to compress, and must not send
// a compressed one to a node too old to inflate it.
func TestFetchInteropWithOldPeer(t *testing.T) {
	t.Parallel()

	payload := bytes.Repeat([]byte("compressible payload\n"), 500)

	// New client, old server: the old server ignores Accept-Encoding and answers in plaintext.
	old := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write(payload)
	}))
	t.Cleanup(old.Close)

	resp, err := http.Get(old.URL) //nolint:noctx // httptest, no cancellation to model
	require.NoError(t, err)

	defer func() { _ = resp.Body.Close() }()
	assert.False(t, zstdEncoded(resp.Header), "an old peer declares no content coding")

	body, release, err := readBudgetedBody(context.Background(), resp.Body, resp.ContentLength, zstdEncoded(resp.Header))
	require.NoError(t, err)
	assert.Equal(t, payload, body)
	release()

	// Old client, new server: no Accept-Encoding, so the answer stays plaintext.
	fresh := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write(compressResponse(w, r.Header, payload))
	}))
	t.Cleanup(fresh.Close)

	req, err := http.NewRequestWithContext(context.Background(), http.MethodGet, fresh.URL, http.NoBody)
	require.NoError(t, err)
	req.Header.Set(acceptEncodingHeader, "identity") // what an old node's transport sends, minus zstd

	resp2, err := http.DefaultClient.Do(req)
	require.NoError(t, err)

	defer func() { _ = resp2.Body.Close() }()
	assert.Empty(t, resp2.Header.Get(contentEncodingHeader), "a peer that cannot inflate is answered in plaintext")

	got, err := io.ReadAll(resp2.Body)
	require.NoError(t, err)
	assert.Equal(t, payload, got)
}

func FuzzWireBody(f *testing.F) {
	f.Add([]byte(""))
	f.Add([]byte("batches"))
	f.Add(bytes.Repeat([]byte("aaaa"), 1000))

	f.Fuzz(func(t *testing.T, payload []byte) {
		enc := wireCompressor.Compress(nil, payload)

		got, release, err := readBudgetedBody(context.Background(), bytes.NewReader(enc), int64(len(enc)), true)
		if err != nil {
			t.Fatal(err)
		}

		release()

		if !bytes.Equal(payload, got) {
			t.Fatalf("round-trip mismatch: %d in, %d out", len(payload), len(got))
		}

		// A corrupt frame must fail rather than panic.
		_, _, _ = readBudgetedBody(context.Background(), bytes.NewReader(payload), int64(len(payload)), true)
	})
}

type countingReader struct {
	r io.Reader
	n int
}

func (c *countingReader) Read(p []byte) (int, error) {
	n, err := c.r.Read(p)
	c.n += n

	return n, err
}
