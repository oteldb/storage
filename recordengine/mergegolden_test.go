package recordengine_test

import (
	"context"
	"crypto/sha256"
	"fmt"
	"math/rand/v2"
	"net/http/httptest"
	"slices"
	"strconv"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	awss3 "github.com/aws/aws-sdk-go-v2/service/s3"
	fsserver "github.com/go-faster/fs/server"
	"github.com/go-faster/fs/storagemem"
	"github.com/go-faster/sdk/gold"
	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/backend"
	"github.com/oteldb/storage/backend/file"
	"github.com/oteldb/storage/backend/s3"
	"github.com/oteldb/storage/encoding/chunk"
	"github.com/oteldb/storage/encoding/compress"
	"github.com/oteldb/storage/recordengine"
	"github.com/oteldb/storage/reliability"
	"github.com/oteldb/storage/signal"
)

// wholeObjectBackend offers no ranged or view reads, so every read a merge makes is a whole-object
// Read — the shape of a minimal embedder backend.
type wholeObjectBackend struct{ backend.Backend }

func inProcessS3(t *testing.T, bucket string) *awss3.Client {
	t.Helper()

	store := storagemem.New()
	require.NoError(t, store.CreateBucket(context.Background(), bucket))

	srv := httptest.NewServer(fsserver.NewHandler(store))
	httpClient := srv.Client()

	t.Cleanup(func() {
		httpClient.CloseIdleConnections()
		srv.Close()
	})

	return awss3.New(awss3.Options{
		Region:       "us-east-1",
		BaseEndpoint: aws.String(srv.URL),
		UsePathStyle: true,
		HTTPClient:   httpClient,
		Credentials:  credentials.NewStaticCredentialsProvider("test", "test", ""),
	})
}

// goldenSchema has one column of every form a record merge reads: ints under two codecs and one
// constant-collapsed, a templated body on the shared dictionary, attributes whose granules mix the
// shared dictionary with self-encoded runs of unique values, a near-unique dictionary column, raw
// fixed-width and variable-width columns, a constant bytes column, and all three bloom modes.
var goldenSchema = recordengine.NewSchema(
	recordengine.Column{Name: "sev", Kind: recordengine.KindInt64, Codec: chunk.CodecT64},
	recordengine.Column{Name: "seq", Kind: recordengine.KindInt64, Codec: chunk.CodecDoD},
	recordengine.Column{Name: "flag", Kind: recordengine.KindInt64, Codec: chunk.CodecT64},
	recordengine.Column{Name: "body", Kind: recordengine.KindBytes, Codec: chunk.CodecDict, Bloom: recordengine.BloomFullText},
	recordengine.Column{Name: "attrs", Kind: recordengine.KindBytes, Codec: chunk.CodecDict, Bloom: recordengine.BloomAttrs},
	recordengine.Column{Name: "tid", Kind: recordengine.KindBytes, Codec: chunk.CodecDict, Bloom: recordengine.BloomEquality},
	recordengine.Column{Name: "trace", Kind: recordengine.KindBytes, Codec: chunk.CodecBytesRaw, Bloom: recordengine.BloomEquality},
	recordengine.Column{Name: "note", Kind: recordengine.KindBytes, Codec: chunk.CodecBytesRaw},
	recordengine.Column{Name: "level", Kind: recordengine.KindBytes, Codec: chunk.CodecDict},
)

const goldenStreams = 6

type goldenCorpus struct {
	e   *recordengine.Engine
	rnd *rand.Rand
	ser []signal.Series
}

// flush ingests rows records per stream starting at timestamp from, with every third record sharing
// its predecessor's timestamp, then flushes them as one part (or several, under a part cap).
func (c *goldenCorpus) flush(t *testing.T, from int64, rows int) {
	t.Helper()

	for s := range c.ser {
		series := &c.ser[s]
		b := &recordengine.Batch{
			Stream:   series.Hash(),
			Identity: func() signal.Series { return *series },
			Ints:     make([][]int64, 3),
			Bytes:    make([][][]byte, 6),
		}

		for i := range rows {
			b.Ts = append(b.Ts, from+int64(i/3)*10+int64(s))
			b.Ints[0] = append(b.Ints[0], int64(c.rnd.IntN(24)))
			b.Ints[1] = append(b.Ints[1], from+int64(i)*7+c.rnd.Int64N(5))
			b.Ints[2] = append(b.Ints[2], 7)

			b.Bytes[0] = append(b.Bytes[0], fmt.Appendf(nil,
				"GET /api/v%d/orders/%d status=%d", c.rnd.IntN(3), c.rnd.IntN(40), 200+100*c.rnd.IntN(4)))

			// Unique values push a granule off the shared dictionary: a whole stream of them fills
			// one, and short runs elsewhere leave granules that join despite them.
			attr := "node-" + strconv.Itoa(c.rnd.IntN(8))
			if s == len(c.ser)-1 || (i/1500)%3 == 1 {
				attr = fmt.Sprintf("req-%016x", c.rnd.Uint64())
			}

			b.Bytes[1] = append(b.Bytes[1], signal.NewAttributes(
				signal.KeyValue{Key: []byte("host"), Value: signal.StringValue([]byte(attr))},
			).AppendHashInput(nil))

			b.Bytes[2] = append(b.Bytes[2], fmt.Appendf(nil, "%032x", c.rnd.Uint64()))

			var trace [16]byte
			for j := range trace {
				trace[j] = byte(c.rnd.Uint32())
			}

			b.Bytes[3] = append(b.Bytes[3], trace[:])
			b.Bytes[4] = append(b.Bytes[4], []byte(strings.Repeat("n", c.rnd.IntN(24))))
			b.Bytes[5] = append(b.Bytes[5], []byte("INFO"))
		}

		_, err := c.e.AppendBatch(b, recordengine.AppendLimits{})
		require.NoError(t, err)
	}

	require.NoError(t, c.e.Flush(context.Background()))
}

// mergeAll forces merges until the part set stops changing.
func (c *goldenCorpus) mergeAll(t *testing.T, retainFrom int64) {
	t.Helper()

	ctx := context.Background()

	for range 16 {
		prev := c.e.PartPrefixes()

		require.NoError(t, c.e.MergeWith(ctx, recordengine.MergeOptions{RetainFrom: retainFrom, Force: true}))

		if slices.Equal(prev, c.e.PartPrefixes()) {
			return
		}
	}

	t.Fatal("merges did not converge")
}

// goldenMergeCorpus builds a store over b and merges it twice: first the flushed parts, then those
// merged parts together with new flushes under retention, so the second round reads merge output —
// compressed, and carrying the merge's own dictionary layout — as well as flushed parts.
func goldenMergeCorpus(t *testing.T, b backend.Backend, maxPartBytes, window int64) *recordengine.Engine {
	t.Helper()

	ctx := context.Background()
	c := &goldenCorpus{
		e: recordengine.New(recordengine.Config{
			Schema: goldenSchema, Backend: b, Prefix: "golden/logs",
			MaxPartBytes: maxPartBytes, MergeMemoryBytes: -1,
			MergeCompression: compress.AlgorithmZSTD,
		}),
		rnd: rand.New(rand.NewPCG(7, 11)),
	}
	c.e.SetMergeReadWindow(window)
	t.Cleanup(func() { require.NoError(t, c.e.Close(context.WithoutCancel(ctx))) })

	for s := range goldenStreams {
		c.ser = append(c.ser, signal.Series{Resource: signal.Resource{Attributes: signal.NewAttributes(
			signal.KeyValue{Key: []byte("service.name"), Value: signal.StringValue([]byte("svc-" + strconv.Itoa(s)))},
		)}})
	}

	const rows = 1800

	for p := range 4 {
		// Overlapping ranges, so equal timestamps meet across parts as well as within them.
		c.flush(t, int64(p)*4000, rows)
	}

	c.mergeAll(t, 0)

	c.flush(t, 20_000, rows)
	c.flush(t, 22_000, rows)
	c.mergeAll(t, 3000)

	return c.e
}

// partDigest renders every object of every live part, keyed by its name within the part, since the
// part's own prefix is random.
func partDigest(t *testing.T, e *recordengine.Engine, b backend.Backend) string {
	t.Helper()

	ctx := context.Background()

	var lines []string

	for _, prefix := range e.PartPrefixes() {
		keys, err := b.List(ctx, prefix+"/")
		require.NoError(t, err)

		for _, key := range keys {
			data, err := b.Read(ctx, key)
			require.NoError(t, err)

			lines = append(lines, fmt.Sprintf("%-24s %8d %x",
				strings.TrimPrefix(key, prefix+"/"), len(data), sha256.Sum256(data)))
		}
	}

	slices.Sort(lines)

	return strings.Join(lines, "\n") + "\n"
}

// TestMergeOutputGolden pins the merged parts byte for byte, whatever backend the sources are read
// through and however far ahead: how a merge reads its sources is not allowed to change what it
// writes. A zero window reads frame by frame; 4 KiB is below one frame, so every read serves one.
func TestMergeOutputGolden(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name string
		open func(t *testing.T) backend.Backend
	}{
		{"memory", func(*testing.T) backend.Backend { return backend.Memory() }},
		{"file", func(t *testing.T) backend.Backend {
			t.Helper()

			b, err := file.New(t.TempDir())
			require.NoError(t, err)

			return b
		}},
		{"cached-file", func(t *testing.T) backend.Backend {
			t.Helper()

			b, err := file.New(t.TempDir())
			require.NoError(t, err)

			return backend.Cached(b, 1<<20)
		}},
		{"s3", func(t *testing.T) backend.Backend {
			t.Helper()

			return s3.New(s3.NewAWS(inProcessS3(t, "golden"), "golden"), "", s3.WithRetry(reliability.Default()))
		}},
		{"whole-object", func(*testing.T) backend.Backend { return wholeObjectBackend{backend.Memory()} }},
	} {
		for _, shape := range []struct {
			name     string
			maxPart  int64
			goldFile string
		}{
			{"single", 0, "merge_output_single.txt"},
			{"split", 256 << 10, "merge_output_split.txt"},
		} {
			for _, window := range []int64{0, 4 << 10, 1 << 20} {
				t.Run(fmt.Sprintf("%s/%s/window=%d", tc.name, shape.name, window), func(t *testing.T) {
					t.Parallel()

					b := tc.open(t)
					e := goldenMergeCorpus(t, b, shape.maxPart, window)

					gold.Str(t, partDigest(t, e, b), shape.goldFile)
				})
			}
		}
	}
}
