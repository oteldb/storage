package engine

import (
	"bytes"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/backend"
	"github.com/oteldb/storage/signal"
	"github.com/oteldb/storage/wal"
)

type gatedEncodingWriter struct {
	dst     *bytes.Buffer
	entered chan struct{}
	release chan struct{}
	once    sync.Once
}

func (w *gatedEncodingWriter) Write(p []byte) (int, error) {
	w.once.Do(func() {
		close(w.entered)
		<-w.release
	})
	return w.dst.Write(p)
}

func TestReadersDuringWALEncoding(t *testing.T) {
	t.Parallel()
	for _, op := range []string{"append", "batch", "primary"} {
		for _, fresh := range []bool{false, true} {
			name := op + "/existing"
			if fresh {
				name = op + "/fresh"
			}
			t.Run(name, func(t *testing.T) {
				t.Parallel()
				e, _ := newFaultWALEngine(t, 0)
				e.cfg.Backend, e.cfg.Prefix = backend.Memory(), "metrics"
				s := stagedTestSeries("known")
				_, err := e.Append(s, 1, 1)
				require.NoError(t, err)
				_, err = e.Append(stagedTestSeries("other"), 1, 2)
				require.NoError(t, err)
				require.NoError(t, e.Flush(t.Context()))
				if fresh {
					s = stagedTestSeries("fresh")
				}
				var input bytes.Buffer
				fw := wal.NewWriter(&input)
				require.NoError(t, fw.WriteSeries(s.Hash(), s))
				require.NoError(t, fw.WriteSamples(s.Hash(), []int64{100}, []float64{3}))
				gate := &gatedEncodingWriter{
					dst: &e.walB.frames, entered: make(chan struct{}), release: make(chan struct{}),
				}
				unblock := sync.OnceFunc(func() { close(gate.release) })
				t.Cleanup(unblock)
				e.walB.fw = wal.NewWriter(gate)
				written := make(chan error, 1)
				go func() {
					var err error
					switch op {
					case "append":
						_, err = e.Append(s, 100, 3)
					case "batch":
						_, err = e.AppendBatch([]signal.SeriesID{s.Hash()}, []int64{100}, []float64{3}, nil,
							func(int) signal.Series { return s }, AppendLimits{})
					case "primary":
						_, _, err = e.ApplyPrimary(input.Bytes(), AppendLimits{})
					}
					written <- err
				}()
				<-gate.entered
				wait := sync.OnceValue(func() error { return <-written })
				t.Cleanup(func() { unblock(); require.NoError(t, wait()) })
				assertReadableDuringWAL(t, e, 2)
				unblock()
				require.NoError(t, wait())
			})
		}
	}
}
