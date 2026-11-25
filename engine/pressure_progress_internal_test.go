package engine

import (
	"context"
	"sync"
	"testing"
	"testing/synctest"

	"github.com/stretchr/testify/require"
)

type pressureReclaimGate struct {
	*pressureGateBackend

	entered, resume chan struct{}
	once            sync.Once
}

func (b *pressureReclaimGate) List(ctx context.Context, prefix string) ([]string, error) {
	if prefix == "pressure-contract/retired/" {
		b.once.Do(func() { close(b.entered) })
		select {
		case <-b.resume:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	return b.pressureGateBackend.List(ctx, prefix)
}

func TestPressureWritersResumeBeforeRetiredDeletion(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		e, _, be, resumeBuild := pressureTestEngine(t)
		defer resumeBuild()
		gate := &pressureReclaimGate{pressureGateBackend: be, entered: make(chan struct{}), resume: make(chan struct{})}
		resumeReclaim := sync.OnceFunc(func() { close(gate.resume) })
		defer resumeReclaim()
		e.cfg.Backend = gate
		_, err := e.Append(benchSeries(1000), 200, 1)
		require.NoError(t, err)
		e.mu.Lock()
		e.cfg.ResidentHeadBytes = e.head.residentIdentityBytes() + e.head.inFlightBytes() - 1
		e.retiring = append(e.retiring, &part{prefix: "pressure-contract/retired"})
		e.mu.Unlock()
		done := make(chan error, 1)
		go func() { done <- e.pipelineResidentPressure() }()
		synctest.Wait()
		select {
		case <-done:
			t.Error("writer must wait while input is still resident")
		default:
		}
		resumeBuild()
		<-gate.entered
		synctest.Wait()
		select {
		case err := <-done:
			require.NoError(t, err)
		default:
			t.Error("writer remains parked after input release while retired deletion blocks")
		}
		resumeReclaim()
		require.NoError(t, e.Close(t.Context()))
	})
}
