package cluster

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/go-faster/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/readbudget"
)

// A budget refusal is the one error on this path that is the caller's fault, and an embedder maps it
// to a 4xx on that basis. The sentinel has to survive the hop for that to hold: a read served
// locally and the same read served by a peer must be the same kind of error.
func TestBudgetSentinelCrossesTheWire(t *testing.T) {
	t.Parallel()

	local := errors.Wrap(readbudget.ErrExceeded, "reserve 1227385730 bytes: holding 0 of 966367641")

	w := httptest.NewRecorder()
	writeRPCError(w, local)
	require.Equal(t, budgetStatus, w.Code)

	remote := statusError("peer:7946", "fetch", w.Code, w.Body.Bytes())
	require.ErrorIs(t, remote, readbudget.ErrExceeded,
		"a budget refusal from a peer is still a budget refusal")
	require.NotErrorIs(t, remote, ErrShardAbsent,
		"and must not read as absence, which would fail over to an owner that refuses identically")
	assert.Contains(t, remote.Error(), "holding 0 of 966367641",
		"the peer's numbers reach the operator, not just the sentinel")
}

// Absence and exhaustion are both non-200s a caller branches on, and they call for opposite
// responses: one fails over, the other must not.
func TestRPCErrorStatusesAreDistinct(t *testing.T) {
	t.Parallel()

	require.NotEqual(t, absentStatus, budgetStatus)

	for name, tt := range map[string]struct {
		err  error
		code int
		is   error
	}{
		"absent": {ErrShardAbsent, absentStatus, ErrShardAbsent},
		"budget": {readbudget.ErrExceeded, budgetStatus, readbudget.ErrExceeded},
		"other":  {errors.New("disk on fire"), http.StatusInternalServerError, nil},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			w := httptest.NewRecorder()
			writeRPCError(w, tt.err)
			require.Equal(t, tt.code, w.Code)

			got := statusError("peer:7946", "fetch", w.Code, w.Body.Bytes())
			if tt.is == nil {
				require.NotErrorIs(t, got, ErrShardAbsent)
				require.NotErrorIs(t, got, readbudget.ErrExceeded)

				return
			}

			assert.ErrorIs(t, got, tt.is)
		})
	}
}
