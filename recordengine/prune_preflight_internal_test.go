package recordengine

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/backend"
	"github.com/oteldb/storage/signal"
)

func TestNoopIdentityPruneDoesNotJoinActiveBuild(t *testing.T) {
	t.Parallel()
	for _, tt := range []struct {
		name       string
		identities int
		dirty      bool
	}{
		{name: "clean registry", identities: pruneMinIdentities},
		{name: "small dirty registry", identities: 1, dirty: true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			e := New(Config{Backend: backend.Memory(), Schema: NewSchema()})
			for i := range tt.identities {
				e.head.registerStream(signal.Series{Attributes: signal.NewAttributes(signal.KeyValue{
					Key: []byte("stream"), Value: signal.IntValue(int64(i)),
				})})
			}
			e.identityDirty = tt.dirty
			e.flushMu.RLock()
			defer e.flushMu.RUnlock()
			removed, err := e.PruneIdentities(t.Context())
			require.NoError(t, err)
			require.Zero(t, removed)
			require.Equal(t, tt.identities, e.head.series.Len())
			require.Equal(t, tt.dirty, e.identityDirty)
		})
	}
}
