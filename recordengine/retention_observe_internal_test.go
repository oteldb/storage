package recordengine

import (
	"sync/atomic"
	"testing"

	"github.com/go-faster/errors"

	"github.com/stretchr/testify/require"
)

func TestRetentionRequestBelongsToCurrentTenure(t *testing.T) {
	t.Parallel()
	var current atomic.Uint64
	current.Store(7)
	e := New(Config{Term: current.Load, Schema: NewSchema(Column{Name: "body", Kind: KindBytes})})
	e.recordRetentionRequest(100, RetentionBoundAge)
	require.EqualValues(t, 100, e.Stats().RetainFrom)
	current.Store(0)
	require.Zero(t, e.Stats().RetainFrom)
	e.recordRetentionRequest(200, RetentionBoundSignalBytes)
	current.Store(7)
	require.EqualValues(t, 100, e.Stats().RetainFrom, "refused requests do not overwrite the last authorized tenure")
	current.Store(8)
	require.Zero(t, e.Stats().RetainFrom)
	e.recordRetentionRequest(300, RetentionBoundPooledBytes)
	stats := e.Stats()
	require.EqualValues(t, 300, stats.RetainFrom)
	require.Equal(t, RetentionBoundPooledBytes, stats.RetentionBoundBy)
	e.recordRetentionRequest(0, RetentionBoundAge)
	require.Zero(t, e.Stats().RetainFrom)
	require.Equal(t, RetentionBoundNone, e.Stats().RetentionBoundBy)
}

func TestRetentionRequestRemainsVisibleWhenIndexIsFenced(t *testing.T) {
	t.Parallel()
	e := New(Config{Schema: NewSchema(Column{Name: "body", Kind: KindBytes})})
	e.loadErr = errors.New("index unavailable")
	err := e.MergeWith(t.Context(), MergeOptions{RetainFrom: 200, RetentionBoundBy: RetentionBoundAge})
	require.Error(t, err)
	stats := e.Stats()
	require.True(t, stats.IndexFenced)
	require.EqualValues(t, 200, stats.RetainFrom)
	require.Equal(t, RetentionBoundAge, stats.RetentionBoundBy)
}
