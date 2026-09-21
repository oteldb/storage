package mergestreamtest_test

import (
	"context"
	"encoding/binary"
	"strconv"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/internal/mergestream/mergestreamtest"
	"github.com/oteldb/storage/signal"
)

// fakeStore is a store that holds the suite's invariants by construction: it reads each source part
// once, concatenates, and publishes the output as a data object plus a manifest — so a failure at
// either write leaves at most an orphan object and never a part.
type fakeStore struct {
	be    *mergestreamtest.Backend
	parts []string
	next  int
}

func (s *fakeStore) Ingest(ctx context.Context, rows []mergestreamtest.Row) error {
	key := "part/" + strconv.Itoa(s.next) + "/data"
	s.next++

	if err := s.be.Write(ctx, key, encodeRows(rows)); err != nil {
		return err
	}

	s.parts = append(s.parts, key)

	return nil
}

func (s *fakeStore) Merge(ctx context.Context) error {
	var merged []mergestreamtest.Row

	for _, key := range s.parts {
		rows, err := s.read(ctx, key)
		if err != nil {
			return err
		}

		merged = append(merged, rows...)
	}

	key := "part/" + strconv.Itoa(s.next) + "/data"
	s.next++

	if err := s.be.Write(ctx, key, encodeRows(merged)); err != nil {
		return err
	}

	if err := s.be.Write(ctx, key+".manifest", nil); err != nil {
		return err
	}

	s.parts = []string{key}

	return nil
}

func (s *fakeStore) Rows(ctx context.Context) ([]mergestreamtest.Row, error) {
	var out []mergestreamtest.Row

	for _, key := range s.parts {
		rows, err := s.read(ctx, key)
		if err != nil {
			return nil, err
		}

		out = append(out, rows...)
	}

	return out, nil
}

func (s *fakeStore) Parts() []string { return s.parts }

func (s *fakeStore) Want(src [][]mergestreamtest.Row) []mergestreamtest.Row {
	var out []mergestreamtest.Row
	for _, rows := range src {
		out = append(out, rows...)
	}

	return out
}

func (s *fakeStore) read(ctx context.Context, key string) ([]mergestreamtest.Row, error) {
	data, err := s.be.Read(ctx, key)
	if err != nil {
		return nil, err
	}

	return decodeRows(data), nil
}

func encodeRows(rows []mergestreamtest.Row) []byte {
	out := make([]byte, 0, len(rows)*24)
	for _, r := range rows {
		out = binary.LittleEndian.AppendUint64(out, uint64(r.Stream))
		out = binary.LittleEndian.AppendUint64(out, uint64(r.Ts))
		out = binary.LittleEndian.AppendUint64(out, uint64(r.Val))
	}

	return out
}

func decodeRows(data []byte) []mergestreamtest.Row {
	var out []mergestreamtest.Row
	for ; len(data) >= 24; data = data[24:] {
		out = append(out, mergestreamtest.Row{
			Stream: int(binary.LittleEndian.Uint64(data)),
			Ts:     int64(binary.LittleEndian.Uint64(data[8:])),
			Val:    int64(binary.LittleEndian.Uint64(data[16:])),
		})
	}

	return out
}

// TestSuiteAcceptsACorrectStore is the suite's own acceptance test: a store that holds every
// invariant must pass, or a green engine run means nothing.
func TestSuiteAcceptsACorrectStore(t *testing.T) {
	t.Parallel()

	mergestreamtest.Run(t, func(_ *testing.T, be *mergestreamtest.Backend) mergestreamtest.Store {
		return &fakeStore{be: be}
	})
}

func TestUnion(t *testing.T) {
	t.Parallel()

	assert.Empty(t, mergestreamtest.Union(nil))
	assert.Equal(t,
		[]signal.SeriesID{{Lo: 1}, {Lo: 2}, {Hi: 1}},
		mergestreamtest.Union([][]signal.SeriesID{{{Lo: 2}, {Hi: 1}}, {{Lo: 1}, {Lo: 2}}}),
	)
}

func TestCheckKeys(t *testing.T) {
	t.Parallel()

	mergestreamtest.CheckKeys(t, [][]signal.SeriesID{{{Lo: 1}, {Lo: 3}}, {{Lo: 2}, {Lo: 3}}})
}

func TestBackendFailWriteAt(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	be := mergestreamtest.NewBackend()

	be.FailWriteAt(2)
	require.NoError(t, be.Write(ctx, "a", []byte("1")))
	require.ErrorIs(t, be.Write(ctx, "b", []byte("2")), mergestreamtest.ErrInjected)
	require.NoError(t, be.Write(ctx, "c", []byte("3")))
	assert.Equal(t, 3, be.Writes())

	_, err := be.Read(ctx, "a")
	require.NoError(t, err)
	assert.Equal(t, map[string]int{"a": 1}, be.Reads())
}
