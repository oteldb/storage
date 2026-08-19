package partid_test

import (
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/internal/partid"
)

func TestNewUnique(t *testing.T) {
	t.Parallel()

	const n = 10000

	seen := make(map[partid.ID]struct{}, n)
	for range n {
		id := partid.New()
		_, dup := seen[id]
		require.False(t, dup, "ids must be unique")
		seen[id] = struct{}{}
	}
}

func TestNewMonotonic(t *testing.T) {
	t.Parallel()

	ids := make([]string, 1000)
	for i := range ids {
		ids[i] = partid.New().String()
	}

	require.True(t, slices.IsSorted(ids), "textual ids must sort in creation order")
	require.Len(t, slices.Compact(slices.Clone(ids)), len(ids), "ids must be unique")
}

func TestNewConcurrent(t *testing.T) {
	t.Parallel()

	const (
		workers = 8
		perW    = 500
	)

	var (
		wg  sync.WaitGroup
		mu  sync.Mutex
		all = make(map[partid.ID]struct{}, workers*perW)
	)

	for range workers {
		wg.Go(func() {
			ids := make([]partid.ID, perW)
			for i := range ids {
				ids[i] = partid.New()
			}

			mu.Lock()
			defer mu.Unlock()

			for _, id := range ids {
				all[id] = struct{}{}
			}
		})
	}

	wg.Wait()
	assert.Len(t, all, workers*perW)
}

func TestTime(t *testing.T) {
	t.Parallel()

	before := time.Now().Add(-time.Second)
	id := partid.New()
	after := time.Now().Add(time.Second)

	got := id.Time()
	assert.False(t, got.Before(before))
	assert.False(t, got.After(after))
}

func TestStringLen(t *testing.T) {
	t.Parallel()

	assert.Len(t, partid.New().String(), partid.EncodedLen)
}

func TestParse(t *testing.T) {
	t.Parallel()

	for _, tt := range []struct {
		name string
		in   string
		want partid.ID
	}{
		{"zero", "00000000000000000000000000", partid.ID{}},
		{
			"max",
			"7ZZZZZZZZZZZZZZZZZZZZZZZZZ",
			partid.ID{
				0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF,
				0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF,
			},
		},
		{
			"low bit",
			"00000000000000000000000001",
			partid.ID{15: 1},
		},
		{
			"high bit",
			"40000000000000000000000000",
			partid.ID{0: 0x80},
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got, err := partid.Parse(tt.in)
			require.NoError(t, err)
			assert.Equal(t, tt.want, got)
			assert.Equal(t, tt.in, got.String())
		})
	}
}

func TestParseInvalid(t *testing.T) {
	t.Parallel()

	for _, tt := range []struct {
		name string
		in   string
	}{
		{"empty", ""},
		{"short", "0000000000000000000000000"},
		{"long", "000000000000000000000000000"},
		{"lowercase", "0000000000000000000000000a"},
		{"excluded letter", "0000000000000000000000000I"},
		{"overflow", "80000000000000000000000000"},
		{"overflow max", "ZZZZZZZZZZZZZZZZZZZZZZZZZZ"},
		{"separator", "0000000000000/000000000000"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			_, err := partid.Parse(tt.in)
			require.ErrorIs(t, err, partid.ErrInvalid)
			assert.False(t, partid.Valid(tt.in))
		})
	}
}

func TestValid(t *testing.T) {
	t.Parallel()

	assert.True(t, partid.Valid(partid.New().String()))
	assert.False(t, partid.Valid("0000000000"), "a legacy zero-padded sequence is not a part id")
}

func TestAppendText(t *testing.T) {
	t.Parallel()

	id := partid.New()

	appended, err := id.AppendText([]byte("p/"))
	require.NoError(t, err)
	assert.Equal(t, "p/"+id.String(), string(appended))

	got, err := partid.Parse(string(appended[len("p/"):]))
	require.NoError(t, err)
	assert.Equal(t, id, got)
}
