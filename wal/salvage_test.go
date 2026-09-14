package wal

import (
	"fmt"
	"strings"
	"testing"

	"github.com/go-faster/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/internal/vfs/faultfs"
	"github.com/oteldb/storage/signal"
)

var (
	errHandler = errors.New("handler refused")
	errRefused = errors.New("too much damage")
)

func sideFrames(payloads ...string) []byte {
	var data []byte
	for _, p := range payloads {
		data = appendFrame(data, recordSide, []byte(p))
	}

	return data
}

// replayDirSides replays fsys, returning the side payloads; samples records are decoded and dropped.
// With onDamage nil the replay is strict.
func replayDirSides(fsys *faultfs.FS, onDamage func(Damage) error) ([]string, error) {
	var got []string

	err := replayDirFrom(fsys, 0, Handlers{
		OnSide:    func(p []byte) error { got = append(got, string(p)); return nil },
		OnSamples: func(signal.SeriesID, []int64, []float64) error { return nil },
		OnDamage:  onDamage,
	})

	return got, err
}

// salvaged replays fsys salvaging damage, returning the side payloads and the damage reported.
func salvaged(t *testing.T, fsys *faultfs.FS) ([]string, []Damage) {
	t.Helper()

	var damages []Damage

	got, err := replayDirSides(fsys, func(d Damage) error { damages = append(damages, d); return nil })
	require.NoError(t, err)

	return got, damages
}

func TestSalvageSkipsDamage(t *testing.T) {
	t.Parallel()

	a, b, c := sideFrames("a"), sideFrames("b"), sideFrames("c")

	badCRC := sideFrames("a", "b", "c")
	badCRC[len(a)+len(b)-1] ^= 0xFF

	hole := sideFrames("a", "b", "c", "d")
	clear(hole[len(a) : len(a)+len(b)])

	undecodable := append(sideFrames("a"), appendFrame(nil, recordSamples, []byte("short"))...)
	undecodable = append(undecodable, sideFrames("c")...)

	for _, tc := range []struct {
		name     string
		segments [][]byte
		want     []string
		damaged  []int // offsets of the reported regions, per the segment order
	}{
		{name: "BadCRC", segments: [][]byte{badCRC}, want: []string{"a", "c"}, damaged: []int{len(a)}},
		{name: "Hole", segments: [][]byte{hole}, want: []string{"a", "c", "d"}, damaged: []int{len(a)}},
		{name: "Undecodable", segments: [][]byte{undecodable}, want: []string{"a", "c"}, damaged: []int{len(a)}},
		{
			name:     "TornNonFinalSegment",
			segments: [][]byte{sideFrames("a", "b")[:len(a)+2], c},
			want:     []string{"a", "c"},
			damaged:  []int{len(a)},
		},
		{
			name:     "BadCRCAtEnd",
			segments: [][]byte{append(sideFrames("a"), badCRC[len(a):len(a)+len(b)]...), c},
			want:     []string{"a", "c"},
			damaged:  []int{len(a)},
		},
		{
			name:     "TornFinalTailIsNotDamage",
			segments: [][]byte{a, sideFrames("b", "c")[:len(b)+2]},
			want:     []string{"a", "b"},
		},
		{
			name:     "ZeroFilledTailIsNotDamage",
			segments: [][]byte{append(sideFrames("a", "b"), make([]byte, 4096)...)},
			want:     []string{"a", "b"},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			fsys := faultfs.New()
			for i, seg := range tc.segments {
				writeSeg(t, fsys, i+1, seg)
			}

			got, damages := salvaged(t, fsys)
			assert.Equal(t, tc.want, got)

			offsets := make([]int, 0, len(damages))
			for _, d := range damages {
				offsets = append(offsets, d.Offset)
				require.ErrorIs(t, d.Err, ErrCorrupt)
				assert.NotEmpty(t, d.Kept)
			}

			assert.ElementsMatch(t, tc.damaged, offsets)

			if len(tc.damaged) == 0 {
				return
			}

			_, err := replayDirSides(fsys, nil)
			require.ErrorIs(t, err, ErrCorrupt, "a strict replay refuses the same log")
		})
	}
}

// TestSalvageNeedsTwoFramesToResync: a region past damage is trusted only from a frame followed by
// another valid frame or by the end of the segment, so a lone chance match inside garbage is not
// replayed.
func TestSalvageNeedsTwoFramesToResync(t *testing.T) {
	t.Parallel()

	lone := sideFrames("lone")
	torn := sideFrames("torn")

	data := append(make([]byte, 64), lone...)
	data = append(data, torn[:len(torn)-1]...)

	fsys := faultfs.New()
	writeSeg(t, fsys, 1, data)

	got, damages := salvaged(t, fsys)
	assert.Empty(t, got)
	assert.Empty(t, damages, "without a trusted frame after it, the damage is the torn tail")
}

func TestSalvageStopsOnHandlerError(t *testing.T) {
	t.Parallel()

	fsys := faultfs.New()
	writeSeg(t, fsys, 1, sideFrames("a", "b"))

	err := replayDirFrom(fsys, 0, Handlers{
		OnSide:   func([]byte) error { return errHandler },
		OnDamage: func(Damage) error { return nil },
	})
	require.ErrorIs(t, err, errHandler)
}

func TestSalvageStopsWhenDamageIsRefused(t *testing.T) {
	t.Parallel()

	data := sideFrames("a", "b", "c")
	data[len(sideFrames("a"))+2] ^= 0xFF

	fsys := faultfs.New()
	writeSeg(t, fsys, 1, data)

	var got []string

	err := replayDirFrom(fsys, 0, Handlers{
		OnSide:   func(p []byte) error { got = append(got, string(p)); return nil },
		OnDamage: func(Damage) error { return errRefused },
	})
	require.ErrorIs(t, err, errRefused)
	assert.Equal(t, []string{"a"}, got)
}

// TestSalvageKeepsBoundedCopies: a damaged segment is copied aside once, under a name replay never
// reads, and only the newest copies are kept.
func TestSalvageKeepsBoundedCopies(t *testing.T) {
	t.Parallel()

	fsys := faultfs.New()

	for seq := 1; seq <= maxDamagedCopies+2; seq++ {
		data := sideFrames(fmt.Sprintf("s%d-a", seq), "b", "c")
		data[len(sideFrames(fmt.Sprintf("s%d-a", seq)))+2] ^= 0xFF
		writeSeg(t, fsys, seq, data)
	}

	for range 2 {
		got, damages := salvaged(t, fsys)
		assert.Len(t, got, 2*(maxDamagedCopies+2))
		assert.Len(t, damages, maxDamagedCopies+2)
	}

	entries, err := fsys.ReadDir(".")
	require.NoError(t, err)

	var copies []string

	for _, e := range entries {
		if strings.HasSuffix(e.Name(), damagedExt) {
			copies = append(copies, e.Name())
		}
	}

	want := make([]string, 0, maxDamagedCopies)
	for seq := 3; seq <= maxDamagedCopies+2; seq++ {
		want = append(want, segmentName(seq, 1)+damagedExt)
	}

	assert.Equal(t, want, copies)

	for _, name := range copies {
		kept, err := fsys.ReadFile(name)
		require.NoError(t, err)

		orig, err := fsys.ReadFile(strings.TrimSuffix(name, damagedExt))
		require.NoError(t, err)
		assert.Equal(t, orig, kept)
	}
}

// TestCreateCutsOnlyTheTornTailPastDamage: the startup repair walks past a hole to find where the
// readable log ends, and cuts only the torn tail behind it.
func TestCreateCutsOnlyTheTornTailPastDamage(t *testing.T) {
	t.Parallel()

	a, b := sideFrames("a"), sideFrames("b")

	data := sideFrames("a", "b", "c", "d", "torn")
	clear(data[len(a) : len(a)+len(b)])

	tail := len(sideFrames("a", "b", "c", "d"))
	data = data[:len(data)-2]

	fsys := faultfs.New()
	writeSeg(t, fsys, 1, data)

	w, err := createFS(fsys, 0)
	require.NoError(t, err)
	require.NoError(t, w.WriteSide([]byte("after")))
	require.NoError(t, w.Close())

	repaired, err := fsys.ReadFile(segmentName(1, 1))
	require.NoError(t, err)
	assert.Equal(t, data[:tail], repaired)

	got, damages := salvaged(t, fsys)
	assert.Equal(t, []string{"a", "c", "d", "after"}, got)
	assert.Len(t, damages, 1)
}
