package recordengine

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestAppendCollapseDigits(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct{ in, want string }{
		{"", ""},
		{"no digits here", "no digits here"},
		{"1", "#"},
		{"12345", "#"},
		{"a1b22c333", "a#b#c#"},
		{"2026-08-15T10:04:05.123456Z", "#-#-#T#:#:#.#Z"},
		{"pvc-42 bound in 7ms", "pvc-# bound in #ms"},
		{"999", "#"},
	} {
		assert.Equal(t, tc.want, string(appendCollapseDigits(nil, []byte(tc.in))), "input %q", tc.in)
	}
}

func TestAppendCollapseDigitsReusesBuffer(t *testing.T) {
	t.Parallel()

	buf := make([]byte, 0, 64)
	for range 100 {
		buf = appendCollapseDigits(buf[:0], []byte("volume 12345 attached"))
	}

	assert.Equal(t, "volume # attached", string(buf))
	assert.Equal(t, 64, cap(buf), "the scratch is never regrown")
}

func FuzzAppendCollapseDigits(f *testing.F) {
	for _, s := range []string{"", "1", "a1b22c333", "2026-08-15T10:04:05Z", "no digits"} {
		f.Add([]byte(s))
	}

	f.Fuzz(func(t *testing.T, v []byte) {
		got := appendCollapseDigits(nil, v)

		require.LessOrEqual(t, len(got), len(v))
		require.False(t, bytes.ContainsAny(got, "0123456789"), "digits are collapsed away")
		require.Equal(t, string(got), string(appendCollapseDigits(nil, got)), "collapsing is idempotent")
	})
}

// BenchmarkAppendCollapseDigits sizes by the input, the logical data the collapse walks. It runs
// only inside [Engine.StreamCost], never on the write path.
func BenchmarkAppendCollapseDigits(b *testing.B) {
	line := []byte("I0815 10:04:05.123456       1 volume.go:120] reconciling pvc-4815162342 on node-7")
	buf := make([]byte, 0, len(line))

	b.ReportAllocs()
	b.SetBytes(int64(len(line)))
	b.ResetTimer()

	for range b.N {
		buf = appendCollapseDigits(buf[:0], line)
	}

	if len(buf) == 0 {
		b.Fatal("empty")
	}
}
