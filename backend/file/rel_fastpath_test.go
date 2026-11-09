package file

import (
	"path"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestRelCanonicalPath(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		key, want string
		invalid   bool
	}{
		{"part/c/0", "part/c/0", false}, {"./part//c/0", "part/c/0", false},
		{"part/c/0/", "part/c/0", false}, {"", ".", false}, {".", ".", false},
		{"../escape", "", true}, {"/part/c/0", "part/c/0", false},
	} {
		t.Run(tc.key, func(t *testing.T) {
			t.Parallel()
			got, err := (*File)(nil).rel(tc.key)
			require.Equal(t, tc.invalid, err != nil)
			if !tc.invalid {
				require.Equal(t, tc.want, got)
			}
		})
	}
}

func FuzzRelCanonicalPath(f *testing.F) {
	for _, key := range []string{"part/c/0", "./part//c/0", "part/c/0/", "", ".", "../escape", "/part/c/0", "x\\y", "x\x00y", "\xff"} {
		f.Add(key)
	}
	f.Fuzz(func(t *testing.T, key string) {
		want := strings.TrimPrefix(path.Clean("/"+key), "/")
		if want == "" {
			want = "."
		}
		invalid := want != path.Clean("./"+key)
		got, err := (*File)(nil).rel(key)
		if invalid != (err != nil) || (!invalid && got != want) {
			t.Fatalf("key %q: got %q/%v, want %q invalid=%v", key, got, err, want, invalid)
		}
	})
}
