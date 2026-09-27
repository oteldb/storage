package partsync

import (
	"context"
	"strconv"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/backend"
	"github.com/oteldb/storage/backend/bucketindex"
	"github.com/oteldb/storage/internal/partid"
)

func livePartScan(key string, liveParts map[string]struct{}) bool {
	if part, _, ok := strings.Cut(key, shardMarker); ok {
		_, live := liveParts[part]

		return live
	}

	for part := range liveParts {
		if strings.HasPrefix(key, part+"/") {
			return true
		}
	}

	return false
}

func partSet(parts []string) map[string]struct{} {
	m := make(map[string]struct{}, len(parts))
	for _, p := range parts {
		m[p] = struct{}{}
	}

	return m
}

var livePartCases = []struct {
	name string
	key  string
	live []string
	want bool
}{
	{"object", "t/m/p1/col.bin", []string{"t/m/p1"}, true},
	{"deep object", "t/m/p1/cols/ts/0.bin", []string{"t/m/p1"}, true},
	{"trailing slash", "t/m/p1/", []string{"t/m/p1"}, true},
	{"double slash", "t/m/p1//x", []string{"t/m/p1"}, true},
	{"key equals prefix", "t/m/p1", []string{"t/m/p1"}, false},
	{"superseded", "t/m/p2/col.bin", []string{"t/m/p1"}, false},
	{"string prefix shorter", "t/m/p10/col.bin", []string{"t/m/p1"}, false},
	{"string prefix longer", "t/m/p1/col.bin", []string{"t/m/p10"}, false},
	{"string prefix both", "t/m/p10/col.bin", []string{"t/m/p1", "t/m/p10"}, true},
	{"string prefix neither", "t/m/p100/col.bin", []string{"t/m/p1", "t/m/p10"}, false},
	{"nested outer", "t/m/a/bc", []string{"t/m/a", "t/m/a/b"}, true},
	{"nested inner", "t/m/a/b/c", []string{"t/m/a/b"}, true},
	{"nested both", "t/m/a/b/c", []string{"t/m/a", "t/m/a/b"}, true},
	{"sidecar", "t/m/bucket-index.bin", []string{"t/m/p1"}, false},
	{"engine prefix live", "t/m/bucket-index.bin", []string{"t/m"}, true},
	{"empty prefix", "/x", []string{""}, true},
	{"empty prefix no slash", "x", []string{""}, false},
	{"empty key", "", []string{"t/m/p1"}, false},
	{"no parts", "t/m/p1/col.bin", nil, false},
	{"shard live", "t/m/p1/ecshard/0/col.bin", []string{"t/m/p1"}, true},
	{"shard superseded", "t/m/p2/ecshard/0/col.bin", []string{"t/m/p1"}, false},
	{"shard string prefix", "t/m/p10/ecshard/0/col.bin", []string{"t/m/p1"}, false},
	{"shard nested part", "t/m/a/b/ecshard/1/x", []string{"t/m/a/b"}, true},
	{"shard under outer part only", "t/m/a/c/ecshard/1/x", []string{"t/m/a"}, false},
	{"shard first marker wins", "t/m/p1/ecshard/0/ecshard/1", []string{"t/m/p1/ecshard/0"}, false},
}

func TestLivePart(t *testing.T) {
	t.Parallel()

	for _, tc := range livePartCases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			live := partSet(tc.live)
			assert.Equal(t, tc.want, livePartScan(tc.key, live), "reference")
			assert.Equal(t, tc.want, livePart(tc.key, live))
		})
	}
}

func FuzzLivePart(f *testing.F) {
	for _, tc := range livePartCases {
		f.Add(tc.key, strings.Join(tc.live, "\n"))
	}

	f.Fuzz(func(t *testing.T, key, parts string) {
		live := partSet(strings.Split(parts, "\n"))
		if got, want := livePart(key, live), livePartScan(key, live); got != want {
			t.Fatalf("livePart(%q, %q) = %v, scan = %v", key, parts, got, want)
		}
	})
}

func partOfCut(key, enginePrefix string) string {
	if part, _, ok := strings.Cut(key, shardMarker); ok {
		return part
	}

	rest, ok := strings.CutPrefix(key, enginePrefix+"/")
	if !ok {
		return ""
	}

	seg, _, ok := strings.Cut(rest, "/")
	if !ok {
		return ""
	}

	return enginePrefix + "/" + seg
}

var partOfCases = []struct {
	key, want string
}{
	{"t/m/p1/col.bin", "t/m/p1"},
	{"t/m/p1/cols/ts.bin", "t/m/p1"},
	{"t/m/p1/", "t/m/p1"},
	{"t/m/p1/ecshard/0/col.bin", "t/m/p1"},
	{"t/m/bucket-index.bin", ""},
	{"t/m", ""},
	{"t/m/", ""},
	{"t/mx/p1/col.bin", ""},
	{"x/p1/col.bin", ""},
	{"t/m//col.bin", "t/m/"},
}

func TestPartOf(t *testing.T) {
	t.Parallel()

	for _, tc := range partOfCases {
		assert.Equal(t, tc.want, partOfCut(tc.key, "t/m"), "reference %s", tc.key)
		assert.Equal(t, tc.want, partOf(tc.key, "t/m"), tc.key)
	}
}

func FuzzPartOf(f *testing.F) {
	for _, tc := range partOfCases {
		f.Add(tc.key, "t/m")
	}

	f.Fuzz(func(t *testing.T, key, enginePrefix string) {
		if got, want := partOf(key, enginePrefix), partOfCut(key, enginePrefix); got != want {
			t.Fatalf("partOf(%q, %q) = %q, cut = %q", key, enginePrefix, got, want)
		}
	})
}

type pruneFixture struct {
	syncer *Syncer
	prefix string
	keys   []string
	del    deletion
}

// newPruneFixture lays out a replica the size of a production exemplar prefix: live parts of
// objectsPerPart objects each, plus a tenth as many superseded parts the peer still lists.
func newPruneFixture(tb testing.TB, liveParts, objectsPerPart int) pruneFixture {
	tb.Helper()

	const prefix = "default/exemplars"

	ctx := context.Background()
	be := backend.Memory()
	s := New(be, nil)

	var (
		keys []string
		live = make(map[string]struct{}, liveParts)
	)

	addPart := func(isLive bool) {
		part := prefix + "/" + partid.New().String()
		if isLive {
			live[part] = struct{}{}
		}

		for j := range objectsPerPart {
			keys = append(keys, part+"/cols/"+strconv.Itoa(j)+".bin")
		}
	}

	for range liveParts {
		addPart(true)
	}

	for range liveParts / 10 {
		addPart(false)
	}

	keys = append(keys, prefix+"/"+bucketindex.Object, prefix+"/streams.bin")

	for _, k := range keys {
		require.NoError(tb, be.Write(ctx, k, []byte{1}))
	}

	s.stateFor(prefix).remote = keySet(keys)

	return pruneFixture{syncer: s, prefix: prefix, keys: keys, del: deletion{live: live}}
}

func BenchmarkLivePart(b *testing.B) {
	fx := newPruneFixture(b, 6400, 12)

	b.ReportAllocs()

	for b.Loop() {
		for _, k := range fx.keys {
			livePart(k, fx.del.live)
		}
	}

	b.ReportMetric(float64(len(fx.keys)), "keys/op")
}

func BenchmarkPrune(b *testing.B) {
	fx := newPruneFixture(b, 6400, 12)
	ctx := context.Background()

	b.ReportAllocs()

	for b.Loop() {
		var st Stats
		if err := fx.syncer.prune(ctx, &st, fx.prefix, keepAll, fx.del); err != nil {
			b.Fatal(err)
		}

		if st.Pruned != 0 {
			b.Fatalf("pruned %d objects", st.Pruned)
		}
	}
}
