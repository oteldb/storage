package backendtest

import (
	"context"
	"crypto/sha256"
	"fmt"
	"maps"
	"path"
	"slices"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/backend"
	"github.com/oteldb/storage/internal/partid"
)

// IsPartObject reports whether key names an object inside a part directory rather than an
// engine-level object (the bucket index, the series or stream index).
func IsPartObject(key string) bool {
	return slices.ContainsFunc(strings.Split(path.Dir(key), "/"), partid.Valid)
}

// PartDirs returns the sorted part ids that have objects under enginePrefix. Part ids are minted, so
// a test cannot name them up front and asserts over this set instead.
func PartDirs(ctx context.Context, tb testing.TB, b backend.Backend, enginePrefix string) []string {
	tb.Helper()

	keys, err := b.List(ctx, enginePrefix+"/")
	require.NoError(tb, err)

	seen := make(map[string]struct{}, len(keys))

	for _, k := range keys {
		dir, _, ok := strings.Cut(strings.TrimPrefix(k, enginePrefix+"/"), "/")
		if ok && partid.Valid(dir) {
			seen[dir] = struct{}{}
		}
	}

	return slices.Sorted(maps.Keys(seen))
}

// Digest renders every object under each of partPrefixes as one sorted "name size sha256" line, the
// name relative to its part prefix, since that prefix is random. It is the golden-file form of a
// part set.
func Digest(tb testing.TB, b backend.Backend, partPrefixes []string) string {
	tb.Helper()

	ctx := context.Background()

	var lines []string

	for _, prefix := range partPrefixes {
		keys, err := b.List(ctx, prefix+"/")
		require.NoError(tb, err)

		for _, key := range keys {
			data, err := b.Read(ctx, key)
			require.NoError(tb, err)

			lines = append(lines, fmt.Sprintf("%-24s %8d %x",
				strings.TrimPrefix(key, prefix+"/"), len(data), sha256.Sum256(data)))
		}
	}

	slices.Sort(lines)

	return strings.Join(lines, "\n") + "\n"
}
