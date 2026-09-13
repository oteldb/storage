package block

import (
	"context"
	"strconv"
	"strings"

	"github.com/oteldb/storage/backend"
)

// PartSizes is the on-backend size of a part's objects.
type PartSizes struct {
	Total int64
	// Columns is keyed by manifest column index. A constant-collapsed column has no object, so no entry.
	Columns map[int]int64
	// Other is every non-column object (manifest, marks, and whatever indexes the engine writes
	// alongside), keyed by its name under the part prefix.
	Other map[string]int64
}

// PartObjectSizes sizes every object under the part at prefix, using the [backend.Sizer] fast path
// when available. A nil backend has no objects.
func PartObjectSizes(ctx context.Context, b backend.Backend, prefix string) (PartSizes, error) {
	out := PartSizes{Columns: map[int]int64{}, Other: map[string]int64{}}
	if b == nil {
		return out, nil
	}

	dir := prefix + "/"

	keys, err := b.List(ctx, dir)
	if err != nil {
		return out, err
	}

	for _, k := range keys {
		n, err := backend.SizeOf(ctx, b, k)
		if err != nil {
			return out, err
		}

		out.Total += n

		name := strings.TrimPrefix(k, dir)
		if i, ok := columnIndex(name); ok {
			out.Columns[i] += n
		} else {
			out.Other[name] += n
		}
	}

	return out, nil
}

// columnIndex parses the "c/{i}" name [columnKey] gives column i.
func columnIndex(name string) (int, bool) {
	s, ok := strings.CutPrefix(name, "c/")
	if !ok {
		return 0, false
	}

	i, err := strconv.Atoi(s)
	if err != nil || i < 0 || strconv.Itoa(i) != s {
		return 0, false
	}

	return i, true
}
