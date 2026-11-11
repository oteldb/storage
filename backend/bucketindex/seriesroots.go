package bucketindex

import (
	"encoding/binary"
	"slices"
	"strings"

	"github.com/go-faster/errors"
)

// SeriesRoot names an immutable identity-only run and binds it to its manifest bytes.
type SeriesRoot struct {
	Prefix         string
	ManifestDigest [32]byte
}

// AddSeriesRoot adds root without replacing a conflicting manifest binding.
func (ix *Index) AddSeriesRoot(root SeriesRoot) error {
	roots, err := MergeSeriesRoots(ix.SeriesRoots, []SeriesRoot{root})
	if err != nil {
		return err
	}
	ix.SeriesRoots = roots
	return nil
}

// MergeSeriesRoots returns an owned, sorted union for catalog adoption and commit retries.
// A prefix with different manifest digests is corrupt; inputs remain unchanged on every path.
func MergeSeriesRoots(groups ...[]SeriesRoot) ([]SeriesRoot, error) {
	var out []SeriesRoot
	for _, roots := range groups {
		for _, root := range roots {
			if err := validateSeriesRoot(root); err != nil {
				return nil, err
			}
			out = append(out, root)
		}
	}
	slices.SortFunc(out, compareSeriesRoot)
	n := 0
	for _, root := range out {
		if n > 0 && out[n-1].Prefix == root.Prefix {
			if out[n-1].ManifestDigest != root.ManifestDigest {
				return nil, errors.Wrapf(ErrCorrupt, "conflicting series root %q", root.Prefix)
			}
			continue
		}
		out[n] = root
		n++
	}
	return out[:n], nil
}

func compareSeriesRoot(a, b SeriesRoot) int { return strings.Compare(a.Prefix, b.Prefix) }

func validateSeriesRoot(root SeriesRoot) error {
	if strings.ContainsAny(root.Prefix, "\\\x00") {
		return errors.Wrap(ErrCorrupt, "invalid series root prefix")
	}
	for segment := range strings.SplitSeq(root.Prefix, "/") {
		if segment == "" || segment == "." || segment == ".." {
			return errors.Wrap(ErrCorrupt, "invalid series root prefix")
		}
	}
	if root.ManifestDigest == ([32]byte{}) {
		return errors.Wrap(ErrCorrupt, "empty series root manifest digest")
	}
	return nil
}

func validateSeriesRoots(roots []SeriesRoot) error {
	for i, root := range roots {
		if err := validateSeriesRoot(root); err != nil {
			return err
		}
		if i > 0 && compareSeriesRoot(roots[i-1], root) >= 0 {
			return errors.Wrap(ErrCorrupt, "series roots are not strictly sorted")
		}
	}
	return nil
}

func appendSeriesRoots(dst []byte, roots []SeriesRoot) []byte {
	dst = binary.AppendUvarint(dst, uint64(len(roots)))
	for _, root := range roots {
		dst = binary.AppendUvarint(dst, uint64(len(root.Prefix)))
		dst = append(dst, root.Prefix...)
		dst = append(dst, root.ManifestDigest[:]...)
	}
	return dst
}

func decodeSeriesRoots(buf []byte) ([]SeriesRoot, []byte, error) {
	n, buf, ok := readUvarint(buf)
	if !ok || n > uint64(len(buf)/34) {
		return nil, nil, errors.Wrap(ErrCorrupt, "bad series root count")
	}
	if n == 0 {
		return nil, buf, nil
	}
	roots := make([]SeriesRoot, 0, n)
	for range n {
		size, rest, ok := readUvarint(buf)
		if !ok || size == 0 || size > uint64(len(rest)) || len(rest)-int(size) < 32 {
			return nil, nil, errors.Wrap(ErrCorrupt, "bad series root prefix length")
		}
		root := SeriesRoot{Prefix: string(rest[:size])}
		copy(root.ManifestDigest[:], rest[size:size+32])
		roots = append(roots, root)
		buf = rest[size+32:]
	}
	if err := validateSeriesRoots(roots); err != nil {
		return nil, nil, err
	}
	return roots, buf, nil
}
