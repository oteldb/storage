package storage

import "strings"

func backendWriteKind(key string) string {
	if strings.Contains(key, "/.postings/") {
		return "postings_spill"
	}

	dir, name := cutObjectPath(key)
	switch name {
	case "sidx":
		return "sample_index"
	case "smax":
		return "coverage"
	case "stats":
		return "aggregate_stats"
	case "series-index", "identity":
		return "descriptor_index"
	case "bucket-index.bin":
		return "catalog"
	case "manifest", "series-ref":
		return "manifest"
	case "marks":
		return "marks"
	}
	if prefix, ok := strings.CutSuffix(dir, "/c"); ok {
		_, part := cutObjectPath(prefix)
		if strings.HasPrefix(part, "s-") {
			return "descriptor_ids"
		}
		return "column"
	}
	return "other"
}

func cutObjectPath(key string) (dir, name string) {
	i := strings.LastIndexByte(key, '/')
	if i < 0 {
		return "", key
	}
	return key[:i], key[i+1:]
}
