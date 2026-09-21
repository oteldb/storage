package mergestream

// Budget is the pair of limits a merge seals an output part on. The units are not interchangeable
// and neither bounds the other: DiskBytes is the size the part reaches encoded on the backend,
// ResidentBytes what the merge holds in RAM while building it — per-key state that grows with
// distinct keys rather than with encoded bytes. A limit of zero or less does not bound the merge.
type Budget struct {
	DiskBytes     int64
	ResidentBytes int64
}

// Reached reports whether an output part of the given encoded and resident size has met a limit.
func (b Budget) Reached(disk, resident int64) bool {
	return (b.DiskBytes > 0 && disk >= b.DiskBytes) ||
		(b.ResidentBytes > 0 && resident >= b.ResidentBytes)
}

// Bounded reports whether the budget seals at all.
func (b Budget) Bounded() bool { return b.DiskBytes > 0 || b.ResidentBytes > 0 }
