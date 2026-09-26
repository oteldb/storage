package block

// arenaChunkBytes is the size of a [byteArena] chunk; a longer value gets a chunk of its own.
const arenaChunkBytes = 64 << 10

// byteArena copies values into chunks that never reallocate, so a copy stays valid — and keeps
// only its own chunk alive — while the arena grows. A single growing buffer would strand every
// earlier copy's backing array, holding the arena twice over.
type byteArena struct {
	chunks [][]byte
	cur    int
}

func (a *byteArena) copy(v []byte) []byte {
	n := len(v)
	if n == 0 {
		return nil
	}

	for ; a.cur < len(a.chunks); a.cur++ {
		c := a.chunks[a.cur]
		if cap(c)-len(c) >= n {
			off := len(c)
			a.chunks[a.cur] = append(c, v...)

			return a.chunks[a.cur][off : off+n : off+n]
		}
	}

	c := append(make([]byte, 0, max(arenaChunkBytes, n)), v...)
	a.chunks = append(a.chunks, c)
	a.cur = len(a.chunks) - 1

	return c[:n:n]
}

// reset empties the arena for reuse, dropping any chunk a long value made oversized.
func (a *byteArena) reset() {
	kept := a.chunks[:0]

	for _, c := range a.chunks {
		if cap(c) == arenaChunkBytes {
			kept = append(kept, c[:0])
		}
	}

	clear(a.chunks[len(kept):])
	a.chunks, a.cur = kept, 0
}

func (a *byteArena) release() { a.chunks, a.cur = nil, 0 }

func (a *byteArena) size() int64 {
	var n int64
	for _, c := range a.chunks {
		n += int64(cap(c))
	}

	return n + int64(cap(a.chunks))*24
}
