package backend

// WriterMemorySizer bounds an object's incremental writer workspace without opening it.
type WriterMemorySizer interface {
	WriterMemory(key string) (bytes int64, known bool)
}

// WriterMemory returns a trustworthy writer-workspace bound, or false when unavailable.
func WriterMemory(b Backend, key string) (int64, bool) {
	if s, ok := b.(WriterMemorySizer); ok {
		if n, known := s.WriterMemory(key); known && n >= 0 {
			return n, true
		}
	}
	return 0, false
}
