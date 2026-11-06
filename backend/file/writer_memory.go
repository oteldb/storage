package file

import "math"

// WriterMemory bounds the fixed buffer, file metadata and retained path storage.
func (f *File) WriterMemory(key string) (int64, bool) {
	return objectWriterMemory(int64(len(f.dir)), int64(len(key))), true
}

func objectWriterMemory(dir, key int64) int64 {
	const base = objectWriteBufferBytes + 4<<10
	pathBytes := dir + key
	if pathBytes < 0 || pathBytes > (math.MaxInt64-base)/8 {
		return math.MaxInt64
	}
	return base + pathBytes*8
}
