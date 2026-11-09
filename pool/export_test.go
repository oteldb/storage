package pool

import "sync"

func ReplaceByteIntMapPoolForTest(factory func() *ByteIntMap) func() {
	prior := byteIntMapPool
	byteIntMapPool = &sync.Pool{New: func() any { return factory() }}
	return func() { byteIntMapPool = prior }
}
