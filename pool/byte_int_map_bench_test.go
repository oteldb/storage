package pool

import "testing"

func benchmarkByteIntKeys(rows, card int) [][]byte {
	keys := make([][]byte, rows)
	for i := range keys {
		keys[i] = []byte("key-" + itoa(i%card))
	}
	return keys
}

func BenchmarkByteIntMapPutOrGetLowCard(b *testing.B) {
	keys := benchmarkByteIntKeys(1000, 10)
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		m := NewByteIntMap()
		for i, key := range keys {
			m.PutOrGet(key, i)
		}
		m.PutBack()
	}
}

func BenchmarkByteIntMapPutOrGetHighCard(b *testing.B) {
	keys := benchmarkByteIntKeys(1000, 1000)
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		m := NewByteIntMap()
		for i, key := range keys {
			m.PutOrGet(key, i)
		}
		m.PutBack()
	}
}

func BenchmarkByteIntMapGet(b *testing.B) {
	keys := benchmarkByteIntKeys(1000, 1000)
	m := NewByteIntMap()
	defer m.PutBack()
	for i, key := range keys {
		m.Put(key, i)
	}

	b.ReportAllocs()
	b.ResetTimer()
	for i := range b.N {
		_, _ = m.Get(keys[i%len(keys)])
	}
}

func BenchmarkByteIntMapReset(b *testing.B) {
	keys := benchmarkByteIntKeys(1000, 1000)
	m := NewByteIntMap()
	for i, key := range keys {
		m.Put(key, i)
	}

	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		m.Reset()
	}
	b.StopTimer()
	m.PutBack()
}
