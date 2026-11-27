package seriestable

import "math/bits"

// idRoutingFilter is a blocked Bloom filter over the low word of validated series IDs.
// A negative probe needs one word load. Hits still require the full ID and canonical descriptor.
type idRoutingFilter struct{ words []uint64 }

func routingFilterWords(count uint32) int {
	const bitsPerID = 12
	return int((uint64(count)*bitsPerID + 63) / 64)
}

func routingBitmask(key uint64) uint64 {
	return 1<<(key&63) | 1<<((key>>6)&63) | 1<<((key>>12)&63) | 1<<((key>>18)&63)
}

func (f *idRoutingFilter) add(key uint64) {
	word, _ := bits.Mul64(key, uint64(len(f.words)))
	f.words[word] |= routingBitmask(key)
}

func (f *idRoutingFilter) mayContain(key uint64) bool {
	word, _ := bits.Mul64(key, uint64(len(f.words)))
	mask := routingBitmask(key)
	return f.words[word]&mask == mask
}
