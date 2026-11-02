package reliable

import "math/bits"

// Bitmap operations work per 64-byte span, not per byte. Bounds are validated
// before entering these helpers. No unsafe code and no allocations.
func wordMask(offset, count int) uint64 {
	if count == 64 {
		return ^uint64(0)
	}
	return ((uint64(1) << uint(count)) - 1) << uint(offset)
}
func markBits(bitmap []uint64, start, end int) int {
	added := 0
	for start < end {
		limit := min(end, (start/64+1)*64)
		mask := wordMask(start%64, limit-start)
		added += bits.OnesCount64(mask &^ bitmap[start/64])
		bitmap[start/64] |= mask
		start = limit
	}
	return added
}
func firstMissing(bitmap []uint64, start, end int) int {
	for start < end {
		limit := min(end, (start/64+1)*64)
		missing := wordMask(start%64, limit-start) &^ bitmap[start/64]
		if missing != 0 {
			return start/64*64 + bits.TrailingZeros64(missing)
		}
		start = limit
	}
	return end
}
