// SPDX-License-Identifier: Apache-2.0
package snapshot

import "math/bits"

type NodeID = uint32
type KindID = int16

// Bitset is a dense bit vector backed by uint64 words.
type Bitset struct {
	words []uint64
	count int
}

// NewBitset creates a bitset for up to n bits (indexed 0 to n-1).
func NewBitset(n int) *Bitset {
	numWords := (n + 63) / 64
	return &Bitset{
		words: make([]uint64, numWords),
		count: 0,
	}
}

// Set marks the bit at index i as set. Does nothing if i is out of bounds.
func (b *Bitset) Set(i NodeID) {
	wordIdx := i / 64
	if wordIdx >= uint32(len(b.words)) {
		return
	}
	bitIdx := i % 64
	mask := uint64(1) << bitIdx
	if (b.words[wordIdx] & mask) == 0 {
		b.count++
	}
	b.words[wordIdx] |= mask
}

// Has returns true if the bit at index i is set, false otherwise.
func (b *Bitset) Has(i NodeID) bool {
	wordIdx := i / 64
	if wordIdx >= uint32(len(b.words)) {
		return false
	}
	bitIdx := i % 64
	return (b.words[wordIdx] & (uint64(1) << bitIdx)) != 0
}

// Count returns the number of set bits.
func (b *Bitset) Count() int {
	return b.count
}

// Iterate calls fn for each set bit in ascending order. Stops if fn returns false.
func (b *Bitset) Iterate(fn func(NodeID) bool) {
	for wordIdx := 0; wordIdx < len(b.words); wordIdx++ {
		word := b.words[wordIdx]
		for word != 0 {
			tz := bits.TrailingZeros64(word)
			idx := NodeID(wordIdx*64 + tz)
			if !fn(idx) {
				return
			}
			word &= ^(uint64(1) << uint(tz))
		}
	}
}

// KindMask is a dense bit vector for KindIDs from 0 to max (inclusive).
type KindMask struct {
	words []uint64
	max   KindID
}

// NewKindMask creates a mask for kind IDs from 0 to max (inclusive).
func NewKindMask(max KindID) *KindMask {
	numWords := (int(max) + 1 + 63) / 64
	return &KindMask{
		words: make([]uint64, numWords),
		max:   max,
	}
}

// Set marks the kind as set. Does nothing if k < 0 or k > max.
func (m *KindMask) Set(k KindID) {
	if k < 0 || k > m.max {
		return
	}
	wordIdx := int(k) / 64
	bitIdx := int(k) % 64
	m.words[wordIdx] |= uint64(1) << bitIdx
}

// Has returns true if the kind is set, false if k < 0, k > max, or the kind is not set.
func (m *KindMask) Has(k KindID) bool {
	if k < 0 || k > m.max {
		return false
	}
	wordIdx := int(k) / 64
	bitIdx := int(k) % 64
	return (m.words[wordIdx] & (uint64(1) << bitIdx)) != 0
}

// SetAll sets all kinds from 0 to max (inclusive).
func (m *KindMask) SetAll() {
	for i := 0; i < len(m.words); i++ {
		m.words[i] = ^uint64(0)
	}
	// Mask off bits beyond max in the last word.
	if len(m.words) > 0 {
		lastWordBits := (int(m.max) % 64) + 1
		if lastWordBits < 64 {
			m.words[len(m.words)-1] &= (uint64(1) << uint(lastWordBits)) - 1
		}
	}
}
