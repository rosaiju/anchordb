// Package index implements the ordered in-memory index used by AnchorDB: a
// skip list keyed by byte strings and ordered by bytes.Compare.
//
// Algorithm: W. Pugh, "Skip Lists: A Probabilistic Alternative to Balanced
// Trees", CACM 33(6), 1990. Each node gets a random height; a node of height h
// is linked into the h lowest levels. Searching starts at the highest level
// and drops down a level whenever the next node would overshoot the target.
// With promotion probability 1/4, the expected search cost is O(log n).
//
// A SkipList is NOT safe for concurrent mutation. AnchorDB protects it with the
// database lock: many goroutines may read concurrently, but only while no
// goroutine is writing.
package index

import (
	"bytes"
	"math/rand/v2"
)

const (
	maxLevel = 24 // supports ~4^24 (2.8e14) keys at p = 1/4
	pBits    = 2  // promote with probability 1/2^pBits = 1/4
)

type node struct {
	key   []byte
	value []byte
	next  []*node // next[i] is the successor at level i
}

// SkipList is an ordered map from []byte to []byte.
type SkipList struct {
	head  *node // sentinel; its key is never compared
	level int   // number of levels currently in use (>= 1)
	n     int
	rng   *rand.Rand
}

// New returns an empty skip list. The seed makes node heights, and therefore
// performance, reproducible; it never affects results.
func New(seed uint64) *SkipList {
	return &SkipList{
		head:  &node{next: make([]*node, maxLevel)},
		level: 1,
		rng:   rand.New(rand.NewPCG(seed, 0x9e3779b97f4a7c15)),
	}
}

// Len returns the number of keys.
func (s *SkipList) Len() int { return s.n }

func (s *SkipList) randomLevel() int {
	lvl := 1
	for lvl < maxLevel && s.rng.Uint32()&(1<<pBits-1) == 0 {
		lvl++
	}
	return lvl
}

// findGE returns the first node with key >= key. If prev is non-nil it is
// filled with the rightmost node before that position on every level, which is
// exactly where an insert or delete must splice.
func (s *SkipList) findGE(key []byte, prev []*node) *node {
	x := s.head
	for i := s.level - 1; i >= 0; i-- {
		for x.next[i] != nil && bytes.Compare(x.next[i].key, key) < 0 {
			x = x.next[i]
		}
		if prev != nil {
			prev[i] = x
		}
	}
	return x.next[0]
}

// Get returns the value for key and whether it exists.
func (s *SkipList) Get(key []byte) ([]byte, bool) {
	x := s.findGE(key, nil)
	if x != nil && bytes.Equal(x.key, key) {
		return x.value, true
	}
	return nil, false
}

// Set inserts or replaces key. The slices are stored as given; the caller is
// responsible for copying them if it may modify them later.
func (s *SkipList) Set(key, value []byte) {
	var prev [maxLevel]*node
	x := s.findGE(key, prev[:])
	if x != nil && bytes.Equal(x.key, key) {
		x.value = value
		return
	}
	lvl := s.randomLevel()
	if lvl > s.level {
		for i := s.level; i < lvl; i++ {
			prev[i] = s.head
		}
		s.level = lvl
	}
	nd := &node{key: key, value: value, next: make([]*node, lvl)}
	for i := 0; i < lvl; i++ {
		nd.next[i] = prev[i].next[i]
		prev[i].next[i] = nd
	}
	s.n++
}

// Delete removes key and reports whether it was present.
func (s *SkipList) Delete(key []byte) bool {
	var prev [maxLevel]*node
	x := s.findGE(key, prev[:])
	if x == nil || !bytes.Equal(x.key, key) {
		return false
	}
	for i := 0; i < len(x.next); i++ {
		prev[i].next[i] = x.next[i]
	}
	for s.level > 1 && s.head.next[s.level-1] == nil {
		s.level--
	}
	s.n--
	return true
}

// Iterator walks keys in ascending order. It is invalidated by any mutation
// of the skip list.
type Iterator struct{ cur *node }

// Seek returns an iterator positioned at the first key >= key. A nil or empty
// key positions it at the first key.
func (s *SkipList) Seek(key []byte) *Iterator {
	if len(key) == 0 {
		return &Iterator{cur: s.head.next[0]}
	}
	return &Iterator{cur: s.findGE(key, nil)}
}

// Valid reports whether the iterator is positioned at a key.
func (it *Iterator) Valid() bool { return it.cur != nil }

// Key returns the current key. Only call when Valid.
func (it *Iterator) Key() []byte { return it.cur.key }

// Value returns the current value. Only call when Valid.
func (it *Iterator) Value() []byte { return it.cur.value }

// Next advances to the following key.
func (it *Iterator) Next() { it.cur = it.cur.next[0] }
