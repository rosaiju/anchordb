// Package refmodel is an independent, deliberately naive reference model of
// AnchorDB used only by tests. It is written from docs/architecture.md, not
// from the engine's code, and favours obviousness over speed: the state is a
// plain map, ordering is done by sorting on every scan.
//
// It also contains reference encoders for the on-disk formats (format.go), so
// tests can build and inspect files without trusting the engine's codecs.
package refmodel

import (
	"bytes"
	"sort"
)

// Op is one buffered write. Delete ops have a nil Value.
type Op struct {
	Delete bool
	Key    []byte
	Value  []byte
}

// Model is the committed key/value state plus a transaction counter.
type Model struct {
	m        map[string][]byte
	LastTxID uint64
}

// New returns an empty model.
func New() *Model { return &Model{m: map[string][]byte{}} }

// Clone returns a deep copy.
func (md *Model) Clone() *Model {
	c := New()
	c.LastTxID = md.LastTxID
	for k, v := range md.m {
		c.m[k] = append([]byte{}, v...)
	}
	return c
}

// Len is the number of live keys.
func (md *Model) Len() int { return len(md.m) }

// Get returns the value and whether the key exists.
func (md *Model) Get(key []byte) ([]byte, bool) {
	v, ok := md.m[string(key)]
	if !ok {
		return nil, false
	}
	return append([]byte{}, v...), true
}

// Put sets key to value (outside any transaction; does not touch LastTxID).
func (md *Model) Put(key, value []byte) { md.m[string(key)] = append([]byte{}, value...) }

// Delete removes key (outside any transaction).
func (md *Model) Delete(key []byte) { delete(md.m, string(key)) }

// KV is one key/value pair.
type KV struct{ Key, Value []byte }

// Scan returns all pairs with start <= k < end in ascending order, with the
// nil/empty-bound semantics of spec §2.3.
func (md *Model) Scan(start, end []byte) []KV {
	return ScanMap(md.m, start, end)
}

// All returns every pair in ascending order.
func (md *Model) All() []KV { return md.Scan(nil, nil) }

// ScanMap is Scan over an arbitrary map.
func ScanMap(m map[string][]byte, start, end []byte) []KV {
	keys := make([]string, 0, len(m))
	for k := range m {
		kb := []byte(k)
		if len(start) > 0 && bytes.Compare(kb, start) < 0 {
			continue
		}
		if len(end) > 0 && bytes.Compare(kb, end) >= 0 {
			continue
		}
		keys = append(keys, k)
	}
	sort.Strings(keys) // Go string order == bytes.Compare order
	out := make([]KV, len(keys))
	for i, k := range keys {
		out[i] = KV{Key: []byte(k), Value: append([]byte{}, m[k]...)}
	}
	return out
}

// Apply commits a transaction's ops atomically. Per spec §2.2 rule 9 an
// empty op list consumes no txid; any non-empty list consumes exactly one,
// even if its net effect is nil.
func (md *Model) Apply(ops []Op) {
	if len(ops) == 0 {
		return
	}
	for _, op := range ops {
		if op.Delete {
			delete(md.m, string(op.Key))
		} else {
			md.m[string(op.Key)] = append([]byte{}, op.Value...)
		}
	}
	md.LastTxID++
}

// Equal reports whether the model's state equals the given ordered pairs.
func (md *Model) Equal(kvs []KV) bool {
	all := md.All()
	if len(all) != len(kvs) {
		return false
	}
	for i := range all {
		if !bytes.Equal(all[i].Key, kvs[i].Key) || !bytes.Equal(all[i].Value, kvs[i].Value) {
			return false
		}
	}
	return true
}

// Tx is a transaction against the model with read-your-writes semantics.
type Tx struct {
	md  *Model
	buf map[string]*Op
	ops []Op // in call order; collapsed at commit
}

// Begin starts a transaction.
func (md *Model) Begin() *Tx { return &Tx{md: md, buf: map[string]*Op{}} }

// Put buffers a put.
func (t *Tx) Put(k, v []byte) {
	t.buf[string(k)] = &Op{Key: append([]byte{}, k...), Value: append([]byte{}, v...)}
	t.ops = append(t.ops, Op{Key: k, Value: v})
}

// Delete buffers a delete.
func (t *Tx) Delete(k []byte) {
	t.buf[string(k)] = &Op{Delete: true, Key: append([]byte{}, k...)}
	t.ops = append(t.ops, Op{Delete: true, Key: k})
}

// Get reads through the buffer.
func (t *Tx) Get(k []byte) ([]byte, bool) {
	if op, ok := t.buf[string(k)]; ok {
		if op.Delete {
			return nil, false
		}
		return append([]byte{}, op.Value...), true
	}
	return t.md.Get(k)
}

// Scan reads through the buffer.
func (t *Tx) Scan(start, end []byte) []KV {
	m := make(map[string][]byte, len(t.md.m))
	for k, v := range t.md.m {
		m[k] = v
	}
	for k, op := range t.buf {
		if op.Delete {
			delete(m, k)
		} else {
			m[k] = op.Value
		}
	}
	return ScanMap(m, start, end)
}

// Ops returns the collapsed, key-ascending op list (last write wins).
func (t *Tx) Ops() []Op {
	keys := make([]string, 0, len(t.buf))
	for k := range t.buf {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	out := make([]Op, len(keys))
	for i, k := range keys {
		out[i] = *t.buf[k]
	}
	return out
}

// Commit applies the buffer to the model.
func (t *Tx) Commit() { t.md.Apply(t.Ops()) }
