// Package wal implements AnchorDB's write-ahead log: segment files, the
// Commit record payload, and the segment scanner used by recovery.
package wal

import (
	"bytes"
	"encoding/binary"
	"fmt"
)

// Record types stored in WAL segments.
const RecordCommit byte = 1

// Limits shared with the engine (docs/architecture.md §2.1).
const (
	MaxKeySize   = 64 << 10
	MaxValueSize = 16 << 20
	MaxTxBytes   = 64 << 20 // maximum encoded Commit payload
)

const (
	opPut    byte = 1
	opDelete byte = 2
)

// Op is one write inside a committed transaction.
type Op struct {
	Delete bool
	Key    []byte
	Value  []byte // nil for deletes
}

// Commit is the payload of one WAL record: everything a transaction wrote.
// The whole transaction lives in one checksummed record, which is what makes
// commits atomic: recovery sees all of it or none of it.
type Commit struct {
	TxID uint64
	Ops  []Op // strictly ascending by key
}

// CommitHeaderSize is the fixed part of the payload (txid + opCount).
const CommitHeaderSize = 12

// OpSize returns the encoded size of one op; the engine uses it to enforce
// MaxTxBytes while the transaction is still being built.
func OpSize(del bool, keyLen, valLen int) int {
	if del {
		return 1 + 4 + keyLen
	}
	return 1 + 4 + keyLen + 4 + valLen
}

// EncodeCommit serializes c. Layout:
//
//	u64 txid | u32 opCount | opCount × (u8 op | u32 keyLen | key | [u32 valLen | value])
func EncodeCommit(c Commit) []byte {
	size := CommitHeaderSize
	for _, op := range c.Ops {
		size += OpSize(op.Delete, len(op.Key), len(op.Value))
	}
	b := make([]byte, 0, size)
	b = binary.LittleEndian.AppendUint64(b, c.TxID)
	b = binary.LittleEndian.AppendUint32(b, uint32(len(c.Ops)))
	for _, op := range c.Ops {
		if op.Delete {
			b = append(b, opDelete)
		} else {
			b = append(b, opPut)
		}
		b = binary.LittleEndian.AppendUint32(b, uint32(len(op.Key)))
		b = append(b, op.Key...)
		if !op.Delete {
			b = binary.LittleEndian.AppendUint32(b, uint32(len(op.Value)))
			b = append(b, op.Value...)
		}
	}
	return b
}

// DecodeCommit parses a Commit payload. Keys and values alias p. Every length
// is checked against the bytes that remain before anything is sliced or
// allocated, so a hostile payload cannot make the decoder allocate more than
// O(len(p)).
func DecodeCommit(p []byte) (Commit, error) {
	if len(p) < CommitHeaderSize {
		return Commit{}, fmt.Errorf("commit payload too short (%d bytes)", len(p))
	}
	c := Commit{TxID: binary.LittleEndian.Uint64(p[0:8])}
	count := binary.LittleEndian.Uint32(p[8:12])
	p = p[CommitHeaderSize:]
	if count == 0 {
		return Commit{}, fmt.Errorf("commit has zero ops")
	}
	// Smallest op: 1 (op) + 4 (keyLen) + 1 (key) = 6 bytes.
	if uint64(count) > uint64(len(p))/6 {
		return Commit{}, fmt.Errorf("op count %d impossible for %d payload bytes", count, len(p))
	}
	c.Ops = make([]Op, 0, count)
	var prev []byte
	for i := uint32(0); i < count; i++ {
		if len(p) < 5 {
			return Commit{}, fmt.Errorf("op %d: truncated", i)
		}
		code := p[0]
		if code != opPut && code != opDelete {
			return Commit{}, fmt.Errorf("op %d: unknown op code %d", i, code)
		}
		klen := binary.LittleEndian.Uint32(p[1:5])
		p = p[5:]
		if klen == 0 || klen > MaxKeySize || uint64(klen) > uint64(len(p)) {
			return Commit{}, fmt.Errorf("op %d: bad key length %d", i, klen)
		}
		op := Op{Delete: code == opDelete, Key: p[:klen:klen]}
		p = p[klen:]
		if prev != nil && bytes.Compare(prev, op.Key) >= 0 {
			return Commit{}, fmt.Errorf("op %d: keys not strictly ascending", i)
		}
		prev = op.Key
		if !op.Delete {
			if len(p) < 4 {
				return Commit{}, fmt.Errorf("op %d: truncated value length", i)
			}
			vlen := binary.LittleEndian.Uint32(p[0:4])
			p = p[4:]
			if vlen > MaxValueSize || uint64(vlen) > uint64(len(p)) {
				return Commit{}, fmt.Errorf("op %d: bad value length %d", i, vlen)
			}
			op.Value = p[:vlen:vlen]
			p = p[vlen:]
		}
		c.Ops = append(c.Ops, op)
	}
	if len(p) != 0 {
		return Commit{}, fmt.Errorf("%d trailing bytes after ops", len(p))
	}
	return c, nil
}
