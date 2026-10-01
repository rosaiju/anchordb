package refmodel

// Reference encoders/decoders for the on-disk formats, written directly from
// docs/architecture.md §4 without reusing any engine package. Tests use them
// to cross-check the engine's codecs and to craft or inspect files.

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
)

var castagnoli = crc32.MakeTable(crc32.Castagnoli)

// CRC is CRC-32C.
func CRC(b []byte) uint32 { return crc32.Checksum(b, castagnoli) }

// Spec constants (§2.1, §4).
const (
	FrameHeaderSize   = 16
	FileHeaderSize    = 32
	MaxKeySize        = 64 << 10
	MaxValueSize      = 16 << 20
	MaxTxBytes        = 64 << 20
	MaxPayload        = 64<<20 + 1<<10
	TypeCommit   byte = 1
	TypeEntries  byte = 2
	TypeTrailer  byte = 3
)

// SegmentName / CheckpointName follow §4.
func SegmentName(seq uint64) string     { return fmt.Sprintf("wal-%016d.log", seq) }
func CheckpointName(txid uint64) string { return fmt.Sprintf("checkpoint-%020d.ckpt", txid) }

// Frame encodes one record frame (§4.1).
func Frame(typ byte, payload []byte) []byte {
	b := make([]byte, FrameHeaderSize+len(payload))
	binary.LittleEndian.PutUint32(b[0:4], uint32(len(payload)))
	binary.LittleEndian.PutUint32(b[4:8], CRC(b[0:4]))
	b[12] = typ
	copy(b[16:], payload)
	c := crc32.Update(0, castagnoli, b[12:13])
	c = crc32.Update(c, castagnoli, payload)
	binary.LittleEndian.PutUint32(b[8:12], c)
	return b
}

// Frame classification results, mirroring §4.1's error table.
var (
	ErrIncomplete  = errors.New("ref: incomplete")
	ErrBadHeader   = errors.New("ref: bad header")
	ErrTooLarge    = errors.New("ref: too large")
	ErrBadChecksum = errors.New("ref: bad checksum")
)

// DecodeFrame is an independent implementation of record.Decode.
func DecodeFrame(buf []byte) (typ byte, payload []byte, n int, err error) {
	if len(buf) < 16 {
		return 0, nil, 0, ErrIncomplete
	}
	if binary.LittleEndian.Uint32(buf[4:8]) != CRC(buf[0:4]) || buf[13]|buf[14]|buf[15] != 0 {
		return 0, nil, 0, ErrBadHeader
	}
	N := binary.LittleEndian.Uint32(buf[0:4])
	if N > MaxPayload {
		return 0, nil, 0, ErrTooLarge
	}
	if uint64(len(buf)) < 16+uint64(N) {
		return 0, nil, 0, ErrIncomplete
	}
	c := crc32.Update(0, castagnoli, buf[12:13])
	c = crc32.Update(c, castagnoli, buf[16:16+N])
	if c != binary.LittleEndian.Uint32(buf[8:12]) {
		return 0, nil, 0, ErrBadChecksum
	}
	return buf[12], buf[16 : 16+N], int(16 + N), nil
}

func fileHeader(magic string, num uint64) []byte {
	b := make([]byte, 32)
	copy(b, magic)
	binary.LittleEndian.PutUint32(b[8:12], 1)
	binary.LittleEndian.PutUint64(b[16:24], num)
	binary.LittleEndian.PutUint32(b[24:28], CRC(b[0:24]))
	return b
}

// SegmentHeader encodes a WAL segment header (§4.2).
func SegmentHeader(seq uint64) []byte { return fileHeader("ANCHWAL1", seq) }

// CheckpointHeader encodes a checkpoint file header (§4.3).
func CheckpointHeader(txid uint64) []byte { return fileHeader("ANCHCKP1", txid) }

// CommitPayload encodes a Commit payload (§4.2). Ops are written as given.
func CommitPayload(txid uint64, ops []Op) []byte {
	var b []byte
	b = binary.LittleEndian.AppendUint64(b, txid)
	b = binary.LittleEndian.AppendUint32(b, uint32(len(ops)))
	for _, op := range ops {
		if op.Delete {
			b = append(b, 2)
		} else {
			b = append(b, 1)
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

// CommitPayloadSize is 12 + Σ opSize (§2.1).
func CommitPayloadSize(ops []Op) int {
	n := 12
	for _, op := range ops {
		if op.Delete {
			n += 5 + len(op.Key)
		} else {
			n += 9 + len(op.Key) + len(op.Value)
		}
	}
	return n
}

// ErrBadPayload is returned by the reference payload decoders.
var ErrBadPayload = errors.New("ref: bad payload")

// ParseCommit is an independent strict decoder for a Commit payload.
func ParseCommit(p []byte) (uint64, []Op, error) {
	if len(p) < 12 {
		return 0, nil, ErrBadPayload
	}
	txid := binary.LittleEndian.Uint64(p)
	cnt := binary.LittleEndian.Uint32(p[8:])
	if cnt == 0 {
		return 0, nil, ErrBadPayload
	}
	r := p[12:]
	var ops []Op
	var prev []byte
	for i := uint32(0); i < cnt; i++ {
		if len(r) < 5 {
			return 0, nil, ErrBadPayload
		}
		code := r[0]
		if code != 1 && code != 2 {
			return 0, nil, ErrBadPayload
		}
		kl := binary.LittleEndian.Uint32(r[1:5])
		r = r[5:]
		if kl < 1 || kl > MaxKeySize || uint64(len(r)) < uint64(kl) {
			return 0, nil, ErrBadPayload
		}
		k := r[:kl]
		r = r[kl:]
		if prev != nil && bytes.Compare(prev, k) >= 0 {
			return 0, nil, ErrBadPayload
		}
		prev = k
		op := Op{Delete: code == 2, Key: k}
		if code == 1 {
			if len(r) < 4 {
				return 0, nil, ErrBadPayload
			}
			vl := binary.LittleEndian.Uint32(r)
			r = r[4:]
			if vl > MaxValueSize || uint64(len(r)) < uint64(vl) {
				return 0, nil, ErrBadPayload
			}
			op.Value = r[:vl]
			r = r[vl:]
		}
		ops = append(ops, op)
	}
	if len(r) != 0 {
		return 0, nil, ErrBadPayload
	}
	return txid, ops, nil
}

// EntriesPayload encodes a checkpoint Entries payload (§4.3).
func EntriesPayload(kvs []KV) []byte {
	var b []byte
	b = binary.LittleEndian.AppendUint32(b, uint32(len(kvs)))
	for _, kv := range kvs {
		b = binary.LittleEndian.AppendUint32(b, uint32(len(kv.Key)))
		b = append(b, kv.Key...)
		b = binary.LittleEndian.AppendUint32(b, uint32(len(kv.Value)))
		b = append(b, kv.Value...)
	}
	return b
}

// ParseEntries is an independent strict decoder for an Entries payload.
func ParseEntries(p []byte) ([]KV, error) {
	if len(p) < 4 {
		return nil, ErrBadPayload
	}
	cnt := binary.LittleEndian.Uint32(p)
	if cnt == 0 {
		return nil, ErrBadPayload
	}
	r := p[4:]
	var out []KV
	var prev []byte
	for i := uint32(0); i < cnt; i++ {
		if len(r) < 4 {
			return nil, ErrBadPayload
		}
		kl := binary.LittleEndian.Uint32(r)
		r = r[4:]
		if kl < 1 || kl > MaxKeySize || uint64(len(r)) < uint64(kl) {
			return nil, ErrBadPayload
		}
		k := r[:kl]
		r = r[kl:]
		if prev != nil && bytes.Compare(prev, k) >= 0 {
			return nil, ErrBadPayload
		}
		prev = k
		if len(r) < 4 {
			return nil, ErrBadPayload
		}
		vl := binary.LittleEndian.Uint32(r)
		r = r[4:]
		if vl > MaxValueSize || uint64(len(r)) < uint64(vl) {
			return nil, ErrBadPayload
		}
		out = append(out, KV{Key: k, Value: r[:vl]})
		r = r[vl:]
	}
	if len(r) != 0 {
		return nil, ErrBadPayload
	}
	return out, nil
}

// TrailerPayload encodes a checkpoint Trailer payload.
func TrailerPayload(txid, total uint64) []byte {
	var b []byte
	b = binary.LittleEndian.AppendUint64(b, txid)
	return binary.LittleEndian.AppendUint64(b, total)
}

// CheckpointFile builds a complete checkpoint file image. kvs must be sorted.
// perFrame bounds entries per Entries frame (<=0 means all in one frame).
func CheckpointFile(txid uint64, kvs []KV, perFrame int) []byte {
	b := CheckpointHeader(txid)
	if perFrame <= 0 {
		perFrame = len(kvs) + 1
	}
	for i := 0; i < len(kvs); i += perFrame {
		j := i + perFrame
		if j > len(kvs) {
			j = len(kvs)
		}
		b = append(b, Frame(TypeEntries, EntriesPayload(kvs[i:j]))...)
	}
	return append(b, Frame(TypeTrailer, TrailerPayload(txid, uint64(len(kvs))))...)
}

// CurrentFile encodes CURRENT (§4.4). ckpt == "" means "none".
func CurrentFile(ckpt string, txid, walStart uint64) []byte {
	if ckpt == "" {
		ckpt = "none"
	}
	body := fmt.Sprintf("ANCHORDB-CURRENT 1\ncheckpoint %s\ncheckpoint_txid %d\nwal_start %d\n", ckpt, txid, walStart)
	return []byte(fmt.Sprintf("%scrc32c %08x\n", body, CRC([]byte(body))))
}

// SegRecord describes one valid record found in a segment image.
type SegRecord struct {
	Offset int // frame start
	Len    int // whole frame length
	TxID   uint64
	Ops    []Op
}

// ParseSegment walks a segment image and returns its valid Commit records and
// the error (if any) that stopped the walk, with the offset where it stopped.
func ParseSegment(img []byte) (recs []SegRecord, stopOff int, stopErr error) {
	if len(img) < 32 {
		return nil, 0, ErrIncomplete
	}
	off := 32
	for off < len(img) {
		typ, p, n, err := DecodeFrame(img[off:])
		if err != nil {
			return recs, off, err
		}
		if typ != TypeCommit {
			return recs, off, ErrBadPayload
		}
		txid, ops, err := ParseCommit(p)
		if err != nil {
			return recs, off, err
		}
		recs = append(recs, SegRecord{Offset: off, Len: n, TxID: txid, Ops: ops})
		off += n
	}
	return recs, off, nil
}
