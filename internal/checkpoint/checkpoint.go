// Package checkpoint writes and reads AnchorDB checkpoint files: a sorted,
// checksummed snapshot of every key/value pair as of one transaction id.
// Format: docs/architecture.md §4.3.
package checkpoint

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/rosaiju/anchordb/internal/record"
	"github.com/rosaiju/anchordb/internal/vfs"
	"github.com/rosaiju/anchordb/internal/wal"
)

const (
	HeaderSize = 32
	magic      = "ANCHCKP1"
	version    = 1

	RecordEntries byte = 2
	RecordTrailer byte = 3

	// TargetFrame is the payload size at which an Entries frame is flushed.
	TargetFrame = 1 << 20
)

// Name returns the file name of the checkpoint covering txid.
func Name(txid uint64) string { return fmt.Sprintf("checkpoint-%020d.ckpt", txid) }

// ParseName extracts the txid from a checkpoint file name.
func ParseName(name string) (uint64, bool) {
	const pre, suf = "checkpoint-", ".ckpt"
	if len(name) != len(pre)+20+len(suf) || !strings.HasPrefix(name, pre) || !strings.HasSuffix(name, suf) {
		return 0, false
	}
	txid, err := strconv.ParseUint(name[len(pre):len(pre)+20], 10, 64)
	return txid, err == nil
}

// Entry is one key/value pair.
type Entry struct{ Key, Value []byte }

func encodeHeader(txid uint64) []byte {
	h := make([]byte, HeaderSize)
	copy(h, magic)
	binary.LittleEndian.PutUint32(h[8:12], version)
	binary.LittleEndian.PutUint64(h[16:24], txid)
	binary.LittleEndian.PutUint32(h[24:28], record.Checksum(h[0:24]))
	return h
}

// Stage names points inside Write, for crash hooks.
type Stage int

const (
	StagePartial Stage = iota // first Entries frame written (or, if empty, before the trailer)
	StageSynced               // temp file complete and fsynced
	StageRenamed              // renamed to its final name
)

// Write creates Name(txid) in dir: entries go to a temp file, which is fsynced
// and then renamed into place, and the directory is synced. each must yield
// entries in strictly ascending key order. On error the temp file is removed.
// hook may be nil.
func Write(fsys vfs.FS, dir string, txid uint64, each func(yield func(k, v []byte) bool), hook func(Stage)) (err error) {
	final := filepath.Join(dir, Name(txid))
	tmp := final + ".tmp"
	f, err := fsys.OpenFile(tmp, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o644)
	if err != nil {
		return err
	}
	renamed := false
	defer func() {
		if err != nil && !renamed {
			f.Close()
			fsys.Remove(tmp)
		}
	}()
	if _, err = f.Write(encodeHeader(txid)); err != nil {
		return err
	}
	var (
		payload  []byte
		count    uint32
		total    uint64
		frame    []byte
		partial  bool
		writeErr error
	)
	flush := func() error {
		if count == 0 {
			return nil
		}
		binary.LittleEndian.PutUint32(payload[0:4], count)
		frame = record.Append(frame[:0], RecordEntries, payload)
		if _, err := f.Write(frame); err != nil {
			return err
		}
		if !partial && hook != nil {
			partial = true
			hook(StagePartial)
		}
		payload, count = payload[:0], 0
		return nil
	}
	each(func(k, v []byte) bool {
		if count == 0 {
			payload = append(payload[:0], 0, 0, 0, 0) // count placeholder
		}
		payload = binary.LittleEndian.AppendUint32(payload, uint32(len(k)))
		payload = append(payload, k...)
		payload = binary.LittleEndian.AppendUint32(payload, uint32(len(v)))
		payload = append(payload, v...)
		count++
		total++
		if len(payload) >= TargetFrame {
			if writeErr = flush(); writeErr != nil {
				return false
			}
		}
		return true
	})
	if writeErr != nil {
		return writeErr
	}
	if err = flush(); err != nil {
		return err
	}
	if !partial && hook != nil {
		hook(StagePartial)
	}
	var tr [16]byte
	binary.LittleEndian.PutUint64(tr[0:8], txid)
	binary.LittleEndian.PutUint64(tr[8:16], total)
	if _, err = f.Write(record.Append(nil, RecordTrailer, tr[:])); err != nil {
		return err
	}
	if err = f.Sync(); err != nil {
		return err
	}
	if err = f.Close(); err != nil {
		return err
	}
	if hook != nil {
		hook(StageSynced)
	}
	renamed = true // from here the temp file is closed; leave cleanup to Open
	if err = fsys.Rename(tmp, final); err != nil {
		return err
	}
	if hook != nil {
		hook(StageRenamed)
	}
	return fsys.SyncDir(dir)
}

// EncodeEntries builds an Entries payload; it is the inverse of DecodeEntries.
func EncodeEntries(es []Entry) []byte {
	b := binary.LittleEndian.AppendUint32(nil, uint32(len(es)))
	for _, e := range es {
		b = binary.LittleEndian.AppendUint32(b, uint32(len(e.Key)))
		b = append(b, e.Key...)
		b = binary.LittleEndian.AppendUint32(b, uint32(len(e.Value)))
		b = append(b, e.Value...)
	}
	return b
}

// DecodeEntries parses an Entries payload. Keys and values alias p; lengths
// are validated against the remaining bytes before slicing, so allocation is
// O(len(p)).
func DecodeEntries(p []byte) ([]Entry, error) {
	if len(p) < 4 {
		return nil, errors.New("entries payload too short")
	}
	count := binary.LittleEndian.Uint32(p[0:4])
	p = p[4:]
	if count == 0 {
		return nil, errors.New("entries frame with zero entries")
	}
	// Smallest entry: 4 + 1 (key) + 4 = 9 bytes.
	if uint64(count) > uint64(len(p))/9 {
		return nil, fmt.Errorf("entry count %d impossible for %d bytes", count, len(p))
	}
	out := make([]Entry, 0, count)
	for i := uint32(0); i < count; i++ {
		if len(p) < 4 {
			return nil, fmt.Errorf("entry %d: truncated", i)
		}
		klen := binary.LittleEndian.Uint32(p[0:4])
		p = p[4:]
		if klen == 0 || klen > wal.MaxKeySize || uint64(klen) > uint64(len(p)) {
			return nil, fmt.Errorf("entry %d: bad key length %d", i, klen)
		}
		k := p[:klen:klen]
		p = p[klen:]
		if len(p) < 4 {
			return nil, fmt.Errorf("entry %d: truncated value length", i)
		}
		vlen := binary.LittleEndian.Uint32(p[0:4])
		p = p[4:]
		if vlen > wal.MaxValueSize || uint64(vlen) > uint64(len(p)) {
			return nil, fmt.Errorf("entry %d: bad value length %d", i, vlen)
		}
		if len(out) > 0 && bytes.Compare(out[len(out)-1].Key, k) >= 0 {
			return nil, fmt.Errorf("entry %d: keys not strictly ascending", i)
		}
		out = append(out, Entry{Key: k, Value: p[:vlen:vlen]})
		p = p[vlen:]
	}
	if len(p) != 0 {
		return nil, fmt.Errorf("%d trailing bytes after entries", len(p))
	}
	return out, nil
}

// Read validates the checkpoint at path and calls visit for every entry in key
// order. The slices passed to visit are only valid during the call. Any
// deviation from the format is returned as a *wal.FormatError.
func Read(fsys vfs.FS, path string, wantTxID uint64, visit func(k, v []byte)) error {
	f, err := fsys.OpenFile(path, os.O_RDONLY, 0)
	if err != nil {
		return err
	}
	defer f.Close()
	st, err := f.Stat()
	if err != nil {
		return err
	}
	size := st.Size()
	bad := func(off int64, format string, args ...any) error {
		return &wal.FormatError{Offset: off, Reason: fmt.Sprintf(format, args...)}
	}
	r := bufio.NewReaderSize(f, 1<<16)
	hdr := make([]byte, HeaderSize)
	if _, err := io.ReadFull(r, hdr); err != nil {
		return bad(0, "checkpoint header truncated")
	}
	if string(hdr[0:8]) != magic || binary.LittleEndian.Uint32(hdr[24:28]) != record.Checksum(hdr[0:24]) {
		return bad(0, "bad checkpoint header")
	}
	if v := binary.LittleEndian.Uint32(hdr[8:12]); v != version {
		return bad(0, "unsupported checkpoint version %d", v)
	}
	if got := binary.LittleEndian.Uint64(hdr[16:24]); got != wantTxID {
		return bad(0, "checkpoint header txid %d, expected %d", got, wantTxID)
	}

	off := int64(HeaderSize)
	var (
		buf   []byte
		prev  []byte
		total uint64
	)
	for {
		if off == size {
			return bad(off, "checkpoint has no trailer")
		}
		if size-off < record.HeaderSize {
			return bad(off, "truncated frame header")
		}
		buf = append(buf[:0], make([]byte, record.HeaderSize)...)
		if _, err := io.ReadFull(r, buf); err != nil {
			return err
		}
		n, err := record.PayloadLen(buf)
		if err != nil {
			return bad(off, "%v", err)
		}
		if int64(n) > size-off-record.HeaderSize {
			return bad(off, "frame runs past end of file")
		}
		buf = append(buf, make([]byte, n)...)
		if _, err := io.ReadFull(r, buf[record.HeaderSize:]); err != nil {
			return err
		}
		fr, used, err := record.Decode(buf)
		if err != nil {
			return bad(off, "%v", err)
		}
		switch fr.Type {
		case RecordEntries:
			es, err := DecodeEntries(fr.Payload)
			if err != nil {
				return bad(off, "%v", err)
			}
			for _, e := range es {
				if prev != nil && bytes.Compare(prev, e.Key) >= 0 {
					return bad(off, "keys not strictly ascending")
				}
				prev = append(prev[:0], e.Key...)
				visit(e.Key, e.Value)
				total++
			}
		case RecordTrailer:
			if len(fr.Payload) != 16 {
				return bad(off, "bad trailer length")
			}
			if binary.LittleEndian.Uint64(fr.Payload[0:8]) != wantTxID {
				return bad(off, "trailer txid mismatch")
			}
			if c := binary.LittleEndian.Uint64(fr.Payload[8:16]); c != total {
				return bad(off, "trailer says %d entries, read %d", c, total)
			}
			if off+int64(used) != size {
				return bad(off+int64(used), "data after trailer")
			}
			return nil
		default:
			return bad(off, "unknown record type %d", fr.Type)
		}
		off += int64(used)
	}
}
