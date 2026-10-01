package wal

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/rosaiju/anchordb/internal/record"
	"github.com/rosaiju/anchordb/internal/vfs"
)

// Segment header layout (32 bytes): magic(8) version(4) reserved(4) seq(8)
// headerCRC(4) reserved(4). See docs/architecture.md §4.2.
const (
	SegmentHeaderSize = 32
	segmentMagic      = "ANCHWAL1"
	segmentVersion    = 1
)

// SegmentName returns the file name of segment seq.
func SegmentName(seq uint64) string { return fmt.Sprintf("wal-%016d.log", seq) }

// ParseSegmentName extracts the sequence number from a segment file name.
func ParseSegmentName(name string) (uint64, bool) {
	if len(name) != len("wal-")+16+len(".log") || !strings.HasPrefix(name, "wal-") || !strings.HasSuffix(name, ".log") {
		return 0, false
	}
	seq, err := strconv.ParseUint(name[4:20], 10, 64)
	return seq, err == nil
}

// EncodeSegmentHeader returns the header for segment seq.
func EncodeSegmentHeader(seq uint64) []byte {
	h := make([]byte, SegmentHeaderSize)
	copy(h, segmentMagic)
	binary.LittleEndian.PutUint32(h[8:12], segmentVersion)
	binary.LittleEndian.PutUint64(h[16:24], seq)
	binary.LittleEndian.PutUint32(h[24:28], record.Checksum(h[0:24]))
	return h
}

func checkSegmentHeader(h []byte, seq uint64) error {
	if len(h) < SegmentHeaderSize {
		return errors.New("segment header truncated")
	}
	if string(h[0:8]) != segmentMagic {
		return errors.New("bad segment magic")
	}
	if binary.LittleEndian.Uint32(h[24:28]) != record.Checksum(h[0:24]) {
		return errors.New("segment header checksum mismatch")
	}
	if v := binary.LittleEndian.Uint32(h[8:12]); v != segmentVersion {
		return fmt.Errorf("unsupported segment version %d", v)
	}
	if got := binary.LittleEndian.Uint64(h[16:24]); got != seq {
		return fmt.Errorf("segment header says seq %d, file name says %d", got, seq)
	}
	return nil
}

// FormatError describes on-disk data that violates the format. The engine
// turns it into a CorruptionError naming the file.
type FormatError struct {
	Offset int64
	Reason string
}

func (e *FormatError) Error() string { return fmt.Sprintf("offset %d: %s", e.Offset, e.Reason) }

// ScanResult describes how a segment ended.
type ScanResult struct {
	ValidEnd int64 // offset just past the last valid record
	Torn     bool  // a torn tail follows ValidEnd (only possible when final)
}

// ScanSegment validates the segment image data (the whole file) and calls fn
// for every Commit record in order. Classification of a bad tail follows the
// table in docs/architecture.md §6.2: only the final segment may end in a torn
// tail, and only an incomplete frame or an all-zero region counts as torn.
// Everything else is reported as a *FormatError. If fn returns an error,
// scanning stops and that error is returned unchanged.
func ScanSegment(data []byte, seq uint64, final bool, fn func(Commit) error) (ScanResult, error) {
	if err := checkSegmentHeader(data, seq); err != nil {
		return ScanResult{}, &FormatError{Offset: 0, Reason: err.Error()}
	}
	off := SegmentHeaderSize
	for off < len(data) {
		rest := data[off:]
		f, n, err := record.Decode(rest)
		if err != nil {
			torn := errors.Is(err, record.ErrIncomplete) ||
				(errors.Is(err, record.ErrBadHeader) && allZero(rest))
			if torn && final {
				return ScanResult{ValidEnd: int64(off), Torn: true}, nil
			}
			reason := err.Error()
			if torn {
				reason += " in a non-final segment"
			}
			return ScanResult{}, &FormatError{Offset: int64(off), Reason: reason}
		}
		if f.Type != RecordCommit {
			return ScanResult{}, &FormatError{Offset: int64(off), Reason: fmt.Sprintf("unknown record type %d", f.Type)}
		}
		c, err := DecodeCommit(f.Payload)
		if err != nil {
			return ScanResult{}, &FormatError{Offset: int64(off), Reason: "bad commit payload: " + err.Error()}
		}
		if err := fn(c); err != nil {
			var fe *FormatError
			if errors.As(err, &fe) && fe.Offset < 0 {
				fe.Offset = int64(off)
			}
			return ScanResult{}, err
		}
		off += n
	}
	return ScanResult{ValidEnd: int64(off)}, nil
}

func allZero(b []byte) bool {
	return len(bytes.TrimLeft(b, "\x00")) == 0
}

// Writer appends to one segment file.
type Writer struct {
	f    vfs.File
	seq  uint64
	size int64
}

// Seq returns the segment's sequence number.
func (w *Writer) Seq() uint64 { return w.seq }

// Size returns the segment's current length in bytes (as far as we know).
func (w *Writer) Size() int64 { return w.size }

// Write appends p. On a short write, Size still advances by the bytes that
// were written, though after any error the engine stops using the writer.
func (w *Writer) Write(p []byte) (int, error) {
	n, err := w.f.Write(p)
	w.size += int64(n)
	if err == nil && n < len(p) {
		err = fmt.Errorf("short write: %d of %d bytes", n, len(p))
	}
	return n, err
}

// Sync fsyncs the segment.
func (w *Writer) Sync() error { return w.f.Sync() }

// Close closes the file without syncing.
func (w *Writer) Close() error { return w.f.Close() }

// OpenForAppend opens an existing segment of the given (already validated)
// size for appending.
func OpenForAppend(fsys vfs.FS, dir string, seq uint64, size int64) (*Writer, error) {
	f, err := fsys.OpenFile(filepath.Join(dir, SegmentName(seq)), os.O_WRONLY|os.O_APPEND, 0)
	if err != nil {
		return nil, err
	}
	return &Writer{f: f, seq: seq, size: size}, nil
}

// CreateStage names the steps of CreateSegment, for crash hooks.
type CreateStage int

const (
	StageTmpSynced CreateStage = iota // temp file written and fsynced
	StageRenamed                      // renamed into place, directory not yet synced
)

// CreateSegment creates segment seq durably: write header to a temp file,
// fsync, rename into place, sync the directory. It reports whether the rename
// was attempted, because after that point the new name may exist on disk even
// if an error is returned, and the caller must treat the outcome as uncertain
// (docs/architecture.md §5.4). hook may be nil.
func CreateSegment(fsys vfs.FS, dir string, seq uint64, hook func(CreateStage)) (w *Writer, renameAttempted bool, err error) {
	final := filepath.Join(dir, SegmentName(seq))
	tmp := final + ".tmp"
	f, err := fsys.OpenFile(tmp, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o644)
	if err != nil {
		return nil, false, err
	}
	fail := func(err error) (*Writer, bool, error) {
		f.Close()
		fsys.Remove(tmp)
		return nil, false, err
	}
	if _, err := f.Write(EncodeSegmentHeader(seq)); err != nil {
		return fail(err)
	}
	if err := f.Sync(); err != nil {
		return fail(err)
	}
	if err := f.Close(); err != nil {
		fsys.Remove(tmp)
		return nil, false, err
	}
	if hook != nil {
		hook(StageTmpSynced)
	}
	if err := fsys.Rename(tmp, final); err != nil {
		return nil, true, err
	}
	if hook != nil {
		hook(StageRenamed)
	}
	if err := fsys.SyncDir(dir); err != nil {
		return nil, true, err
	}
	w, err = OpenForAppend(fsys, dir, seq, SegmentHeaderSize)
	return w, true, err
}
