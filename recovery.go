package anchordb

import (
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"sort"

	"github.com/rosaiju/anchordb/internal/checkpoint"
	"github.com/rosaiju/anchordb/internal/crashpoint"
	"github.com/rosaiju/anchordb/internal/index"
	"github.com/rosaiju/anchordb/internal/manifest"
	"github.com/rosaiju/anchordb/internal/wal"
)

type segFile struct {
	seq  uint64
	size int64
}

// dirListing is the set of engine files found in the database directory.
type dirListing struct {
	hasCurrent bool
	segs       []segFile // ascending by seq
	ckpts      []string
}

func (db *DB) list() (dirListing, error) {
	entries, err := db.fs.ReadDir(db.dir)
	if err != nil {
		return dirListing{}, err
	}
	var l dirListing
	for _, e := range entries {
		name := e.Name()
		switch {
		case name == manifest.FileName:
			l.hasCurrent = true
		case isSegmentName(name):
			seq, _ := wal.ParseSegmentName(name)
			info, err := e.Info()
			if err != nil {
				return dirListing{}, err
			}
			l.segs = append(l.segs, segFile{seq: seq, size: info.Size()})
		case isCheckpointName(name):
			l.ckpts = append(l.ckpts, name)
		}
	}
	sort.Slice(l.segs, func(i, j int) bool { return l.segs[i].seq < l.segs[j].seq })
	return l, nil
}

func isSegmentName(n string) bool    { _, ok := wal.ParseSegmentName(n); return ok }
func isCheckpointName(n string) bool { _, ok := checkpoint.ParseName(n); return ok }

func corrupt(file string, off int64, reason string) error {
	return &CorruptionError{File: file, Offset: off, Reason: reason}
}

// asCorruption converts a format error from an internal package into a
// CorruptionError naming file; other errors (real I/O errors) pass through.
func asCorruption(file string, err error) error {
	var fe *wal.FormatError
	if errors.As(err, &fe) {
		return corrupt(file, fe.Offset, fe.Reason)
	}
	return err
}

func (db *DB) readFile(name string) ([]byte, error) {
	f, err := db.fs.OpenFile(filepath.Join(db.dir, name), os.O_RDONLY, 0)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	st, err := f.Stat()
	if err != nil {
		return nil, err
	}
	buf := make([]byte, st.Size())
	if _, err := f.ReadAt(buf, 0); err != nil && !(errors.Is(err, io.EOF) && len(buf) == 0) {
		return nil, err
	}
	return buf, nil
}

// initialize creates a brand-new database: segment 1 first, then CURRENT.
// The order matters: "CURRENT exists but names no segment" is then always
// corruption, never a half-finished initialization that would hide a lost
// segment.
func (db *DB) initialize(createSegment bool) error {
	if createSegment {
		w, _, err := wal.CreateSegment(db.fs, db.dir, 1, nil)
		if err != nil {
			return fmt.Errorf("anchordb: initialize: %w", err)
		}
		w.Close()
		crashpoint.Hit("init.after-segment")
	}
	if _, err := manifest.Publish(db.fs, db.dir, manifest.Current{WALStart: 1}, nil); err != nil {
		return fmt.Errorf("anchordb: initialize: %w", err)
	}
	return nil
}

// recover implements docs/architecture.md §5.3. Everything up to the replay
// of the last segment only reads; files are modified (torn tail truncated,
// garbage deleted) only once the whole database has been validated.
func (db *DB) recover() error {
	l, err := db.list()
	if err != nil {
		return err
	}
	if !l.hasCurrent {
		switch {
		case len(l.segs) == 0 && len(l.ckpts) == 0:
			err = db.initialize(true)
		case len(l.ckpts) == 0 && len(l.segs) == 1 && l.segs[0].seq == 1 && l.segs[0].size == wal.SegmentHeaderSize:
			// Crash between the two initialization steps; check the header.
			data, rerr := db.readFile(wal.SegmentName(1))
			if rerr != nil {
				return rerr
			}
			if _, serr := wal.ScanSegment(data, 1, true, func(wal.Commit) error { return nil }); serr != nil {
				return asCorruption(wal.SegmentName(1), serr)
			}
			err = db.initialize(false)
		default:
			return corrupt(manifest.FileName, 0, "CURRENT is missing but data files exist")
		}
		if err != nil {
			return err
		}
		if l, err = db.list(); err != nil {
			return err
		}
	}

	// 1. CURRENT.
	raw, err := db.readFile(manifest.FileName)
	if err != nil {
		return err
	}
	cur, err := manifest.Decode(raw)
	if err != nil {
		return corrupt(manifest.FileName, 0, err.Error())
	}

	// 2. Checkpoint.
	idx := index.New(1)
	if cur.Checkpoint != "" {
		err := checkpoint.Read(db.fs, filepath.Join(db.dir, cur.Checkpoint), cur.CheckpointTxID,
			func(k, v []byte) { idx.Set(clone(k), clone(v)) })
		if errors.Is(err, fs.ErrNotExist) {
			return corrupt(cur.Checkpoint, 0, "checkpoint named by CURRENT is missing")
		}
		if err != nil {
			return asCorruption(cur.Checkpoint, err)
		}
	}
	last := cur.CheckpointTxID

	// 3. The live segments must be exactly wal_start, wal_start+1, ...
	var live []segFile
	for _, s := range l.segs {
		if s.seq >= cur.WALStart {
			live = append(live, s)
		}
	}
	if len(live) == 0 || live[0].seq != cur.WALStart {
		return corrupt(wal.SegmentName(cur.WALStart), 0, "first WAL segment named by CURRENT is missing")
	}
	for i := 1; i < len(live); i++ {
		if live[i].seq != live[i-1].seq+1 {
			return corrupt(wal.SegmentName(live[i-1].seq+1), 0, "WAL segment is missing (gap in sequence)")
		}
	}

	// 4. Replay. Only the final segment may end in a torn tail.
	var (
		replayed int
		tail     wal.ScanResult
		sealed   int64
	)
	for i, s := range live {
		name := wal.SegmentName(s.seq)
		data, err := db.readFile(name)
		if err != nil {
			return err
		}
		final := i == len(live)-1
		res, err := wal.ScanSegment(data, s.seq, final, func(c wal.Commit) error {
			if c.TxID != last+1 {
				return &wal.FormatError{Offset: -1, Reason: fmt.Sprintf("transaction id %d, expected %d", c.TxID, last+1)}
			}
			for _, op := range c.Ops {
				if op.Delete {
					idx.Delete(op.Key)
				} else {
					idx.Set(clone(op.Key), clone(op.Value))
				}
			}
			last = c.TxID
			replayed++
			return nil
		})
		if err != nil {
			return asCorruption(name, err)
		}
		if final {
			tail = res
		} else {
			sealed += s.size
		}
	}

	// 5. Everything is valid. Only now modify files: drop the torn tail...
	active := live[len(live)-1]
	var truncated int64
	if tail.Torn {
		if err := db.truncateSegment(active.seq, tail.ValidEnd); err != nil {
			return err
		}
		truncated = active.size - tail.ValidEnd
		crashpoint.Hit("recovery.after-truncate")
	}
	activeSize := tail.ValidEnd

	// ...and delete files the durable CURRENT does not reference.
	removed, err := db.removeObsolete(cur.WALStart, cur.Checkpoint, true, "recovery.mid-cleanup")
	if err != nil {
		return fmt.Errorf("anchordb: remove obsolete files: %w", err)
	}

	w, err := wal.OpenForAppend(db.fs, db.dir, active.seq, activeSize)
	if err != nil {
		return err
	}
	db.index = idx
	db.lastTxID = last
	db.cur = cur
	db.w = w
	db.sealedBytes = sealed
	db.recovery = RecoveryInfo{ReplayedTxs: replayed, TruncatedBytes: truncated, RemovedFiles: removed}
	return nil
}

func (db *DB) truncateSegment(seq uint64, size int64) error {
	f, err := db.fs.OpenFile(filepath.Join(db.dir, wal.SegmentName(seq)), os.O_RDWR, 0)
	if err != nil {
		return err
	}
	if err := f.Truncate(size); err != nil {
		f.Close()
		return err
	}
	if err := f.Sync(); err != nil {
		f.Close()
		return err
	}
	return f.Close()
}
