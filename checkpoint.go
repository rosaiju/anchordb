package anchordb

import (
	"fmt"
	"path/filepath"
	"strings"

	"github.com/rosaiju/anchordb/internal/checkpoint"
	"github.com/rosaiju/anchordb/internal/crashpoint"
	"github.com/rosaiju/anchordb/internal/manifest"
	"github.com/rosaiju/anchordb/internal/wal"
)

// Checkpoint writes a snapshot of the current state, publishes it as
// authoritative, and deletes WAL segments and checkpoints it makes obsolete.
// Writers are blocked while the snapshot is written; readers are not.
// See docs/architecture.md §5.5.
func (db *DB) Checkpoint() error {
	db.ckptMu.Lock()
	defer db.ckptMu.Unlock()
	if db.closed {
		return ErrClosed
	}
	if db.failed.Load() {
		return ErrDBFailed
	}

	// Phase 1, holding mu shared: no commit can run, so lastTxID and the
	// index are stable, and rotating the WAL is safe.
	db.mu.RLock()
	txid := db.lastTxID
	db.metaMu.Lock()
	unchanged := txid == db.cur.CheckpointTxID
	db.metaMu.Unlock()
	if unchanged {
		db.mu.RUnlock()
		return nil
	}
	// After rotation every record with id > txid will be in segments >= walStart.
	if err := db.rotate(); err != nil {
		db.mu.RUnlock()
		return err
	}
	crashpoint.Hit("checkpoint.after-rotate")
	walStart := db.w.Seq()
	err := checkpoint.Write(db.fs, db.dir, txid, func(yield func(k, v []byte) bool) {
		for it := db.index.Seek(nil); it.Valid(); it.Next() {
			if !yield(it.Key(), it.Value()) {
				return
			}
		}
	}, func(s checkpoint.Stage) {
		switch s {
		case checkpoint.StagePartial:
			crashpoint.Hit("checkpoint.partial-write")
		case checkpoint.StageSynced:
			crashpoint.Hit("checkpoint.after-file-sync")
		case checkpoint.StageRenamed:
			crashpoint.Hit("checkpoint.after-file-rename")
		}
	})
	db.mu.RUnlock()
	if err != nil {
		return fmt.Errorf("anchordb: write checkpoint: %w", err)
	}

	// Phase 2, publication: writers may run again (they append to walStart
	// or later). Replacing CURRENT is the moment the checkpoint becomes
	// authoritative.
	next := manifest.Current{Checkpoint: checkpoint.Name(txid), CheckpointTxID: txid, WALStart: walStart}
	renameAttempted, err := manifest.Publish(db.fs, db.dir, next, func(s manifest.Stage) {
		switch s {
		case manifest.StageTmpSynced:
			crashpoint.Hit("checkpoint.after-current-tmp")
		case manifest.StageRenamed:
			crashpoint.Hit("checkpoint.after-current-rename")
		}
	})
	if err != nil {
		if renameAttempted {
			// Either CURRENT may be the durable one. Both are valid (nothing
			// has been deleted), but we no longer know which, so stop.
			return db.fail(fmt.Errorf("%w: publish checkpoint: %w", ErrDBFailed, err))
		}
		return fmt.Errorf("anchordb: publish checkpoint: %w", err)
	}
	db.metaMu.Lock()
	db.cur = next
	db.metaMu.Unlock()
	crashpoint.Hit("checkpoint.after-publish")

	// Phase 3, reclamation: only files the durable CURRENT no longer names.
	if _, err := db.removeObsolete(walStart, next.Checkpoint, false, "checkpoint.mid-reclaim"); err != nil {
		return fmt.Errorf("anchordb: reclaim old files: %w", err)
	}
	crashpoint.Hit("checkpoint.after-reclaim")
	return nil
}

// removeObsolete lists the directory and deletes, in order: engine temp files
// (only if withTemps), WAL segments with seq < walStart (ascending), and
// checkpoint files other than keep. It then syncs the directory. crashName
// is hit after the first deletion. It returns how many files were removed.
func (db *DB) removeObsolete(walStart uint64, keep string, withTemps bool, crashName string) (int, error) {
	entries, err := db.fs.ReadDir(db.dir)
	if err != nil {
		return 0, err
	}
	var temps, segs, ckpts []string
	segSize := map[string]int64{}
	for _, e := range entries {
		name := e.Name()
		if seq, ok := wal.ParseSegmentName(name); ok && seq < walStart {
			segs = append(segs, name)
			if info, err := e.Info(); err == nil {
				segSize[name] = info.Size()
			}
		} else if _, ok := checkpoint.ParseName(name); ok && name != keep {
			ckpts = append(ckpts, name)
		} else if withTemps && isTempName(name) {
			temps = append(temps, name)
		}
	}
	// os.ReadDir sorts by name, and zero-padded names sort numerically.
	removed := 0
	for _, list := range [][]string{temps, segs, ckpts} {
		for _, name := range list {
			if err := db.fs.Remove(filepath.Join(db.dir, name)); err != nil {
				return removed, err
			}
			removed++
			if size, ok := segSize[name]; ok {
				db.metaMu.Lock()
				db.sealedBytes -= size
				db.metaMu.Unlock()
			}
			if removed == 1 {
				crashpoint.Hit(crashName)
			}
		}
	}
	return removed, db.fs.SyncDir(db.dir)
}

// isTempName reports whether name is a temp file the engine creates:
// a segment, checkpoint, or CURRENT name followed by ".tmp".
func isTempName(name string) bool {
	base, ok := strings.CutSuffix(name, ".tmp")
	if !ok {
		return false
	}
	if base == manifest.FileName {
		return true
	}
	if _, ok := wal.ParseSegmentName(base); ok {
		return true
	}
	_, ok = checkpoint.ParseName(base)
	return ok
}
