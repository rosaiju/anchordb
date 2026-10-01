package anchordb

import (
	"fmt"

	"github.com/rosaiju/anchordb/internal/crashpoint"
	"github.com/rosaiju/anchordb/internal/index"
	"github.com/rosaiju/anchordb/internal/record"
	"github.com/rosaiju/anchordb/internal/wal"
)

// commit runs the commit protocol of docs/architecture.md §5.2. The caller
// holds db.mu exclusively and has checked that the DB has not failed.
//
//	rotate if needed → encode one record → write → fsync → apply → return
//
// The commit point is the moment the complete record is in the log: recovery
// will replay it from then on. Success is only returned after the fsync (in
// SyncAlways mode) and after the index has been updated.
func (db *DB) commit(writes *index.SkipList, payloadSize int) error {
	if db.w.Size() >= db.opts.SegmentSize && db.w.Size() > wal.SegmentHeaderSize {
		if err := db.rotate(); err != nil {
			return err // nothing of this transaction was written
		}
	}

	txid := db.lastTxID + 1
	c := wal.Commit{TxID: txid, Ops: make([]wal.Op, 0, writes.Len())}
	for it := writes.Seek(nil); it.Valid(); it.Next() {
		v := it.Value()
		c.Ops = append(c.Ops, wal.Op{Delete: v == nil, Key: it.Key(), Value: v})
	}
	frame := record.Append(make([]byte, 0, record.HeaderSize+payloadSize), wal.RecordCommit, wal.EncodeCommit(c))

	crashpoint.Hit("commit.before-write")
	if crashpoint.Should("commit.partial-write") {
		db.w.Write(frame[:len(frame)/2])
		crashpoint.Crash()
	}
	if _, err := db.w.Write(frame); err != nil {
		return db.fail(fmt.Errorf("%w: WAL write: %w", ErrCommitUncertain, err))
	}
	crashpoint.Hit("commit.after-write")
	if db.opts.SyncMode == SyncAlways {
		// A failed fsync leaves the page cache in an unknown state, so we do
		// not retry: we fail the DB and let recovery read what is on disk.
		if err := db.w.Sync(); err != nil {
			return db.fail(fmt.Errorf("%w: WAL fsync: %w", ErrCommitUncertain, err))
		}
	}
	crashpoint.Hit("commit.after-sync")

	// The buffer's keys and values are private copies, so the index can own them.
	for _, op := range c.Ops {
		if op.Delete {
			db.index.Delete(op.Key)
		} else {
			db.index.Set(op.Key, op.Value)
		}
	}
	db.lastTxID = txid
	crashpoint.Hit("commit.after-apply")
	return nil
}

// rotate seals the active segment and switches to a new one
// (docs/architecture.md §5.4). The caller excludes all writers: it holds
// db.mu exclusively, or holds db.mu shared together with db.ckptMu.
func (db *DB) rotate() error {
	old := db.w
	if err := old.Sync(); err != nil {
		return db.fail(fmt.Errorf("%w: fsync segment before rotation: %w", ErrDBFailed, err))
	}
	crashpoint.Hit("rotate.after-seal")
	nw, renameAttempted, err := wal.CreateSegment(db.fs, db.dir, old.Seq()+1, func(s wal.CreateStage) {
		switch s {
		case wal.StageTmpSynced:
			crashpoint.Hit("rotate.after-create-tmp")
		case wal.StageRenamed:
			crashpoint.Hit("rotate.after-rename")
		}
	})
	if err != nil {
		if renameAttempted {
			// The new name may now exist. If we kept appending to the old
			// segment it would no longer be the final one, and a torn record
			// in it would later look like corruption. Stop here.
			return db.fail(fmt.Errorf("%w: create WAL segment: %w", ErrDBFailed, err))
		}
		return fmt.Errorf("anchordb: create WAL segment: %w", err)
	}
	db.metaMu.Lock()
	db.sealedBytes += old.Size()
	db.w = nw
	db.metaMu.Unlock()
	old.Close() // already fsynced; Windows needs it closed before it can be deleted
	return nil
}
