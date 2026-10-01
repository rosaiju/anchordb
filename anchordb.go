// Package anchordb is an educational, single-machine, embedded, transactional
// key-value storage engine.
//
// The whole dataset lives in an in-memory skip list. Durability comes from a
// write-ahead log (one checksummed record per committed transaction) plus
// periodic checkpoints (full sorted snapshots). Transactions are serializable
// because a read-write transaction holds an exclusive lock for its whole life.
//
// docs/architecture.md is the specification this package implements.
package anchordb

import (
	"errors"
	"fmt"
	"path/filepath"
	"sync"
	"sync/atomic"

	"github.com/rosaiju/anchordb/internal/index"
	"github.com/rosaiju/anchordb/internal/lock"
	"github.com/rosaiju/anchordb/internal/manifest"
	"github.com/rosaiju/anchordb/internal/vfs"
	"github.com/rosaiju/anchordb/internal/wal"
)

// SyncMode controls whether Commit waits for the WAL to reach stable storage.
type SyncMode int

const (
	// SyncAlways fsyncs the WAL before Commit returns. Acknowledged commits
	// survive process crashes and (within the documented assumptions) power loss.
	SyncAlways SyncMode = iota
	// SyncNone returns after write(2) without fsync. Acknowledged commits
	// survive a process crash but may be lost on OS crash or power loss.
	SyncNone
)

// DefaultSegmentSize is the WAL rotation threshold when Options.SegmentSize is 0.
const DefaultSegmentSize = 8 << 20

// Options configures Open. The zero value is valid.
type Options struct {
	SyncMode    SyncMode
	SegmentSize int64  // rotate when the active segment reaches this size; <= 0 means DefaultSegmentSize
	FS          vfs.FS // nil means the real file system; tests inject faults here
}

// Stats is a point-in-time summary of the database.
type Stats struct {
	Keys           int
	LastTxID       uint64 // last committed transaction id (0 = none)
	CheckpointTxID uint64 // transaction id covered by the authoritative checkpoint
	WALStartSeq    uint64 // first WAL segment named by CURRENT
	ActiveSeq      uint64 // WAL segment currently appended to
	WALBytes       int64  // bytes in live WAL segments
	Recovery       RecoveryInfo
}

// RecoveryInfo describes what Open did.
type RecoveryInfo struct {
	ReplayedTxs    int   // WAL records applied on top of the checkpoint
	TruncatedBytes int64 // torn-tail bytes removed from the final segment
	RemovedFiles   int   // temp and unreferenced files deleted
}

// DB is an open database. It is safe for concurrent use by multiple goroutines.
type DB struct {
	dir  string
	fs   vfs.FS
	opts Options
	lock *lock.Lock

	// Lock order: ckptMu, then mu, then metaMu.
	ckptMu sync.Mutex   // one Checkpoint at a time; Close waits for it
	mu     sync.RWMutex // held for the whole life of every transaction
	closed bool         // written under ckptMu and mu.Lock

	failed atomic.Bool // set after an I/O error with an uncertain outcome

	// Written only under mu.Lock (so readers holding mu.RLock see a stable view).
	index    *index.SkipList
	lastTxID uint64

	// metaMu guards fields a checkpoint changes while other readers may hold
	// mu.RLock. Writers to these fields also hold mu (shared or exclusive)
	// with ckptMu, or mu exclusively.
	metaMu      sync.Mutex
	cur         manifest.Current
	w           *wal.Writer // active segment
	sealedBytes int64       // bytes in live segments other than the active one

	recovery RecoveryInfo // set once by Open
}

// Open opens (creating if necessary) the database in dir, running crash
// recovery. opts may be nil. Only one DB may have a directory open at a time.
func Open(dir string, opts *Options) (*DB, error) {
	var o Options
	if opts != nil {
		o = *opts
	}
	if o.SyncMode != SyncAlways && o.SyncMode != SyncNone {
		return nil, fmt.Errorf("anchordb: invalid SyncMode %d", o.SyncMode)
	}
	if o.SegmentSize <= 0 {
		o.SegmentSize = DefaultSegmentSize
	}
	if o.FS == nil {
		o.FS = vfs.OS
	}
	if err := o.FS.MkdirAll(dir, 0o755); err != nil {
		return nil, fmt.Errorf("anchordb: create directory: %w", err)
	}
	lk, err := lock.Acquire(filepath.Join(dir, "LOCK"))
	if err != nil {
		if errors.Is(err, lock.ErrLocked) {
			return nil, fmt.Errorf("%w: %s", ErrLocked, dir)
		}
		return nil, fmt.Errorf("anchordb: lock: %w", err)
	}
	db := &DB{dir: dir, fs: o.FS, opts: o, lock: lk}
	if err := db.recover(); err != nil {
		if db.w != nil {
			db.w.Close()
		}
		lk.Unlock()
		return nil, err
	}
	return db, nil
}

// Close waits for open transactions and any running checkpoint, then releases
// the files and the directory lock. In SyncNone mode it fsyncs the WAL first.
// On a failed DB it performs no I/O at all, so that reopening sees exactly
// what reached the disk.
func (db *DB) Close() error {
	db.ckptMu.Lock()
	defer db.ckptMu.Unlock()
	db.mu.Lock()
	defer db.mu.Unlock()
	if db.closed {
		return ErrClosed
	}
	db.closed = true
	var err error
	if !db.failed.Load() && db.opts.SyncMode == SyncNone {
		err = db.w.Sync()
	}
	if cerr := db.w.Close(); err == nil && !db.failed.Load() {
		err = cerr
	}
	if lerr := db.lock.Unlock(); err == nil {
		err = lerr
	}
	return err
}

// fail marks the DB failed and returns err for convenience.
func (db *DB) fail(err error) error {
	db.failed.Store(true)
	return err
}

// Stats returns a summary of the database.
func (db *DB) Stats() (Stats, error) {
	db.mu.RLock()
	defer db.mu.RUnlock()
	if db.closed {
		return Stats{}, ErrClosed
	}
	if db.failed.Load() {
		return Stats{}, ErrDBFailed
	}
	db.metaMu.Lock()
	defer db.metaMu.Unlock()
	return Stats{
		Keys:           db.index.Len(),
		LastTxID:       db.lastTxID,
		CheckpointTxID: db.cur.CheckpointTxID,
		WALStartSeq:    db.cur.WALStart,
		ActiveSeq:      db.w.Seq(),
		WALBytes:       db.sealedBytes + db.w.Size(),
		Recovery:       db.recovery,
	}, nil
}

// View runs fn in a read-only transaction, which is always rolled back.
func (db *DB) View(fn func(tx *Tx) error) error {
	tx, err := db.Begin(false)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	return fn(tx)
}

// Update runs fn in a read-write transaction. If fn returns nil the
// transaction is committed and Commit's result is returned; otherwise it is
// rolled back and fn's error is returned unchanged. A panic in fn rolls back
// and re-panics.
func (db *DB) Update(fn func(tx *Tx) error) error {
	tx, err := db.Begin(true)
	if err != nil {
		return err
	}
	// Rollback after Commit just returns ErrTxClosed, so this deferred call
	// is a no-op on success and cleans up on error or panic.
	defer tx.Rollback()
	if err := fn(tx); err != nil {
		return err
	}
	return tx.Commit()
}

// Get returns the value stored for key, or ErrNotFound.
func (db *DB) Get(key []byte) ([]byte, error) {
	var v []byte
	err := db.View(func(tx *Tx) error {
		var err error
		v, err = tx.Get(key)
		return err
	})
	return v, err
}

// Put stores key=value in its own transaction.
func (db *DB) Put(key, value []byte) error {
	return db.Update(func(tx *Tx) error { return tx.Put(key, value) })
}

// Delete removes key in its own transaction. Deleting a missing key is not an error.
func (db *DB) Delete(key []byte) error {
	return db.Update(func(tx *Tx) error { return tx.Delete(key) })
}

// Scan visits keys in [start, end) in ascending order; see Tx.Scan.
func (db *DB) Scan(start, end []byte, fn func(key, value []byte) bool) error {
	return db.View(func(tx *Tx) error { return tx.Scan(start, end, fn) })
}
