package anchordb

import (
	"bytes"

	"github.com/rosaiju/anchordb/internal/index"
	"github.com/rosaiju/anchordb/internal/wal"
)

// Tx is a transaction. A read-write Tx holds the database's exclusive lock
// from Begin until Commit or Rollback; a read-only Tx holds the shared lock.
// A Tx must not be used by more than one goroutine at a time, and the
// goroutine holding it must not call Begin again (that deadlocks).
type Tx struct {
	db       *DB
	writable bool
	done     bool
	scanning int // depth of active Scan callbacks

	// writes is the private write buffer, ordered by key. A nil value marks a
	// delete; a put of an empty value is stored as a non-nil empty slice.
	writes *index.SkipList
	size   int // encoded Commit payload size if committed now
}

// Begin starts a transaction, blocking until the lock is available.
func (db *DB) Begin(writable bool) (*Tx, error) {
	if writable {
		db.mu.Lock()
	} else {
		db.mu.RLock()
	}
	tx := &Tx{db: db, writable: writable}
	if db.closed || db.failed.Load() {
		err := ErrDBFailed
		if db.closed {
			err = ErrClosed
		}
		tx.unlock()
		return nil, err
	}
	if writable {
		tx.writes = index.New(2)
		tx.size = wal.CommitHeaderSize
	}
	return tx, nil
}

// Writable reports whether the transaction can write.
func (tx *Tx) Writable() bool { return tx.writable }

func (tx *Tx) unlock() {
	if tx.writable {
		tx.db.mu.Unlock()
	} else {
		tx.db.mu.RUnlock()
	}
}

// end closes the transaction and releases its lock.
func (tx *Tx) end() {
	tx.done = true
	tx.writes = nil
	tx.unlock()
}

func checkKey(key []byte) error {
	if len(key) == 0 {
		return ErrEmptyKey
	}
	if len(key) > MaxKeySize {
		return ErrKeyTooLarge
	}
	return nil
}

// clone returns a copy that is never nil, so an empty value stays
// distinguishable from "absent".
func clone(b []byte) []byte { return append([]byte{}, b...) }

// Get returns key's value as seen by this transaction (its own buffered writes
// first, then committed data), or ErrNotFound.
func (tx *Tx) Get(key []byte) ([]byte, error) {
	if tx.done {
		return nil, ErrTxClosed
	}
	if err := checkKey(key); err != nil {
		return nil, err
	}
	if tx.writes != nil {
		if v, ok := tx.writes.Get(key); ok {
			if v == nil {
				return nil, ErrNotFound // deleted in this transaction
			}
			return clone(v), nil
		}
	}
	if v, ok := tx.db.index.Get(key); ok {
		return clone(v), nil
	}
	return nil, ErrNotFound
}

func (tx *Tx) checkWritable() error {
	switch {
	case tx.done:
		return ErrTxClosed
	case !tx.writable:
		return ErrTxReadOnly
	case tx.scanning > 0:
		return ErrScanInProgress
	}
	return nil
}

// Put buffers key=value. It becomes visible to others only after Commit.
func (tx *Tx) Put(key, value []byte) error {
	if err := tx.checkWritable(); err != nil {
		return err
	}
	if err := checkKey(key); err != nil {
		return err
	}
	if len(value) > MaxValueSize {
		return ErrValueTooLarge
	}
	return tx.buffer(key, clone(value))
}

// Delete buffers a delete of key. Deleting a missing key is not an error.
func (tx *Tx) Delete(key []byte) error {
	if err := tx.checkWritable(); err != nil {
		return err
	}
	if err := checkKey(key); err != nil {
		return err
	}
	return tx.buffer(key, nil)
}

// buffer records one write, keeping the running encoded size exact so that
// ErrTxTooLarge is reported here rather than at Commit.
func (tx *Tx) buffer(key, value []byte) error {
	size := tx.size + wal.OpSize(value == nil, len(key), len(value))
	if old, ok := tx.writes.Get(key); ok {
		size -= wal.OpSize(old == nil, len(key), len(old))
	}
	if size > MaxTxBytes {
		return ErrTxTooLarge
	}
	tx.writes.Set(clone(key), value)
	tx.size = size
	return nil
}

// Scan calls fn for each key in [start, end) in ascending order, as seen by
// this transaction. A nil or empty start means the first key; a nil or empty
// end means no upper bound. fn returning false stops the scan. The slices
// passed to fn are copies the caller may keep. fn must not call Put, Delete,
// Commit, or Rollback on this transaction (they return ErrScanInProgress);
// nested Scans are fine.
func (tx *Tx) Scan(start, end []byte, fn func(key, value []byte) bool) error {
	if tx.done {
		return ErrTxClosed
	}
	if len(start) > 0 && len(end) > 0 && bytes.Compare(start, end) >= 0 {
		return nil
	}
	tx.scanning++
	defer func() { tx.scanning-- }()

	inRange := func(it *index.Iterator) bool {
		return it != nil && it.Valid() && (len(end) == 0 || bytes.Compare(it.Key(), end) < 0)
	}
	// Merge two sorted streams: committed data (a) and this tx's buffer (b).
	// On equal keys the buffer wins; a buffered delete hides the key.
	a := tx.db.index.Seek(start)
	var b *index.Iterator
	if tx.writes != nil {
		b = tx.writes.Seek(start)
	}
	for {
		aOK, bOK := inRange(a), inRange(b)
		var k, v []byte
		switch {
		case !aOK && !bOK:
			return nil
		case aOK && (!bOK || bytes.Compare(a.Key(), b.Key()) < 0):
			k, v = a.Key(), a.Value()
			a.Next()
		default:
			if aOK && bytes.Equal(a.Key(), b.Key()) {
				a.Next()
			}
			k, v = b.Key(), b.Value()
			b.Next()
			if v == nil {
				continue // deleted in this transaction
			}
		}
		if !fn(clone(k), clone(v)) {
			return nil
		}
	}
}

// Commit makes the transaction's writes durable and visible, then ends it.
// The lock is released whatever the outcome. See docs/architecture.md §5.2
// for the exact commit point.
func (tx *Tx) Commit() error {
	if tx.done {
		return ErrTxClosed
	}
	if tx.scanning > 0 {
		return ErrScanInProgress
	}
	defer tx.end()
	if tx.db.failed.Load() {
		return ErrDBFailed
	}
	if !tx.writable || tx.writes.Len() == 0 {
		return nil
	}
	return tx.db.commit(tx.writes, tx.size)
}

// Rollback discards the transaction. On an already-closed transaction it
// returns ErrTxClosed and does nothing else, so `defer tx.Rollback()` is safe.
func (tx *Tx) Rollback() error {
	if tx.done {
		return ErrTxClosed
	}
	if tx.scanning > 0 {
		return ErrScanInProgress
	}
	tx.end()
	return nil
}
