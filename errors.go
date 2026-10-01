package anchordb

import (
	"errors"
	"fmt"

	"github.com/rosaiju/anchordb/internal/wal"
)

// Size limits (inclusive). See docs/architecture.md §2.1.
const (
	MaxKeySize   = wal.MaxKeySize   // 64 KiB
	MaxValueSize = wal.MaxValueSize // 16 MiB
	MaxTxBytes   = wal.MaxTxBytes   // 64 MiB, measured on the encoded Commit payload
)

var (
	ErrNotFound       = errors.New("anchordb: key not found")
	ErrEmptyKey       = errors.New("anchordb: empty key")
	ErrKeyTooLarge    = errors.New("anchordb: key too large")
	ErrValueTooLarge  = errors.New("anchordb: value too large")
	ErrTxTooLarge     = errors.New("anchordb: transaction too large")
	ErrTxClosed       = errors.New("anchordb: transaction already committed or rolled back")
	ErrTxReadOnly     = errors.New("anchordb: write in read-only transaction")
	ErrScanInProgress = errors.New("anchordb: transaction modified or ended inside its own Scan callback")
	ErrClosed         = errors.New("anchordb: database closed")
	ErrLocked         = errors.New("anchordb: database directory is locked by another process or handle")
	ErrCorrupt        = errors.New("anchordb: corrupt data")

	// ErrCommitUncertain means the commit's WAL write or fsync failed: the
	// transaction may or may not be durable. The DB has failed; Close and
	// reopen it, and recovery will decide (docs/architecture.md §6.4).
	ErrCommitUncertain = errors.New("anchordb: commit outcome unknown")

	// ErrDBFailed means an earlier I/O error left the in-memory state possibly
	// out of step with the disk. Close and reopen the DB.
	ErrDBFailed = errors.New("anchordb: database failed after an I/O error; close and reopen it")
)

// CorruptionError reports on-disk data that violates the format. It satisfies
// errors.Is(err, ErrCorrupt).
type CorruptionError struct {
	File   string // base name, e.g. "wal-0000000000000003.log" or "CURRENT"
	Offset int64  // offset of the bad frame; 0 for file headers and CURRENT
	Reason string
}

func (e *CorruptionError) Error() string {
	return fmt.Sprintf("anchordb: corrupt data in %s at offset %d: %s", e.File, e.Offset, e.Reason)
}

// Is makes errors.Is(err, ErrCorrupt) true.
func (e *CorruptionError) Is(target error) bool { return target == ErrCorrupt }
